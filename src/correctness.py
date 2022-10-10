import requests
import json
import time

host = "http://localhost:8080"


def init():
    return requests.get(f"{host}/init")

def update_cluster():
    requests.post(f"http://localhost:8080/updateCluster", json={
  "hosts": [
    "127.0.0.1:8090",
    "127.0.0.1:8091",
    "127.0.0.1:8092",
  ],
  "index": 1
})
    requests.post(f"http://localhost:8081/updateCluster", json={
        "hosts": [
            "127.0.0.1:8090",
            "127.0.0.1:8091",
            "127.0.0.1:8092",
        ],
        "index": 2
    })
    requests.post(f"http://localhost:8082/updateCluster", json={
        "hosts": [
            "127.0.0.1:8090",
            "127.0.0.1:8091",
            "127.0.0.1:8092",
        ],
        "index": 3
    })

def add(key: str, value: str):
    return requests.post(f"{host}/add", json={"key": key, "value": value})


def get(key: str):
    return requests.get(f"{host}/query/{key}")


def del_(key: str):
    return requests.get(f"{host}/del/{key}")


def list(l: list):
    return requests.post(f"{host}/list", json=l)


def batch(l: list):
    return requests.post(f"{host}/batch", json=l)


def zadd(key: str, value: str, score: int):
    return requests.post(f"{host}/zadd/{key}", json={"score": score, "value": value})


def zrange(key: str, min_score: int, max_score: int):
    return requests.post(f"{host}/zrange/{key}", json={"min_score": min_score, "max_score": max_score})


def zrmv(key: str, value: str):
    return requests.get(f"{host}/zrmv/{key}/{value}")


def wait_until_inited():
    while (True):
        resp = init()
        if (resp.status_code == 200 and resp.text == "ok"):
            break
        time.sleep(1)


def gen_kv(seed: int):
    k = f"k{seed}"
    v = f"v{seed}"
    return k, v


def gen_z(seed: int, score: int):
    k = f"k{seed}"
    v = f"v{seed}_{score}"
    s = score
    return k, v, s


def test_basic_kv():
    resp = add("k1", "v1")
    assert resp.status_code == 200

    resp = get("k1")
    assert resp.status_code == 200
    assert resp.text == "v1"

    resp = del_("k1")
    assert resp.status_code == 200

    resp = get("k1")
    assert resp.status_code == 404
    assert resp.text == ""


def test_batch_kv():
    data = []
    keys = []
    for i in range(10):
        k = f"k{i}"
        v = f"v{i}"
        data.append({"key": k, "value": v})
        keys.append(k)
    resp = batch(data)
    assert resp.status_code == 200
    resp = list(keys)
    assert resp.status_code == 200
    values = json.loads(resp.text)
    assert len(values) == len(data)
    for i in range(len(values)):
        assert data[i] == values[i]


def test_zset():
    key = "zk"
    for i in range(10):
        value = f"zv{i}"
        score = i
        resp = zadd(key, value, score)
        assert resp.status_code == 200
    resp = zrange(key, 0, 10)
    assert resp.status_code == 200
    values = json.loads(resp.text)
    assert len(values) == 10
    for i in range(len(values)):
        s=values[i]["score"]
        assert values[i]["value"] == f"zv{s}"


    rm_list = []
    left_list = []
    for i in range(10):
        if i & 1 == 1:
            rm_list.append(f"zv{i}")
        else:
            left_list.append({"score": i, "value":  f"zv{i}"})

    for v in rm_list:
        resp = zrmv(key, v)
        assert resp.status_code == 200

    resp = zrange(key, 0, 10)
    assert resp.status_code == 200
    values = json.loads(resp.text)
    assert len(values) == len(left_list)
    for i in range(len(values)):
        s=values[i]["score"]
        assert values[i]["value"] == f"zv{s}"
        assert s%2==0


if __name__ == "__main__":
    update_cluster()
    wait_until_inited()
    test_basic_kv()
    test_batch_kv()
    test_zset()
