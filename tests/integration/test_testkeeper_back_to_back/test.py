import pytest
from helpers.cluster import ClickHouseCluster
import random
import string
import os
import time

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/enable_test_keeper.xml'], with_zookeeper=True)
from kazoo.client import KazooClient

_genuine_zk_instance = None
_fake_zk_instance = None

def get_genuine_zk():
    global _genuine_zk_instance
    if not _genuine_zk_instance:
        print("Zoo1", cluster.get_instance_ip("zoo1"))
        _genuine_zk_instance = cluster.get_kazoo_client('zoo1')
    return _genuine_zk_instance


def get_fake_zk():
    global _fake_zk_instance
    if not _fake_zk_instance:
        print("node", cluster.get_instance_ip("node"))
        _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip("node") + ":9181")
        _fake_zk_instance.start()
    return _fake_zk_instance

def random_string(length):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def create_random_path(prefix="", depth=1):
    if depth == 0:
        return prefix
    return create_random_path(os.path.join(prefix, random_string(3)), depth - 1)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()
        _genuine_zk_instance.stop()
        _genuine_zk_instance.close()
        _fake_zk_instance.stop()
        _fake_zk_instance.close()


def test_simple_commands(started_cluster):
    genuine_zk = get_genuine_zk()
    fake_zk = get_fake_zk()

    for zk in [genuine_zk, fake_zk]:
        zk.create("/test_simple_commands", b"")
        zk.create("/test_simple_commands/somenode1", b"hello")
        zk.set("/test_simple_commands/somenode1", b"world")

    for zk in [genuine_zk, fake_zk]:
        assert zk.exists("/test_simple_commands")
        assert zk.exists("/test_simple_commands/somenode1")
        print(zk.get("/test_simple_commands/somenode1"))
        assert zk.get("/test_simple_commands/somenode1")[0] == b"world"


def test_sequential_nodes(started_cluster):
    genuine_zk = get_genuine_zk()
    fake_zk = get_fake_zk()
    genuine_zk.create("/test_sequential_nodes")
    fake_zk.create("/test_sequential_nodes")
    for i in range(1, 11):
        genuine_zk.create("/test_sequential_nodes/" + ("a" * i) + "-", sequence=True)
        genuine_zk.create("/test_sequential_nodes/" + ("b" * i))
        fake_zk.create("/test_sequential_nodes/" + ("a" * i) + "-", sequence=True)
        fake_zk.create("/test_sequential_nodes/" + ("b" * i))

    genuine_childs = list(sorted(genuine_zk.get_children("/test_sequential_nodes")))
    fake_childs = list(sorted(fake_zk.get_children("/test_sequential_nodes")))
    assert genuine_childs == fake_childs


def assert_eq_stats(stat1, stat2):
    assert stat1.version == stat2.version
    assert stat1.cversion == stat2.cversion
    assert stat1.aversion == stat2.aversion
    assert stat1.aversion == stat2.aversion
    assert stat1.dataLength == stat2.dataLength
    assert stat1.numChildren == stat2.numChildren

def test_stats(started_cluster):
    genuine_zk = get_genuine_zk()
    fake_zk = get_fake_zk()
    genuine_zk.create("/test_stats_nodes")
    fake_zk.create("/test_stats_nodes")
    genuine_stats = genuine_zk.exists("/test_stats_nodes")
    fake_stats = fake_zk.exists("/test_stats_nodes")
    assert_eq_stats(genuine_stats, fake_stats)
    for i in range(1, 11):
        genuine_zk.create("/test_stats_nodes/" + ("a" * i) + "-", sequence=True)
        genuine_zk.create("/test_stats_nodes/" + ("b" * i))
        fake_zk.create("/test_stats_nodes/" + ("a" * i) + "-", sequence=True)
        fake_zk.create("/test_stats_nodes/" + ("b" * i))

    genuine_stats = genuine_zk.exists("/test_stats_nodes")
    fake_stats = fake_zk.exists("/test_stats_nodes")
    assert_eq_stats(genuine_stats, fake_stats)
    for i in range(1, 11):
        print("/test_stats_nodes/" + ("a" * i) + "-" + "{:010d}".format((i - 1) * 2))
        genuine_zk.delete("/test_stats_nodes/" + ("a" * i) + "-" + "{:010d}".format((i - 1) * 2))
        genuine_zk.delete("/test_stats_nodes/" + ("b" * i))
        fake_zk.delete("/test_stats_nodes/" + ("a" * i) + "-" + "{:010d}".format((i - 1) * 2))
        fake_zk.delete("/test_stats_nodes/" + ("b" * i))

    genuine_stats = genuine_zk.exists("/test_stats_nodes")
    fake_stats = fake_zk.exists("/test_stats_nodes")
    print(genuine_stats)
    print(fake_stats)
    assert_eq_stats(genuine_stats, fake_stats)
    for i in range(100):
        genuine_zk.set("/test_stats_nodes", ("q" * i).encode())
        fake_zk.set("/test_stats_nodes", ("q" * i).encode())

    genuine_stats = genuine_zk.exists("/test_stats_nodes")
    fake_stats = fake_zk.exists("/test_stats_nodes")
    print(genuine_stats)
    print(fake_stats)
    assert_eq_stats(genuine_stats, fake_stats)

def test_watchers(started_cluster):
    genuine_zk = get_genuine_zk()
    fake_zk = get_fake_zk()
    genuine_zk.create("/test_data_watches")
    fake_zk.create("/test_data_watches")
    genuine_data_watch_data = None

    def genuine_callback(event):
        print("Genuine data watch called")
        nonlocal genuine_data_watch_data
        genuine_data_watch_data = event

    fake_data_watch_data = None
    def fake_callback(event):
        print("Fake data watch called")
        nonlocal fake_data_watch_data
        fake_data_watch_data = event

    genuine_zk.get("/test_data_watches", watch=genuine_callback)
    fake_zk.get("/test_data_watches", watch=fake_callback)

    print("Calling set genuine")
    genuine_zk.set("/test_data_watches", b"a")
    print("Calling set fake")
    fake_zk.set("/test_data_watches", b"a")
    time.sleep(5)

    print("Genuine data", genuine_data_watch_data)
    print("Fake data", fake_data_watch_data)
    assert genuine_data_watch_data == fake_data_watch_data

    genuine_children = None
    def genuine_child_callback(event):
        print("Genuine child watch called")
        nonlocal genuine_children
        genuine_children = event

    fake_children = None
    def fake_child_callback(event):
        print("Fake child watch called")
        nonlocal fake_children
        fake_children = event

    genuine_zk.get_children("/test_data_watches", watch=genuine_child_callback)
    fake_zk.get_children("/test_data_watches", watch=fake_child_callback)

    print("Calling genuine child")
    genuine_zk.create("/test_data_watches/child", b"b")
    print("Calling fake child")
    fake_zk.create("/test_data_watches/child", b"b")

    time.sleep(5)

    print("Genuine children", genuine_children)
    print("Fake children", fake_children)
    assert genuine_children == fake_children

def test_multitransactions(started_cluster):
    genuine_zk = get_genuine_zk()
    fake_zk = get_fake_zk()
    for zk in [genuine_zk, fake_zk]:
        zk.create('/test_multitransactions')
        t = zk.transaction()
        t.create('/test_multitransactions/freddy')
        t.create('/test_multitransactions/fred', ephemeral=True)
        t.create('/test_multitransactions/smith', sequence=True)
        results = t.commit()
        assert len(results) == 3
        assert results[0] == '/test_multitransactions/freddy'
        assert results[2].startswith('/test_multitransactions/smith0') is True

    from kazoo.exceptions import RolledBackError, NoNodeError
    for i, zk in enumerate([genuine_zk, fake_zk]):
        print("Processing ZK", i)
        t = zk.transaction()
        t.create('/test_multitransactions/q')
        t.delete('/test_multitransactions/a')
        t.create('/test_multitransactions/x')
        results = t.commit()
        print("Results", results)
        assert results[0].__class__ == RolledBackError
        assert results[1].__class__ == NoNodeError
        assert zk.exists('/test_multitransactions/q') is None
        assert zk.exists('/test_multitransactions/a') is None
        assert zk.exists('/test_multitransactions/x') is None


def exists(zk, path):
    result = zk.exists(path)
    return result is not None

def get(zk, path):
    result = zk.get(path)
    return result[0]

def get_children(zk, path):
    return [elem for elem in list(sorted(zk.get_children(path))) if elem not in ('clickhouse', 'zookeeper')]

READ_REQUESTS = [
    ("exists", exists),
    ("get", get),
    ("get_children", get_children),
]


def create(zk, path, data):
    zk.create(path, data.encode())


def set_data(zk, path, data):
    zk.set(path, data.encode())


WRITE_REQUESTS = [
    ("create", create),
    ("set_data", set_data),
]


def delete(zk, path):
    zk.delete(path)

DELETE_REQUESTS = [
    ("delete", delete)
]


class Request(object):
    def __init__(self, name, arguments, callback, is_return):
        self.name = name
        self.arguments = arguments
        self.callback = callback
        self.is_return = is_return

    def __str__(self):
        arg_str = ', '.join([str(k) + "=" + str(v) for k, v in self.arguments.items()])
        return "ZKRequest name {} with arguments {}".format(self.name, arg_str)

def generate_requests(iters=1):
    requests = []
    existing_paths = []
    for i in range(iters):
        for _ in range(100):
            rand_length = random.randint(0, 10)
            path = "/"
            for j in range(1, rand_length):
                path = create_random_path(path, 1)
                existing_paths.append(path)
                value = random_string(1000)
                request = Request("create", {"path" : path, "value": value[0:10]}, lambda zk, path=path, value=value: create(zk, path, value), False)
                requests.append(request)

        for _ in range(100):
            path = random.choice(existing_paths)
            value = random_string(100)
            request = Request("set", {"path": path, "value": value[0:10]}, lambda zk, path=path, value=value: set_data(zk, path, value), False)
            requests.append(request)

        for _ in range(100):
            path = random.choice(existing_paths)
            callback = random.choice(READ_REQUESTS)
            def read_func1(zk, path=path, callback=callback):
                return callback[1](zk, path)

            request = Request(callback[0], {"path": path}, read_func1, True)
            requests.append(request)

        for _ in range(30):
            path = random.choice(existing_paths)
            request = Request("delete", {"path": path}, lambda zk, path=path: delete(zk, path), False)

        for _ in range(100):
            path = random.choice(existing_paths)
            callback = random.choice(READ_REQUESTS)
            def read_func2(zk, path=path, callback=callback):
                return callback[1](zk, path)
            request = Request(callback[0], {"path": path}, read_func2, True)
            requests.append(request)
    return requests


def test_random_requests(started_cluster):
    requests = generate_requests(10)
    genuine_zk = get_genuine_zk()
    fake_zk = get_fake_zk()
    for i, request in enumerate(requests):
        genuine_throw = False
        fake_throw = False
        fake_result = None
        genuine_result = None
        try:
            genuine_result = request.callback(genuine_zk)
        except Exception as ex:
            genuine_throw = True

        try:
            fake_result = request.callback(fake_zk)
        except Exception as ex:
            fake_throw = True

        assert fake_throw == genuine_throw, "Fake throw genuine not or vise versa"
        assert fake_result == genuine_result, "Zookeeper results differ"
    root_children_genuine = [elem for elem in list(sorted(genuine_zk.get_children("/"))) if elem not in ('clickhouse', 'zookeeper')]
    root_children_fake = [elem for elem in list(sorted(fake_zk.get_children("/"))) if elem not in ('clickhouse', 'zookeeper')]
    assert root_children_fake == root_children_genuine
