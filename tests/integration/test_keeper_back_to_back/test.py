import pytest
from helpers.cluster import ClickHouseCluster
import random
import string
import os
import time
from multiprocessing.dummy import Pool

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/enable_keeper.xml'], with_zookeeper=True, use_keeper=False)
from kazoo.client import KazooClient, KazooState, KeeperState

def get_genuine_zk():
    print("Zoo1", cluster.get_instance_ip("zoo1"))
    return cluster.get_kazoo_client('zoo1')

def get_fake_zk():
    print("node", cluster.get_instance_ip("node"))
    _fake_zk_instance =  KazooClient(hosts=cluster.get_instance_ip("node") + ":9181", timeout=30.0)
    def reset_last_zxid_listener(state):
        print("Fake zk callback called for state", state)
        nonlocal _fake_zk_instance
        if state != KazooState.CONNECTED:
            _fake_zk_instance._reset()

    _fake_zk_instance.add_listener(reset_last_zxid_listener)
    _fake_zk_instance.start()
    return _fake_zk_instance

def random_string(length):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def create_random_path(prefix="", depth=1):
    if depth == 0:
        return prefix
    return create_random_path(os.path.join(prefix, random_string(3)), depth - 1)

def stop_zk(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_simple_commands(started_cluster):
    try:
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
    finally:
        for zk in [genuine_zk, fake_zk]:
            stop_zk(zk)


def test_sequential_nodes(started_cluster):
    try:
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
    finally:
        for zk in [genuine_zk, fake_zk]:
            stop_zk(zk)


def assert_eq_stats(stat1, stat2):
    assert stat1.version == stat2.version
    assert stat1.cversion == stat2.cversion
    assert stat1.aversion == stat2.aversion
    assert stat1.aversion == stat2.aversion
    assert stat1.dataLength == stat2.dataLength
    assert stat1.numChildren == stat2.numChildren

def test_stats(started_cluster):
    try:
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
    finally:
        for zk in [genuine_zk, fake_zk]:
            stop_zk(zk)

def test_watchers(started_cluster):
    try:
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
        time.sleep(3)

        print("Genuine data", genuine_data_watch_data)
        print("Fake data", fake_data_watch_data)
        assert genuine_data_watch_data == fake_data_watch_data


        genuine_zk.create("/test_data_watches/child", b"a")
        fake_zk.create("/test_data_watches/child", b"a")

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

        print("Calling non related genuine child")
        genuine_zk.set("/test_data_watches/child", b"q")
        genuine_zk.set("/test_data_watches", b"q")

        print("Calling non related fake child")
        fake_zk.set("/test_data_watches/child", b"q")
        fake_zk.set("/test_data_watches", b"q")

        time.sleep(3)

        assert genuine_children == None
        assert fake_children == None

        print("Calling genuine child")
        genuine_zk.create("/test_data_watches/child_new", b"b")
        print("Calling fake child")
        fake_zk.create("/test_data_watches/child_new", b"b")

        time.sleep(3)

        print("Genuine children", genuine_children)
        print("Fake children", fake_children)
        assert genuine_children == fake_children

        genuine_children_delete = None
        def genuine_child_delete_callback(event):
            print("Genuine child watch called")
            nonlocal genuine_children_delete
            genuine_children_delete = event

        fake_children_delete = None
        def fake_child_delete_callback(event):
            print("Fake child watch called")
            nonlocal fake_children_delete
            fake_children_delete = event

        genuine_child_delete = None
        def genuine_own_delete_callback(event):
            print("Genuine child watch called")
            nonlocal genuine_child_delete
            genuine_child_delete = event

        fake_child_delete = None
        def fake_own_delete_callback(event):
            print("Fake child watch called")
            nonlocal fake_child_delete
            fake_child_delete = event

        genuine_zk.get_children("/test_data_watches", watch=genuine_child_delete_callback)
        fake_zk.get_children("/test_data_watches", watch=fake_child_delete_callback)
        genuine_zk.get_children("/test_data_watches/child", watch=genuine_own_delete_callback)
        fake_zk.get_children("/test_data_watches/child", watch=fake_own_delete_callback)

        print("Calling genuine child delete")
        genuine_zk.delete("/test_data_watches/child")
        print("Calling fake child delete")
        fake_zk.delete("/test_data_watches/child")

        time.sleep(3)

        print("Genuine children delete", genuine_children_delete)
        print("Fake children delete", fake_children_delete)
        assert genuine_children_delete == fake_children_delete

        print("Genuine child delete", genuine_child_delete)
        print("Fake child delete", fake_child_delete)
        assert genuine_child_delete == fake_child_delete

    finally:
        for zk in [genuine_zk, fake_zk]:
            stop_zk(zk)

def test_multitransactions(started_cluster):
    try:
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
    finally:
        for zk in [genuine_zk, fake_zk]:
            stop_zk(zk)

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

def generate_requests(prefix="/", iters=1):
    requests = []
    existing_paths = []
    for i in range(iters):
        for _ in range(100):
            rand_length = random.randint(0, 10)
            path = prefix
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
    try:
        requests = generate_requests("/test_random_requests", 10)
        print("Generated", len(requests), "requests")
        genuine_zk = get_genuine_zk()
        fake_zk = get_fake_zk()
        genuine_zk.create("/test_random_requests")
        fake_zk.create("/test_random_requests")
        for i, request in enumerate(requests):
            genuine_throw = False
            fake_throw = False
            fake_result = None
            genuine_result = None
            try:
                genuine_result = request.callback(genuine_zk)
            except Exception as ex:
                print("i", i, "request", request)
                print("Genuine exception", str(ex))
                genuine_throw = True

            try:
                fake_result = request.callback(fake_zk)
            except Exception as ex:
                print("i", i, "request", request)
                print("Fake exception", str(ex))
                fake_throw = True

            assert fake_throw == genuine_throw, "Fake throw genuine not or vise versa request {}"
            assert fake_result == genuine_result, "Zookeeper results differ"
        root_children_genuine = [elem for elem in list(sorted(genuine_zk.get_children("/test_random_requests"))) if elem not in ('clickhouse', 'zookeeper')]
        root_children_fake = [elem for elem in list(sorted(fake_zk.get_children("/test_random_requests"))) if elem not in ('clickhouse', 'zookeeper')]
        assert root_children_fake == root_children_genuine
    finally:
        for zk in [genuine_zk, fake_zk]:
            stop_zk(zk)

def test_end_of_session(started_cluster):

    fake_zk1 = None
    fake_zk2 = None
    genuine_zk1 = None
    genuine_zk2 = None

    try:
        fake_zk1 = KazooClient(hosts=cluster.get_instance_ip("node") + ":9181")
        fake_zk1.start()
        fake_zk2 = KazooClient(hosts=cluster.get_instance_ip("node") + ":9181")
        fake_zk2.start()
        genuine_zk1 = cluster.get_kazoo_client('zoo1')
        genuine_zk1.start()
        genuine_zk2 = cluster.get_kazoo_client('zoo1')
        genuine_zk2.start()

        fake_zk1.create("/test_end_of_session")
        genuine_zk1.create("/test_end_of_session")
        fake_ephemeral_event = None
        def fake_ephemeral_callback(event):
            print("Fake watch triggered")
            nonlocal fake_ephemeral_event
            fake_ephemeral_event = event

        genuine_ephemeral_event = None
        def genuine_ephemeral_callback(event):
            print("Genuine watch triggered")
            nonlocal genuine_ephemeral_event
            genuine_ephemeral_event = event

        assert fake_zk2.exists("/test_end_of_session") is not None
        assert genuine_zk2.exists("/test_end_of_session") is not None

        fake_zk1.create("/test_end_of_session/ephemeral_node", ephemeral=True)
        genuine_zk1.create("/test_end_of_session/ephemeral_node", ephemeral=True)

        assert fake_zk2.exists("/test_end_of_session/ephemeral_node", watch=fake_ephemeral_callback) is not None
        assert genuine_zk2.exists("/test_end_of_session/ephemeral_node", watch=genuine_ephemeral_callback) is not None

        print("Stopping genuine zk")
        genuine_zk1.stop()
        print("Closing genuine zk")
        genuine_zk1.close()

        print("Stopping fake zk")
        fake_zk1.stop()
        print("Closing fake zk")
        fake_zk1.close()

        assert fake_zk2.exists("/test_end_of_session/ephemeral_node") is None
        assert genuine_zk2.exists("/test_end_of_session/ephemeral_node") is None

        assert fake_ephemeral_event == genuine_ephemeral_event

    finally:
        for zk in [fake_zk1, fake_zk2, genuine_zk1, genuine_zk2]:
            stop_zk(zk)

def test_end_of_watches_session(started_cluster):
    fake_zk1 = None
    fake_zk2 = None
    try:
        fake_zk1 = KazooClient(hosts=cluster.get_instance_ip("node") + ":9181")
        fake_zk1.start()

        fake_zk2 = KazooClient(hosts=cluster.get_instance_ip("node") + ":9181")
        fake_zk2.start()

        fake_zk1.create("/test_end_of_watches_session")

        dummy_set = 0
        def dummy_callback(event):
            nonlocal dummy_set
            dummy_set += 1
            print(event)

        for child_node in range(100):
            fake_zk1.create("/test_end_of_watches_session/" + str(child_node))
            fake_zk1.get_children("/test_end_of_watches_session/" + str(child_node), watch=dummy_callback)

        fake_zk2.get_children("/test_end_of_watches_session/" + str(0), watch=dummy_callback)
        fake_zk2.get_children("/test_end_of_watches_session/" + str(1), watch=dummy_callback)

        fake_zk1.stop()
        fake_zk1.close()

        for child_node in range(100):
            fake_zk2.create("/test_end_of_watches_session/" + str(child_node) + "/" + str(child_node), b"somebytes")

        assert dummy_set == 2
    finally:
        for zk in [fake_zk1, fake_zk2]:
            stop_zk(zk)

def test_concurrent_watches(started_cluster):
    try:
        fake_zk = get_fake_zk()
        fake_zk.restart()
        global_path = "/test_concurrent_watches_0"
        fake_zk.create(global_path)

        dumb_watch_triggered_counter = 0
        all_paths_triggered = []

        existing_path = []
        all_paths_created = []
        watches_created = 0
        def create_path_and_watch(i):
            nonlocal watches_created
            nonlocal all_paths_created
            fake_zk.ensure_path(global_path + "/" + str(i))
            # new function each time
            def dumb_watch(event):
                nonlocal dumb_watch_triggered_counter
                dumb_watch_triggered_counter += 1
                nonlocal all_paths_triggered
                all_paths_triggered.append(event.path)

            fake_zk.get(global_path + "/" + str(i), watch=dumb_watch)
            all_paths_created.append(global_path + "/" + str(i))
            watches_created += 1
            existing_path.append(i)

        trigger_called = 0
        def trigger_watch(i):
            nonlocal trigger_called
            trigger_called += 1
            fake_zk.set(global_path + "/" + str(i), b"somevalue")
            try:
                existing_path.remove(i)
            except:
                pass

        def call(total):
            for i in range(total):
                create_path_and_watch(random.randint(0, 1000))
                time.sleep(random.random() % 0.5)
                try:
                    rand_num = random.choice(existing_path)
                    trigger_watch(rand_num)
                except:
                    pass
            while existing_path:
                try:
                    rand_num = random.choice(existing_path)
                    trigger_watch(rand_num)
                except:
                    pass

        p = Pool(10)
        arguments = [100] * 10
        watches_must_be_created = sum(arguments)
        watches_trigger_must_be_called = sum(arguments)
        watches_must_be_triggered = sum(arguments)
        p.map(call, arguments)
        p.close()

        # waiting for late watches
        for i in range(50):
            if dumb_watch_triggered_counter == watches_must_be_triggered:
                break

            time.sleep(0.1)

        assert watches_created == watches_must_be_created
        assert trigger_called >= watches_trigger_must_be_called
        assert len(existing_path) == 0
        if dumb_watch_triggered_counter != watches_must_be_triggered:
            print("All created paths", all_paths_created)
            print("All triggerred paths", all_paths_triggered)
            print("All paths len", len(all_paths_created))
            print("All triggered len", len(all_paths_triggered))
            print("Diff", list(set(all_paths_created) - set(all_paths_triggered)))

        assert dumb_watch_triggered_counter == watches_must_be_triggered
    finally:
        stop_zk(fake_zk)
