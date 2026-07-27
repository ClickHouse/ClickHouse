import struct
import time
import uuid

import pytest
from kazoo.exceptions import NoNodeError, NodeExistsError

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

# from kazoo.protocol.serialization import Connect, read_buffer, write_buffer

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/keeper_config1.xml"], stay_alive=True
)

node2 = cluster.add_instance(
    "node2", main_configs=["configs/keeper_config2.xml"], stay_alive=True
)

node3 = cluster.add_instance(
    "node3", main_configs=["configs/keeper_config3.xml"], stay_alive=True
)

bool_struct = struct.Struct("B")
int_struct = struct.Struct("!i")
int_int_struct = struct.Struct("!ii")
int_int_long_struct = struct.Struct("!iiq")

int_long_int_long_struct = struct.Struct("!iqiq")
long_struct = struct.Struct("!q")
multiheader_struct = struct.Struct("!iBi")
reply_header_struct = struct.Struct("!iqi")
stat_struct = struct.Struct("!qqqqiiiqiiq")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def destroy_zk_client(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def wait_nodes():
    keeper_utils.wait_nodes(cluster, [node1, node2, node3])


def get_fake_zk(nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def get_keeper_socket(node_name):
    return keeper_utils.get_keeper_socket(cluster, node_name)


def write_buffer(bytes):
    if bytes is None:
        return int_struct.pack(-1)
    else:
        return int_struct.pack(len(bytes)) + bytes


def read_buffer(bytes, offset):
    length = int_struct.unpack_from(bytes, offset)[0]
    offset += int_struct.size
    if length < 0:
        return None, offset
    else:
        index = offset
        offset += length
        return bytes[index : index + length], offset


def handshake(node_name=node1.name, session_timeout=1000, session_id=0):
    client = None
    try:
        client = get_keeper_socket(node_name)
        protocol_version = 0
        last_zxid_seen = 0
        session_passwd = b"\x00" * 16
        read_only = 0

        # Handshake serialize and deserialize code is from 'kazoo.protocol.serialization'.

        # serialize handshake
        req = bytearray()
        req.extend(
            int_long_int_long_struct.pack(
                protocol_version, last_zxid_seen, session_timeout, session_id
            )
        )
        req.extend(write_buffer(session_passwd))
        req.extend([1 if read_only else 0])
        # add header
        req = int_struct.pack(45) + req
        print("handshake request - len:", req.hex(), len(req))

        # send request
        client.send(req)
        data = client.recv(1_000)

        # deserialize response
        print("handshake response - len:", data.hex(), len(data))
        # ignore header
        offset = 4
        proto_version, negotiated_timeout, session_id = int_int_long_struct.unpack_from(
            data, offset
        )
        offset += int_int_long_struct.size
        password, offset = read_buffer(data, offset)
        try:
            read_only = bool_struct.unpack_from(data, offset)[0] == 1
            offset += bool_struct.size
        except struct.error:
            read_only = False

        print("negotiated_timeout - session_id", negotiated_timeout, session_id)
        return negotiated_timeout, session_id
    finally:
        if client is not None:
            client.close()


def test_session_timeout(started_cluster):
    wait_nodes()

    negotiated_timeout, _ = handshake(node1.name, session_timeout=1000, session_id=0)
    assert negotiated_timeout == 5000

    negotiated_timeout, _ = handshake(node1.name, session_timeout=8000, session_id=0)
    assert negotiated_timeout == 8000

    negotiated_timeout, _ = handshake(node1.name, session_timeout=20000, session_id=0)
    assert negotiated_timeout == 10000


def test_session_close_shutdown(started_cluster):
    wait_nodes()

    node1_zk = None
    node2_zk = None
    for i in range(20):
        node1_zk = get_fake_zk(node1.name)
        node2_zk = get_fake_zk(node2.name)

        eph_node = "/test_node"
        node2_zk.create(eph_node, ephemeral=True)
        node1_zk.sync(eph_node)

        node1_zk.exists(eph_node) != None

        # restart while session is active so it's closed during shutdown
        node2.restart_clickhouse()

        if node1_zk.exists(eph_node) == None:
            break

        assert node2.contains_in_log(
            "Sessions cannot be closed during shutdown because there is no active leader"
        )

        try:
            node1_zk.delete(eph_node)
        except NoNodeError:
            pass

        assert node1_zk.exists(eph_node) == None

        destroy_zk_client(node1_zk)
        node1_zk = None
        destroy_zk_client(node2_zk)
        node2_zk = None

        time.sleep(1)
    else:
        assert False, "Session wasn't properly cleaned up on shutdown"

def test_create2(started_cluster):
    wait_nodes()
    node1_zk = None
    node1_zk = get_fake_zk(node1.name)
    uid = str(uuid.uuid4()).replace('-', '')
    path = f"/tea_{uid}"
    _, stats = node1_zk.create(path, include_data=True)
    assert stats is not None
    data, stats_get = node1_zk.get(path)
    stats_exists = node1_zk.exists(path)
    assert stats_get is not None
    assert stats_exists is not None

    assert stats.numChildren == 0
    assert stats.ephemeralOwner == 0
    assert stats.version == 0

    assert stats_get.numChildren == stats.numChildren
    assert stats_get.ephemeralOwner == stats.ephemeralOwner
    assert stats_get.version == stats.version
    assert stats_get.czxid == stats.czxid
    assert stats_get.mzxid == stats.mzxid
    assert stats_get.pzxid == stats.pzxid
    assert stats_get.ctime == stats.ctime
    assert stats_get.mtime == stats.mtime
    assert stats_get.cversion == stats.cversion
    assert stats_get.aversion == stats.aversion

    assert stats_exists.czxid == stats.czxid
    assert stats_exists.mzxid == stats.mzxid
    assert stats_exists.pzxid == stats.pzxid
    assert stats_exists.ctime == stats.ctime
    assert stats_exists.mtime == stats.mtime
    assert stats_exists.version == stats.version
    assert stats_exists.cversion == stats.cversion
    assert stats_exists.aversion == stats.aversion
    assert stats_exists.ephemeralOwner == stats.ephemeralOwner
    assert stats_exists.numChildren == stats.numChildren

    assert stats_get.dataLength == len(data)

def test_create2_stats_match_get_and_exists(started_cluster):
    wait_nodes()
    node1_zk = None
    path = None
    try:
        node1_zk = get_fake_zk(node1.name)
        path = f"/test_create2_stats_{uuid.uuid4().hex}"
        data = b"hello-create2"

        created_path, create_stat = node1_zk.create(path, data, include_data=True)
        assert created_path == path

        got_data, get_stat = node1_zk.get(path)
        assert got_data == data

        exists_stat = node1_zk.exists(path)
        assert exists_stat is not None

        for attr in (
            "czxid",
            "mzxid",
            "ctime",
            "mtime",
            "version",
            "cversion",
            "aversion",
            "ephemeralOwner",
            "numChildren",
            "pzxid",
        ):
            assert getattr(create_stat, attr) == getattr(get_stat, attr)
            assert getattr(create_stat, attr) == getattr(exists_stat, attr)

        assert get_stat.dataLength == len(data)
        assert exists_stat.dataLength == len(data)
    finally:
        if node1_zk is not None:
            try:
                if path:
                    node1_zk.delete(path)
            except NoNodeError:
                pass
            destroy_zk_client(node1_zk)


def test_create2_tree_parent_stats(started_cluster):
    wait_nodes()
    node1_zk = None
    root = None
    children = []
    grandchild = None
    try:
        node1_zk = get_fake_zk(node1.name)
        root = f"/test_create2_tree_{uuid.uuid4().hex}"

        created_path, root_stat = node1_zk.create(root, b"root", include_data=True)
        assert created_path == root
        assert root_stat.numChildren == 0

        children = [f"{root}/child_{i}" for i in range(3)]
        for c in children:
            node1_zk.create(c, b"child", include_data=True)

        _, get_stat = node1_zk.get(root)
        exists_stat = node1_zk.exists(root)
        assert get_stat.numChildren == len(children)
        assert exists_stat.numChildren == len(children)
        assert get_stat.cversion >= root_stat.cversion + len(children)

        grandchild = f"{children[0]}/g"
        node1_zk.create(grandchild, b"g", include_data=True)

        _, root_after = node1_zk.get(root)
        assert root_after.numChildren == len(children)
        _, child0_stat = node1_zk.get(children[0])
        assert child0_stat.numChildren == 1
        assert child0_stat.cversion >= 1
    finally:
        if node1_zk is not None:
            try:
                if grandchild:
                    node1_zk.delete(grandchild)
            except NoNodeError:
                pass
            for c in children[::-1]:
                try:
                    node1_zk.delete(c)
                except NoNodeError:
                    pass
            try:
                if root:
                    node1_zk.delete(root)
            except NoNodeError:
                pass
            destroy_zk_client(node1_zk)


def test_create2_errors_existing_and_missing_parent(started_cluster):
    wait_nodes()
    node1_zk = None
    base = None
    try:
        node1_zk = get_fake_zk(node1.name)
        base = f"/test_create2_error_{uuid.uuid4().hex}"
        node1_zk.create(base, b"v1", include_data=True)

        with pytest.raises(NodeExistsError):
            node1_zk.create(base, b"v2", include_data=True)

        missing_parent_child = f"/test_create2_missing_parent_{uuid.uuid4().hex}/child"
        with pytest.raises(NoNodeError):
            node1_zk.create(missing_parent_child, b"v", include_data=True)
    finally:
        if node1_zk is not None:
            try:
                if base:
                    node1_zk.delete(base)
            except NoNodeError:
                pass
            destroy_zk_client(node1_zk)
