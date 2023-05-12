import pytest
from helpers.cluster import ClickHouseCluster
import time
import socket
import struct

from kazoo.client import KazooClient

# from kazoo.protocol.serialization import Connect, read_buffer, write_buffer

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/keeper_config.xml"], stay_alive=True
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


def wait_node(node):
    for _ in range(100):
        zk = None
        try:
            zk = get_fake_zk(node.name, timeout=30.0)
            print("node", node.name, "ready")
            break
        except Exception as ex:
            time.sleep(0.2)
            print("Waiting until", node.name, "will be ready, exception", ex)
        finally:
            destroy_zk_client(zk)
    else:
        raise Exception("Can't wait node", node.name, "to become ready")


def wait_nodes():
    for n in [node1]:
        wait_node(n)


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def get_keeper_socket(node_name):
    hosts = cluster.get_instance_ip(node_name)
    client = socket.socket()
    client.settimeout(10)
    client.connect((hosts, 9181))
    return client


def close_keeper_socket(cli):
    if cli is not None:
        cli.close()


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
