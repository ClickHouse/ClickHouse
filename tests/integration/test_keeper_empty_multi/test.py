import struct

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper.xml"],
    stay_alive=True,
    with_zookeeper=False,
)

int_struct = struct.Struct("!i")
int_int_long_struct = struct.Struct("!iiq")
int_long_int_long_struct = struct.Struct("!iqiq")
int_long_int_struct = struct.Struct("!iqi")

MULTI_OPNUM = 14
ERROR_OPNUM = -1
XID = 1


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, [node])
        yield cluster
    finally:
        cluster.shutdown()


def open_keeper_session(session_timeout=10000):
    client = keeper_utils.get_keeper_socket(cluster, node.name)
    try:
        request = bytearray()
        request.extend(int_long_int_long_struct.pack(0, 0, session_timeout, 0))
        request.extend(int_struct.pack(16))
        request.extend(b"\x00" * 16)
        request.extend(b"\x00")
        client.sendall(int_struct.pack(45) + request)

        response = client.recv(1000)
        _, _, session_id = int_int_long_struct.unpack_from(response, 4)
        assert session_id != 0
        return client
    except Exception:
        client.close()
        raise


def test_empty_multi_request(started_cluster):
    # A `Multi` request whose subrequest list holds nothing but the terminator record - what a client
    # sends when it builds a transaction from a list that turns out to be empty. It parses, and it is
    # preprocessed into no deltas at all, so it used to be written to the changelog and then read
    # `deltas.front()` on an empty range on the raft commit thread, terminating the process - and
    # terminating it again on every restart that replayed the entry.
    client = open_keeper_session()
    try:
        body = bytearray()
        body.extend(int_struct.pack(XID))
        body.extend(int_struct.pack(MULTI_OPNUM))
        body.extend(int_struct.pack(ERROR_OPNUM))  # terminator record: op_num
        body.extend(b"\x01")  # terminator record: done
        body.extend(int_struct.pack(-1))  # terminator record: error
        client.sendall(int_struct.pack(len(body)) + bytes(body))

        # An empty successful multi response: the header, then only the terminator record.
        length = int_struct.unpack(client.recv(4))[0]
        response = client.recv(length)
        xid, zxid, error = int_long_int_struct.unpack_from(response, 0)
        assert xid == XID, xid
        assert zxid > 0, zxid
        assert error == 0, error
        assert response[int_long_int_struct.size :] == (
            int_struct.pack(ERROR_OPNUM) + b"\x01" + int_struct.pack(-1)
        )

        assert keeper_utils.send_4lw_cmd(cluster, node, "ruok") == "imok"
    finally:
        client.close()

    zk = keeper_utils.get_fake_zk(cluster, node.name)
    try:
        zk.create("/after_empty_multi", b"1")
        assert zk.get("/after_empty_multi")[0] == b"1"
        # A multi transaction with subrequests keeps working.
        zk.transaction().create("/after_empty_multi/child", b"2").commit()
        assert zk.get("/after_empty_multi/child")[0] == b"2"
    finally:
        zk.stop()
        zk.close()

    # The entry of the empty multi is in the changelog, and the restart replays it.
    node.restart_clickhouse()
    keeper_utils.wait_nodes(cluster, [node])
    assert keeper_utils.send_4lw_cmd(cluster, node, "ruok") == "imok"
