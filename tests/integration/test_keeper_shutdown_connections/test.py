import struct
import threading

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/keeper.xml"], stay_alive=True
)

int_struct = struct.Struct("!i")
int_int_long_struct = struct.Struct("!iiq")
int_long_int_long_struct = struct.Struct("!iqiq")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, [node])
        yield cluster
    finally:
        cluster.shutdown()


def open_keeper_session(session_timeout):
    client = keeper_utils.get_keeper_socket(cluster, node.name)
    try:
        request = bytearray()
        request.extend(int_long_int_long_struct.pack(0, 0, session_timeout, 0))
        request.extend(int_struct.pack(16))
        request.extend(b"\x00" * 16)
        request.extend(b"\x00")
        client.sendall(int_struct.pack(45) + request)

        response = client.recv(1000)
        protocol_version, negotiated_timeout, session_id = int_int_long_struct.unpack_from(
            response, 4
        )
        assert protocol_version == 0
        assert session_id != 0
        assert negotiated_timeout == session_timeout
        return client
    except Exception:
        client.close()
        raise


def test_idle_connection_is_closed_immediately_on_shutdown(started_cluster):
    client = open_keeper_session(session_timeout=10000)
    shutdown_errors = []

    def stop_node():
        try:
            node.stop_clickhouse()
        except Exception as ex:
            shutdown_errors.append(ex)

    shutdown_thread = threading.Thread(target=stop_node)
    shutdown_thread.start()

    try:
        client.settimeout(3)
        assert client.recv(1) == b""
    finally:
        client.close()
        shutdown_thread.join(timeout=30)
        if not shutdown_thread.is_alive():
            node.start_clickhouse()

    assert not shutdown_thread.is_alive()
    assert not shutdown_errors
