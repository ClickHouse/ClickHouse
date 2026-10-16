import json
import os
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_minio=True, with_zookeeper=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        start_mock_servers(cluster, os.path.dirname(__file__), [("s3_mock.py", "resolver", "8085")])
        node.copy_file_to_container(
            os.path.join(os.path.dirname(__file__), "configs/storage.xml"),
            "/etc/clickhouse-server/config.d/storage.xml",
        )
        node.query("SYSTEM RELOAD CONFIG")
        yield
    finally:
        cluster.shutdown()


def gate(command):
    return node.exec_in_container(["curl", "--fail", "--silent", "--show-error", "--max-time", "20", f"http://resolver:8085/gate/{command}"])


@pytest.fixture(params=["MergeTree", "ReplicatedMergeTree"])
def table(request):
    engine = request.param
    if engine == "ReplicatedMergeTree":
        engine += "('/clickhouse/{uuid}', 'r1')"
    projection = ", PROJECTION totals (SELECT p, sum(v) GROUP BY p)" if request.node.callspec.params.get("projection", False) else ""
    node.query(
        f"CREATE TABLE t UUID '{uuid.uuid4()}' (p UInt64, k UInt64, v UInt64{projection}) ENGINE = {engine} "
        "PARTITION BY p ORDER BY k SETTINGS storage_policy = 'packed_s3', "
        "min_bytes_for_full_part_storage = '32M', "
        "enable_block_number_column = 1, enable_block_offset_column = 1"
    )
    node.query("SYSTEM STOP MERGES t")
    try:
        yield
    finally:
        gate("release")
        node.query("DROP TABLE t SYNC")


INSERT = "INSERT INTO t SELECT number % 4, number, number FROM numbers(40)"
SETTINGS = {
    "max_threads": 1,
    "max_insert_threads": 1,
    "max_block_size": 100,
    "max_insert_delayed_streams_for_parallel_write": 1000,
}


def wait_for_uploads(count):
    uploaded_keys = json.loads(gate(f"wait?count={count}"))
    assert len(uploaded_keys) == count, uploaded_keys


def check_data(expected_sum=780):
    assert node.query("SELECT count(), sum(v) FROM t") == f"40\t{expected_sum}\n"
    assert node.query("CHECK TABLE t SETTINGS check_query_single_value_result = 1") == "1\n"
    node.query("DETACH TABLE t SYNC")
    node.query("ATTACH TABLE t")
    assert node.query("SELECT count(), sum(v) FROM t") == f"40\t{expected_sum}\n"


@pytest.mark.parametrize("projection", [False, True])
@pytest.mark.parametrize("multipart", [False, True])
def test_uploads_overlap(table, projection, multipart):
    gate("arm")
    settings = dict(SETTINGS)
    if multipart:
        settings["s3_max_single_part_upload_size"] = 1
    request = node.get_query_request(INSERT, settings=settings, timeout=90)
    try:
        # These are distinct packed objects, not attempts to upload the same object.
        wait_for_uploads(8 if projection else 4)
        assert json.loads(gate("multipart")) == [multipart]
        assert node.query("SELECT count() FROM system.parts WHERE table = 't' AND active") == "0\n"
    finally:
        gate("release")
        request.get_answer()
    check_data()
    if projection:
        assert node.query("SELECT p, sum(v) FROM t GROUP BY p ORDER BY p SETTINGS force_optimize_projection = 1") == "0\t180\n1\t190\n2\t200\n3\t210\n"


def test_upload_failure(table):
    gate("arm?fail=1")
    request = node.get_query_request(INSERT, settings=SETTINGS, timeout=90)
    try:
        wait_for_uploads(4)
    finally:
        gate("release")
        error = request.get_error()
    assert "Injected packed upload failure" in error
    assert node.query("SELECT count() FROM system.parts WHERE table = 't' AND active") == "0\n"
    gate("arm")
    gate("release")
    node.query(INSERT, settings=SETTINGS)
    check_data()


def test_cancel_pending_uploads(table):
    gate("arm")
    request = node.get_query_request(INSERT, settings=SETTINGS, query_id="cancel_packed_uploads", timeout=90)
    try:
        wait_for_uploads(4)
        node.query("KILL QUERY WHERE query_id = 'cancel_packed_uploads' ASYNC")
        assert_eq_with_retry(
            node,
            "SELECT is_cancelled FROM system.processes WHERE query_id = 'cancel_packed_uploads'",
            "1",
        )
    finally:
        gate("release")
        error = request.get_error()
    assert "QUERY_WAS_CANCELLED" in error
    # Cancellation may leave completed parts; every published part must be readable.
    assert node.query("CHECK TABLE t SETTINGS check_query_single_value_result = 1") == "1\n"
    node.query("TRUNCATE TABLE t")
    node.query(INSERT, settings=SETTINGS)
    check_data()


def test_patch_uploads_overlap(table):
    node.query(INSERT, settings=SETTINGS)
    gate("arm")
    request = node.get_query_request("UPDATE t SET v = v + 1 WHERE 1", settings=SETTINGS, timeout=90)
    try:
        wait_for_uploads(4)
        assert node.query("SELECT sum(v) FROM t") == "780\n"
    finally:
        gate("release")
        request.get_answer()
    check_data(820)
