import signal
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage.xml", "configs/transactions.xml"],
    user_configs=["configs/users.xml"],
    with_minio=True,
    with_zookeeper=True,
    # Keep system-log writes off S3 so the read failpoint belongs to the rollback.
    with_remote_database_disk=False,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("cancel_method", ["cancel-packet", "disconnect"])
@pytest.mark.parametrize("partial_result_on_first_cancel", [0, 1])
def test_native_insert_cancellation_rolls_back_s3_part(
    started_cluster, cancel_method, partial_result_on_first_cancel
):
    table = f"txn_cancel_{uuid.uuid4().hex}"
    query_id = uuid.uuid4().hex
    part_failpoint = "merge_tree_sink_after_commit_part"
    read_failpoint = "s3_read_before_get_object"
    request = None
    enabled_failpoints = []

    node.query(
        f"CREATE TABLE {table} (n UInt64) ENGINE=MergeTree ORDER BY n SETTINGS "
        "storage_policy='s3', prewarm_mark_cache=0, prewarm_primary_key_cache=0"
    )
    node.query(f"SYSTEM STOP MERGES {table}")
    node.query(f"INSERT INTO {table} VALUES (1)")
    original_pid = node.get_process_pid("clickhouse server")
    assert original_pid is not None

    try:
        # Register a persisted part in an uncommitted transaction before cancelling.
        node.query(f"SYSTEM ENABLE FAILPOINT {part_failpoint}")
        enabled_failpoints.append(part_failpoint)
        request = node.get_query_request(
            f"INSERT INTO {table} SELECT 42 SETTINGS implicit_transaction=1, "
            "max_threads=1, max_insert_threads=1, "
            f"partial_result_on_first_cancel={partial_result_on_first_cancel}, "
            "apply_mutations_on_fly=0",
            query_id=query_id,
            timeout=180,
        )
        node.query(f"SYSTEM WAIT FAILPOINT {part_failpoint} PAUSE", timeout=60)
        part, tid = node.query(
            "SELECT name, creation_tid FROM system.parts "
            f"WHERE database=currentDatabase() AND table='{table}' AND active "
            "AND creation_csn=0"
        ).strip().split("\t")
        assert (
            node.query(f"SELECT n FROM {table} ORDER BY n SETTINGS implicit_transaction=1")
            == "1\n"
        )

        # The handler records cancellation while the pipeline still owns the part.
        if cancel_method == "cancel-packet":
            request.process.send_signal(signal.SIGINT)
        else:
            request.process.kill()
        assert_eq_with_retry(
            node,
            f"SELECT is_cancelled FROM system.processes WHERE query_id='{query_id}'",
            "1",
            retry_count=40,
            sleep_time=0.25,
        )

        # Only the rollback should need an S3 read now. Observe it before `GetObject`,
        # then let the cancelled query read its version metadata under the blocker.
        node.query(f"SYSTEM ENABLE FAILPOINT {read_failpoint}")
        enabled_failpoints.append(read_failpoint)
        node.query(f"SYSTEM NOTIFY FAILPOINT {part_failpoint}")
        node.query(f"SYSTEM WAIT FAILPOINT {read_failpoint} PAUSE", timeout=60)
        assert (
            node.query(f"SELECT state FROM system.transactions WHERE tid={tid}").strip()
            == "ROLLED_BACK"
        )
        node.query(f"SYSTEM NOTIFY FAILPOINT {read_failpoint}")

        answer, error = request.get_answer_and_error()
        if cancel_method == "cancel-packet":
            assert answer == "", answer
            assert error == "", error

        assert_eq_with_retry(
            node,
            f"SELECT count() FROM system.processes WHERE query_id='{query_id}'",
            "0",
        )
        assert node.get_process_pid("clickhouse server") == original_pid
        assert node.query("SELECT 1") == "1\n"
        assert node.query(f"SELECT count() FROM system.transactions WHERE tid={tid}") == "0\n"
        assert (
            node.query(f"SELECT n FROM {table} ORDER BY n SETTINGS implicit_transaction=1")
            == "1\n"
        )
        assert (
            node.query(
                "SELECT count() FROM system.parts "
                f"WHERE database=currentDatabase() AND table='{table}' AND name='{part}' AND active"
            )
            == "0\n"
        )

        node.query("SYSTEM FLUSH LOGS")
        assert (
            node.query(
                "SELECT count() FROM system.transactions_info_log "
                f"WHERE tid={tid} AND type='Rollback'"
            )
            == "1\n"
        )
        assert (
            int(
                node.query(
                    "SELECT count() FROM system.blob_storage_log "
                    f"WHERE query_id='{query_id}' AND event_type='Read' AND error_code=0 "
                    f"AND endsWith(local_path, '/{part}/txn_version.txt')"
                )
            )
            > 0
        )

        node.query(f"INSERT INTO {table} VALUES (7)")
        assert (
            node.query(f"SELECT n FROM {table} ORDER BY n SETTINGS implicit_transaction=1")
            == "1\n7\n"
        )
    finally:
        # Release workers before waiting for or terminating the native client.
        for failpoint in enabled_failpoints:
            node.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")
            node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        if request is not None and request.process.poll() is None:
            request.process.kill()
            request.process.wait(timeout=10)
        node.query(f"DROP TABLE {table} SYNC")
