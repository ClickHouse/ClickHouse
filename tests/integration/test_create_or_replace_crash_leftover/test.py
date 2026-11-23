# On an `Atomic` database, `CREATE TABLE ... AS SELECT` and `CREATE OR REPLACE TABLE` first create a hidden
# table `_tmp_replace_<hash>_<suffix>`, fill it, and then rename it to the final name. If the server is
# killed before that rename, the hidden table must not come back as an ordinary table at the next start:
# it must be dropped (its metadata moved to `metadata_dropped` and its data removed). The kill is done at the `create_or_replace_before_rename`
# failpoint, right before the rename.

import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    stay_alive=True,
    with_zookeeper=True,
)

FAILPOINT = "create_or_replace_before_rename"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_database(prefix):
    db = f"{prefix}_{uuid.uuid4().hex[:8]}"
    node.query(f"CREATE DATABASE {db} ENGINE = Atomic")
    return db


def tmp_tables(db):
    return node.query(
        f"SELECT name FROM system.tables WHERE database = '{db}' AND startsWith(name, '_tmp_replace_') ORDER BY name"
    )


def run_until_failpoint(query):
    """Starts `query` in the background and returns once it is paused right before the rename."""
    # The server creates the union tables of the system logs (such as `system.all_query_log`) with
    # `CREATE OR REPLACE` at the first flush, which must not be the query that stops at the failpoint.
    node.query("SYSTEM FLUSH LOGS")
    node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    request = node.get_query_request(query, ignore_error=True)
    node.query(f"SYSTEM WAIT FAILPOINT {FAILPOINT} PAUSE", timeout=60)
    return request


def kill_and_restart(request):
    node.restart_clickhouse(kill=True)
    # The client of the killed query gets a connection error, which is expected.
    request.get_answer_and_error()


def crash_before_rename(query):
    kill_and_restart(run_until_failpoint(query))


def test_ctas_crash(started_cluster):
    db = create_database("ctas_crash")
    ctas = f"CREATE TABLE {db}.big ENGINE = MergeTree ORDER BY number AS SELECT number FROM numbers(100000)"

    crash_before_rename(ctas)

    assert tmp_tables(db) == ""
    assert node.query(f"EXISTS TABLE {db}.big") == "0\n"

    node.query(ctas)
    assert node.query(f"SELECT count() FROM {db}.big") == "100000\n"

    node.query(f"DROP DATABASE {db} SYNC")


def test_create_or_replace_crash_keeps_old_table(started_cluster):
    db = create_database("replace_crash")
    node.query(f"CREATE TABLE {db}.dst (number UInt64) ENGINE = MergeTree ORDER BY number")
    node.query(f"INSERT INTO {db}.dst VALUES (1), (2), (3)")

    crash_before_rename(
        f"CREATE OR REPLACE TABLE {db}.dst ENGINE = MergeTree ORDER BY number AS SELECT number FROM numbers(1000)"
    )

    assert node.query(f"SELECT count() FROM {db}.dst") == "3\n"
    assert tmp_tables(db) == ""

    node.query(f"DROP DATABASE {db} SYNC")


def test_replicated_merge_tree_leftover(started_cluster):
    db = create_database("rmt_crash")
    ctas = (
        f"CREATE TABLE {db}.big ENGINE = ReplicatedMergeTree('/clickhouse/tables/{db}/big', 'r1') "
        "ORDER BY number AS SELECT number FROM numbers(100000)"
    )

    crash_before_rename(ctas)

    assert tmp_tables(db) == ""
    # The leftover still holds its replica in Keeper until it is really dropped.
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.dropped_tables WHERE database = '{db}'",
        "0\n",
        retry_count=60,
        sleep_time=1,
    )

    # Without the drop of the leftover, this fails with `REPLICA_ALREADY_EXISTS`.
    node.query(ctas)
    assert node.query(f"SELECT count() FROM {db}.big") == "100000\n"

    node.query(f"DROP DATABASE {db} SYNC")


def test_leftover_data_directory_is_removed(started_cluster):
    db = create_database("data_dir_crash")
    request = run_until_failpoint(
        f"CREATE TABLE {db}.big ENGINE = MergeTree ORDER BY number AS SELECT number FROM numbers(100000)"
    )
    leftover_uuid = node.query(
        f"SELECT uuid FROM system.tables WHERE database = '{db}' AND startsWith(name, '_tmp_replace_')"
    ).strip()
    assert leftover_uuid != ""
    data_path = f"/var/lib/clickhouse/store/{leftover_uuid[:3]}/{leftover_uuid}"
    check_path = ["bash", "-c", f"test -e {data_path} && echo exists || echo removed"]
    assert node.exec_in_container(check_path) == "exists\n"

    kill_and_restart(request)

    assert_eq_with_retry(
        node,
        "SELECT 1",
        "removed\n",
        get_result=lambda _: node.exec_in_container(check_path),
        retry_count=60,
        sleep_time=1,
    )

    node.query(f"DROP DATABASE {db} SYNC")
