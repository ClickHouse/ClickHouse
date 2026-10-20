from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", user_configs=["configs/allow_time_series.xml"]
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        node.query("CREATE DATABASE test ENGINE = Atomic")
        yield
    finally:
        cluster.shutdown()


def create_owner(name):
    node.query(
        f"""
        CREATE TABLE test.{name}_owner ENGINE = TimeSeries
        SETTINGS version = 11, recent_samples_ttl_seconds = 0
        """
    )
    # Resolve the `Atomic` inner target from metadata, even when it has no parts.
    samples_tables = node.query(
        f"""
        SELECT name
        FROM system.tables
        WHERE database = 'test'
            AND name = concat('.inner_id.samples.', (
                SELECT toString(uuid)
                FROM system.tables
                WHERE database = 'test' AND name = '{name}_owner'
            ))
        """
    ).splitlines()
    assert len(samples_tables) == 1, samples_tables
    return f"test.`{samples_tables[0]}`"


def run_race(samples_table, failpoint, query, alter_command=None):
    node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    paused = False
    released = False
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(node.query_and_get_answer_with_error, query, timeout=60)
        try:
            node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=30)
            paused = True
            node.query(
                alter_command
                or f"""
                ALTER TABLE {samples_table} MODIFY COLUMN samples
                Array(Tuple(DateTime64(3), Float64))
                """
            )
            node.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")
            released = True
            return future.result(timeout=60)
        finally:
            try:
                if paused and not released:
                    node.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")
            finally:
                node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")


def test_insert_revalidates_bound_samples_target():
    samples_table = create_owner("insert")
    answer, error = run_race(
        samples_table,
        "time_series_insert_pause_after_target_validation",
        """
        INSERT INTO test.insert_owner (metric_name, tags, samples)
        VALUES ('metric', map(), [(toDateTime64(1000, 3), 1.)])
        """,
    )
    assert answer == ""
    assert "BAD_TYPE_OF_FIELD" in error
    assert node.query(f"SELECT count() FROM {samples_table}") == "0\n"


def test_read_revalidates_bound_samples_target():
    samples_table = create_owner("read")
    answer, error = run_race(
        samples_table,
        "time_series_read_pause_after_target_validation",
        "SELECT samples FROM test.read_owner LIMIT 1",
    )
    assert answer == ""
    assert "BAD_TYPE_OF_FIELD" in error


def test_native_selector_revalidates_bound_samples_target():
    samples_table = create_owner("native")
    node.query(
        """
        INSERT INTO test.native_owner (metric_name, tags, samples)
        VALUES ('metric', map(), [(toDateTime64(990, 3), 1.), (toDateTime64(1000, 3), 2.)])
        """
    )
    answer, error = run_race(
        samples_table,
        "time_series_selector_pause_after_target_metadata",
        """
        SELECT count() FROM prometheusQueryRange(
            test.native_owner, 'rate(metric[20s])', 1000, 1010, 10)
        SETTINGS enable_promql_native_plan = 1
        """,
    )
    assert answer == ""
    assert "BAD_TYPE_OF_FIELD" in error
    assert node.query(f"SELECT count() FROM {samples_table}") == "1\n"


def test_insert_rejects_metadata_change_after_initial_validation():
    samples_table = create_owner("metadata")
    answer, error = run_race(
        samples_table,
        "time_series_insert_pause_after_target_validation",
        """
        INSERT INTO test.metadata_owner (metric_name, tags, samples)
        VALUES ('metric', map(), [(toDateTime64(1000, 3), 1.)])
        """,
        f"ALTER TABLE {samples_table} COMMENT COLUMN samples 'changed during planning'",
    )
    assert answer == ""
    assert "UNFINISHED" in error
    assert node.query(f"SELECT count() FROM {samples_table}") == "0\n"
