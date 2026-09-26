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
        CREATE TABLE test.{name}_samples
        (
            id UUID,
            samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3), Float64))),
            bucket DateTime64(3),
            min_time SimpleAggregateFunction(min, DateTime64(3)),
            max_time SimpleAggregateFunction(max, DateTime64(3))
        ) ENGINE = AggregatingMergeTree ORDER BY (id, bucket)
        """
    )
    node.query(
        f"""
        CREATE TABLE test.{name}_owner ENGINE = TimeSeries
        SETTINGS version = 7, recent_samples_ttl_seconds = 0
        SAMPLES test.{name}_samples
        """
    )


def run_race(name, failpoint, query, alter_command=None):
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
                ALTER TABLE test.{name}_samples MODIFY COLUMN samples
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
    create_owner("insert")
    answer, error = run_race(
        "insert",
        "time_series_insert_pause_after_target_validation",
        """
        INSERT INTO test.insert_owner (metric_name, tags, samples)
        VALUES ('metric', map(), [(toDateTime64(1000, 3), 1.)])
        """,
    )
    assert answer == ""
    assert "BAD_TYPE_OF_FIELD" in error
    assert node.query("SELECT count() FROM test.insert_samples") == "0\n"


def test_read_revalidates_bound_samples_target():
    create_owner("read")
    answer, error = run_race(
        "read",
        "time_series_read_pause_after_target_validation",
        "SELECT samples FROM test.read_owner LIMIT 1",
    )
    assert answer == ""
    assert "BAD_TYPE_OF_FIELD" in error


def test_native_selector_revalidates_bound_samples_target():
    create_owner("native")
    node.query(
        """
        INSERT INTO test.native_owner (metric_name, tags, samples)
        VALUES ('metric', map(), [(toDateTime64(990, 3), 1.), (toDateTime64(1000, 3), 2.)])
        """
    )
    answer, error = run_race(
        "native",
        "time_series_selector_pause_after_target_metadata",
        """
        SELECT count() FROM prometheusQueryRange(
            test.native_owner, 'rate(metric[20s])', 1000, 1010, 10)
        SETTINGS enable_promql_native_plan = 1
        """,
    )
    assert answer == ""
    assert "BAD_TYPE_OF_FIELD" in error
    assert node.query("SELECT count() FROM test.native_samples") == "1\n"


def test_insert_rejects_metadata_change_after_initial_validation():
    create_owner("metadata")
    answer, error = run_race(
        "metadata",
        "time_series_insert_pause_after_target_validation",
        """
        INSERT INTO test.metadata_owner (metric_name, tags, samples)
        VALUES ('metric', map(), [(toDateTime64(1000, 3), 1.)])
        """,
        "ALTER TABLE test.metadata_samples COMMENT COLUMN samples 'changed during planning'",
    )
    assert answer == ""
    assert "UNFINISHED" in error
    assert node.query("SELECT count() FROM test.metadata_samples") == "0\n"
