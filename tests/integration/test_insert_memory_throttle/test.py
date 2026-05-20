import uuid
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/memory_limit.xml"],
    mem_limit="600m",
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup(started_cluster):
    node.query("DROP TABLE IF EXISTS src")
    node.query("DROP TABLE IF EXISTS dst")
    yield
    node.query("DROP TABLE IF EXISTS src")
    node.query("DROP TABLE IF EXISTS dst")


def create_tables():
    node.query(
        """
        CREATE TABLE src (x UInt64, s String DEFAULT repeat('x', 1000)) ENGINE = MergeTree ORDER BY x;
        CREATE TABLE dst (x UInt64, s String) ENGINE = MergeTree ORDER BY x;
        INSERT INTO src (x) SELECT number FROM numbers(100000);
        """
    )


def run_insert(query_id, enable_throttle, memory_limit="300M"):
    node.query(
        f"""
        INSERT INTO dst SELECT * FROM src
        SETTINGS
            enable_insert_memory_throttle = {int(enable_throttle)},
            max_insert_threads = 4,
            max_memory_usage = '{memory_limit}'
        """,
        query_id=query_id,
    )


def get_throttle_event_value():
    raw = node.query(
        "SELECT value FROM system.events WHERE event = 'InsertPipelineThrottled'"
    ).strip()
    return int(raw) if raw else 0


def test_throttle_event_fires_when_enabled(started_cluster):
    create_tables()
    qid = f"throttle_parallel_{uuid.uuid4().hex}"

    before = get_throttle_event_value()
    run_insert(qid, enable_throttle=True)
    after = get_throttle_event_value()

    assert after != before, \
        "InsertPipelineThrottled should have changed under memory pressure"
    assert node.query("SELECT count() FROM dst").strip() == "100000"


def test_throttle_event_unchanged_when_disabled(started_cluster):
    create_tables()
    qid = f"no_throttle_{uuid.uuid4().hex}"

    before = get_throttle_event_value()
    run_insert(qid, enable_throttle=False)
    after = get_throttle_event_value()

    assert after == before, \
        "InsertPipelineThrottled must not change when the throttle is disabled"
    assert node.query("SELECT count() FROM dst").strip() == "100000"


def test_data_correctness_under_throttle(started_cluster):
    """All rows and checksums survive the throttled pipeline."""
    create_tables()
    qid = f"correctness_{uuid.uuid4().hex}"

    run_insert(qid, enable_throttle=True)

    ok = node.query(
        "SELECT (SELECT sum(x) FROM dst) = (SELECT sum(x) FROM src)"
    ).strip()
    assert ok == "1", "Checksum mismatch between src and dst"

    ok = node.query(
        "SELECT (SELECT count() FROM dst) = (SELECT count() FROM src)"
    ).strip()
    assert ok == "1", "Row count mismatch between src and dst"
