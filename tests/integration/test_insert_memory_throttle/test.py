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


def test_throttle_limits_parallelism(started_cluster):
    create_tables()
    qid = f"throttle_parallel_{uuid.uuid4().hex}"

    run_insert(qid, enable_throttle=True)

    node.query("SYSTEM FLUSH LOGS")

    # throttle should have restricted outputs (allowed_outputs < total)
    throttle_msgs = node.query(
        f"""
        SELECT count()
        FROM system.text_log
        WHERE query_id = '{qid}'
            AND logger_name = 'InsertMemoryThrottle'
            AND message LIKE '%Throttling%allowed_outputs=%'
        """
    ).strip()

    assert int(throttle_msgs) > 0, "Expected throttle to restrict parallelism under memory pressure"

    # verify throttle limited to fewer than max outputs
    allowed = node.query(
        f"""
        SELECT extract(message, 'allowed_outputs=(\\d+)')
        FROM system.text_log
        WHERE query_id = '{qid}'
            AND logger_name = 'InsertMemoryThrottle'
            AND message LIKE '%Throttling%'
        LIMIT 1
        """
    ).strip()

    assert int(allowed) < 4, f"Expected allowed_outputs < 4 (total), got {allowed}"

    assert node.query("SELECT count() FROM dst").strip() == "100000"


def test_no_throttle_when_disabled(started_cluster):
    create_tables()
    qid = f"no_throttle_{uuid.uuid4().hex}"

    run_insert(qid, enable_throttle=False)

    node.query("SYSTEM FLUSH LOGS")

    throttle_msgs = node.query(
        f"""
        SELECT count()
        FROM system.text_log
        WHERE query_id = '{qid}'
            AND logger_name = 'InsertMemoryThrottle'
        """
    ).strip()

    assert int(throttle_msgs) == 0, "No throttle messages expected when disabled"
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
