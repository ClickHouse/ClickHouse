import logging

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/config.xml",
    ],
)
path_to_data = "/var/lib/clickhouse/"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_it(started_cluster):
    if node.is_built_with_sanitizer():
        # Sanitizer builds might be too slow to run even 100 queries in a reasonable time.
        pytest.skip("Disabled for sanitizers")

    for i in range(2):
        assert_eq_with_retry(
            node, f"INSERT INTO FUNCTION file('t{i}.parquet') SELECT * FROM numbers(10) SETTINGS max_threads=1, engine_file_truncate_on_insert=1", "", retry_count=100, sleep_time=0.0)

    # Parquet read with max_threads=2, max_parsing_threads=2 should grab ~4 threads from the pool,
    # checking cannot_allocate_thread_fault_injection_probability (0.07) 4 times.
    # So probability of success is 0.93^4 ~= 0.75. Probability of 100/100 successes is 2e-13.
    # There might be more cannot_allocate_thread_fault_injection_probability checks in other parts
    # of query processing, bringing success probability down, but hopefully not so many that all
    # queries fail.
    errors, successes = 0, 0
    for attempt in range(100):
        try:
            res = node.query("SELECT sum(number) FROM file('t{0,1}.parquet') SETTINGS max_threads=2, max_parsing_threads=2, max_download_threads=1")
            assert res == "90\n"
            successes += 1
        except Exception as ex:
            if "Cannot schedule a task" in str(ex):
                errors += 1
            else:
                raise
    # (As of the time of writing, this reports ~48 successes on average, suggesting that
    #  cannot_allocate_thread_fault_injection_probability is actually checked 10 times by the query.)
    logging.info(f"{successes} successes, {errors} errors")
    assert successes > 0
    assert errors > 0
