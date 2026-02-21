import logging
import os
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster


# Runs custom python-based S3 endpoint.
def run_endpoint(cluster):
    logging.info("Starting custom S3 endpoint")
    container_id = cluster.get_container_id("resolver")
    current_dir = os.path.dirname(__file__)
    cluster.copy_file_to_container(
        container_id,
        os.path.join(current_dir, "s3_endpoint", "endpoint.py"),
        "endpoint.py",
    )
    cluster.exec_in_container(container_id, ["python", "endpoint.py"], detach=True)

    # Wait for S3 endpoint start
    num_attempts = 100
    for attempt in range(num_attempts):
        ping_response = cluster.exec_in_container(
            cluster.get_container_id("resolver"),
            ["curl", "-s", "http://resolver:8080/"],
            nothrow=True,
        )
        if ping_response != "OK":
            if attempt == num_attempts - 1:
                assert ping_response == "OK", 'Expected "OK", but got "{}"'.format(
                    ping_response
                )
            else:
                time.sleep(1)
        else:
            break

    logging.info("S3 endpoint started")


def fail_request(cluster, request, method=None):
    url = "http://resolver:8080/fail_request/{}".format(request)
    if method:
        url += "/{}".format(method)
    response = cluster.exec_in_container(
        cluster.get_container_id("resolver"),
        ["curl", "-s", url],
    )
    assert response == "OK", 'Expected "OK", but got "{}"'.format(response)


def throttle_request(cluster, request):
    response = cluster.exec_in_container(
        cluster.get_container_id("resolver"),
        ["curl", "-s", "http://resolver:8080/throttle_request/{}".format(request)],
    )
    assert response == "OK", 'Expected "OK", but got "{}"'.format(response)


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/config.d/storage_conf.xml",
                "configs/config.d/instant_moves.xml",
                "configs/config.d/part_log.xml",
                "configs/config.d/merge_tree.xml",
            ],
            with_minio=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        run_endpoint(cluster)

        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_table(cluster):
    yield
    node = cluster.instances["node"]
    node.query("DROP TABLE IF EXISTS s3_failover_test SYNC")


# S3 request will be failed for an appropriate part file write.
FILES_PER_PART_BASE = 7  # partition.dat, metadata_version.txt, default_compression_codec.txt, count.txt, columns.txt, checksums.txt, columns_substreams.txt
FILES_PER_PART_WIDE = (
    FILES_PER_PART_BASE + 1 + 1 + 3 * 2
)  # Primary index, MinMax, Mark and data file for column(s)

# In debug build there are additional requests (from MergeTreeDataPartWriterWide.cpp:554 due to additional validation).
FILES_PER_PART_WIDE_DEBUG = 2  # Additional requests to S3 in debug build

FILES_PER_PART_COMPACT = FILES_PER_PART_BASE + 1 + 1 + 2
FILES_PER_PART_COMPACT_DEBUG = 0


@pytest.mark.parametrize(
    "min_bytes_for_wide_part,request_count,debug_request_count",
    [
        (0, FILES_PER_PART_WIDE, FILES_PER_PART_WIDE_DEBUG),
        (1024 * 1024, FILES_PER_PART_COMPACT, FILES_PER_PART_COMPACT_DEBUG),
    ],
)
def test_write_failover(
    cluster, min_bytes_for_wide_part, request_count, debug_request_count
):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE s3_failover_test (
            dt Date,
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        PARTITION BY dt
        SETTINGS storage_policy='s3', min_bytes_for_wide_part={}, write_marks_for_substreams_in_compact_parts=1
        """.format(
            min_bytes_for_wide_part
        )
    )

    first_success_request = None
    success_count = 0

    for request in range(request_count + debug_request_count + 1):
        # Fail N-th PUT request to S3.
        # Use PUT method filter to avoid counting background GET/DELETE
        # requests (e.g. cleanup of failed parts) that could consume the
        # fail counter and make the test flaky.
        fail_request(cluster, request + 1, "PUT")

        data = "('2020-03-01',0,'data'),('2020-03-01',1,'data')"
        try:
            node.query("INSERT INTO s3_failover_test VALUES {}".format(data))

            if first_success_request is None:
                first_success_request = request

            # INSERT must not succeed too early - all part files must have been written.
            assert request >= request_count, (
                "Insert query should have failed, request {} "
                "(expected at least {} S3 requests)".format(request, request_count)
            )
            success_count += 1
        except QueryRuntimeException as e:
            # INSERT must not fail after it has already succeeded at a lower request number.
            assert first_success_request is None, (
                "Insert query shouldn't have failed at request {} "
                "(first success was at request {})".format(request, first_success_request)
            )

            assert str(e).find("Expected Error") != -1, "Unexpected error {}".format(
                str(e)
            )

        if first_success_request is not None:
            # Disable request failing.
            fail_request(cluster, 0)

            assert node.query("CHECK TABLE s3_failover_test SETTINGS check_query_single_value_result = 1") == "1\n"
            assert (
                success_count > 1
                or node.query("SELECT * FROM s3_failover_test FORMAT Values") == data
            )

    assert first_success_request is not None, (
        "Insert query should have succeeded at least once"
    )
    assert first_success_request <= request_count + debug_request_count, (
        "Insert query succeeded too late at request {}, "
        "expected at most {} S3 requests".format(
            first_success_request, request_count + debug_request_count
        )
    )


# Check that second data part move is ended successfully if first attempt was failed.
def test_move_failover(cluster):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE s3_failover_test (
            dt DateTime,
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        TTL dt + INTERVAL 4 SECOND TO VOLUME 'external'
        SETTINGS storage_policy='s3_cold', temporary_directories_lifetime=1,
        merge_tree_clear_old_temporary_directories_interval_seconds=1
        """
    )

    # Fail the first PUT request to S3 to break first TTL move.
    # Use PUT method filter to avoid counting background GET/DELETE
    # requests that could consume the fail counter and make the test flaky.
    fail_request(cluster, 1, "PUT")

    node.query(
        "INSERT INTO s3_failover_test VALUES (now() - 1, 0, 'data'), (now() - 1, 1, 'data')"
    )

    # Wait for part move to S3.
    max_attempts = 10
    for attempt in range(max_attempts + 1):
        disk = node.query(
            "SELECT disk_name FROM system.parts WHERE table='s3_failover_test' LIMIT 1"
        )
        if disk != "s3\n":
            if attempt == max_attempts:
                assert disk == "s3\n", (
                    "Expected move to S3 while part still on disk " + disk
                )
            else:
                time.sleep(1)
        else:
            break

    # Ensure part_log is created.
    node.query("SYSTEM FLUSH LOGS")

    # There should be 2 attempts to move part.
    assert (
        node.query(
            """
        SELECT count(*) FROM system.part_log
        WHERE event_type='MovePart' AND table_uuid=(select uuid from system.tables where name='s3_failover_test' and database=currentDatabase()) AND database=currentDatabase()
        """
        )
        == "2\n"
    )

    # First attempt should be failed with expected error.
    exception = node.query(
        """
        SELECT exception FROM system.part_log
        WHERE event_type='MovePart' AND table_uuid=(select uuid from system.tables where name='s3_failover_test' and database=currentDatabase()) AND notEmpty(exception) AND database=currentDatabase()
        ORDER BY event_time
        LIMIT 1
        """
    )
    assert exception.find("Expected Error") != -1, exception

    # Ensure data is not corrupted.
    assert node.query("CHECK TABLE s3_failover_test SETTINGS check_query_single_value_result = 1") == "1\n"
    assert (
        node.query("SELECT id,data FROM s3_failover_test FORMAT Values")
        == "(0,'data'),(1,'data')"
    )

    node.query("DROP TABLE s3_failover_test")


# Check that throttled request retries and does not cause an error on disk with default `retry_attempts` (>0)
def test_throttle_retry(cluster):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE s3_throttle_retry_test (
            id Int64
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='s3_retryable'
        """
    )

    data = "(42)"
    node.query("INSERT INTO s3_throttle_retry_test VALUES {}".format(data))

    throttle_request(cluster, 1)

    assert (
        node.query(
            """
        SELECT * FROM s3_throttle_retry_test
        """
        )
        == "42\n"
    )

    node.query("DROP TABLE s3_throttle_retry_test")


# Check that loading of parts is retried.
def test_retry_loading_parts(cluster):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE s3_retry_loading_parts (
            id Int64
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='s3_no_retries'
        """
    )

    node.query("INSERT INTO s3_retry_loading_parts VALUES (42)")
    node.query("DETACH TABLE s3_retry_loading_parts")

    fail_request(cluster, 5)
    node.query("ATTACH TABLE s3_retry_loading_parts")

    assert node.contains_in_log(
        "Failed to load data part all_1_1_0 at try 0 with retryable error"
    )
    assert node.query("SELECT * FROM s3_retry_loading_parts") == "42\n"
    node.query("DROP TABLE s3_retry_loading_parts")
