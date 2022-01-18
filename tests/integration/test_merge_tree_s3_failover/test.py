import logging
import os
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster


# Runs custom python-based S3 endpoint.
def run_endpoint(cluster):
    logging.info("Starting custom S3 endpoint")
    container_id = cluster.get_container_id('resolver')
    current_dir = os.path.dirname(__file__)
    cluster.copy_file_to_container(container_id, os.path.join(current_dir, "s3_endpoint", "endpoint.py"), "endpoint.py")
    cluster.exec_in_container(container_id, ["python", "endpoint.py"], detach=True)

    # Wait for S3 endpoint start
    num_attempts = 100
    for attempt in range(num_attempts):
        ping_response = cluster.exec_in_container(cluster.get_container_id('resolver'),
                                  ["curl", "-s", "http://resolver:8080/"], nothrow=True)
        if ping_response != 'OK':
            if attempt == num_attempts - 1:
                assert ping_response == 'OK', 'Expected "OK", but got "{}"'.format(ping_response)
            else:
                time.sleep(1)
        else:
            break

    logging.info("S3 endpoint started")


def fail_request(cluster, request):
    response = cluster.exec_in_container(cluster.get_container_id('resolver'),
                                         ["curl", "-s", "http://resolver:8080/fail_request/{}".format(request)])
    assert response == 'OK', 'Expected "OK", but got "{}"'.format(response)


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node",
                             main_configs=["configs/config.d/storage_conf.xml",
                                           "configs/config.d/instant_moves.xml",
                                           "configs/config.d/part_log.xml"],
                             with_minio=True)
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
    node.query("DROP TABLE IF EXISTS s3_failover_test NO DELAY")


# S3 request will be failed for an appropriate part file write.
FILES_PER_PART_BASE = 5  # partition.dat, default_compression_codec.txt, count.txt, columns.txt, checksums.txt
FILES_PER_PART_WIDE = FILES_PER_PART_BASE + 1 + 1 + 3 * 2  # Primary index, MinMax, Mark and data file for column(s)
FILES_PER_PART_COMPACT = FILES_PER_PART_BASE + 1 + 1 + 2


@pytest.mark.parametrize(
    "min_bytes_for_wide_part,request_count",
    [
        (0, FILES_PER_PART_WIDE),
        (1024 * 1024, FILES_PER_PART_COMPACT)
    ]
)
def test_write_failover(cluster, min_bytes_for_wide_part, request_count):
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
        SETTINGS storage_policy='s3', min_bytes_for_wide_part={}
        """
        .format(min_bytes_for_wide_part)
    )

    for request in range(request_count + 1):
        # Fail N-th request to S3.
        fail_request(cluster, request + 1)

        data = "('2020-03-01',0,'data'),('2020-03-01',1,'data')"
        positive = request == request_count
        try:
            node.query("INSERT INTO s3_failover_test VALUES {}".format(data))

            assert positive, "Insert query should be failed, request {}".format(request)
        except QueryRuntimeException as e:
            assert not positive, "Insert query shouldn't be failed, request {}".format(request)
            assert str(e).find("Expected Error") != -1, "Unexpected error {}".format(str(e))

        if positive:
            # Disable request failing.
            fail_request(cluster, 0)

            assert node.query("CHECK TABLE s3_failover_test") == '1\n'
            assert node.query("SELECT * FROM s3_failover_test FORMAT Values") == data


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
        TTL dt + INTERVAL 3 SECOND TO VOLUME 'external'
        SETTINGS storage_policy='s3_cold'
        """
    )

    # Fail a request to S3 to break first TTL move.
    fail_request(cluster, 1)

    node.query("INSERT INTO s3_failover_test VALUES (now() - 2, 0, 'data'), (now() - 2, 1, 'data')")

    # Wait for part move to S3.
    max_attempts = 10
    for attempt in range(max_attempts + 1):
        disk = node.query("SELECT disk_name FROM system.parts WHERE table='s3_failover_test' LIMIT 1")
        if disk != "s3\n":
            if attempt == max_attempts:
                assert disk == "s3\n", "Expected move to S3 while part still on disk " + disk
            else:
                time.sleep(1)
        else:
            break

    # Ensure part_log is created.
    node.query("SYSTEM FLUSH LOGS")

    # There should be 2 attempts to move part.
    assert node.query("""
        SELECT count(*) FROM system.part_log
        WHERE event_type='MovePart' AND table='s3_failover_test'
        """) == '2\n'

    # First attempt should be failed with expected error.
    exception = node.query("""
        SELECT exception FROM system.part_log
        WHERE event_type='MovePart' AND table='s3_failover_test' AND notEmpty(exception)
        ORDER BY event_time
        LIMIT 1
        """)
    assert exception.find("Expected Error") != -1, exception

    # Ensure data is not corrupted.
    assert node.query("CHECK TABLE s3_failover_test") == '1\n'
    assert node.query("SELECT id,data FROM s3_failover_test FORMAT Values") == "(0,'data'),(1,'data')"
