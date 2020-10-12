import logging
import os
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


# Runs custom python-based S3 endpoint.
def run_endpoint(cluster):
    logging.info("Starting custom S3 endpoint")
    container_id = cluster.get_container_id('resolver')
    current_dir = os.path.dirname(__file__)
    cluster.copy_file_to_container(container_id, os.path.join(current_dir, "s3_endpoint", "endpoint.py"), "endpoint.py")
    cluster.exec_in_container(container_id, ["python", "endpoint.py"], detach=True)

    # Wait for S3 endpoint start
    for attempt in range(10):
        ping_response = cluster.exec_in_container(cluster.get_container_id('resolver'),
                                  ["curl", "-s", "http://resolver:8080/"], nothrow=True)
        if ping_response != 'OK':
            if attempt == 9:
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
                             main_configs=["configs/config.d/log_conf.xml", "configs/config.d/storage_conf.xml"],
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
