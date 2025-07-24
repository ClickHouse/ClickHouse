import logging
import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def run_sts_mock(started_cluster):
    script_dir = os.path.join(os.path.dirname(__file__), "mocks")
    start_mock_servers(
        started_cluster,
        script_dir,
        [
            ("mock_sts.py", "resolver", "8081"),
        ],
    )


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    try:
        cluster.add_instance(
            "node1",
            with_minio=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        run_sts_mock(cluster)
        yield cluster

    finally:
        cluster.shutdown()


def test_using_assumed_creds(started_cluster):
    instance = started_cluster.instances["node1"]

    # Create some file in non public-accessible minio
    instance.query(
        """
            INSERT INTO FUNCTION s3
                (
                    'http://minio1:9001/root/test_assume.csv', 'minio', 'ClickHouse_Minio_P@ssw0rd', 'CSVWithNames'
                )
            SELECT number as num, toString(number) as strnum FROM numbers(5);
        """
    )

    # Read them using credentials received from our fake STS
    r = instance.query(
        """
            SELECT count() FROM s3
                ('http://minio1:9001/root/test_assume.csv',
                    SOME_FAKE_ID, SOME_FAKE_SECRET, 'CSVWithNames',
                    extra_credentials(
                        role_arn = 'arn:aws:iam::111111111111:role/BucketAccessRole-001',
                        sts_endpoint_override = 'http://resolver:8081'
                    )
                )
        """
    )

    assert r == "5\n"


