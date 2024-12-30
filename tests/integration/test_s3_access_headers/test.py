import logging
import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers
from helpers.s3_tools import prepare_s3_bucket

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def run_s3_mocks(started_cluster):
    script_dir = os.path.join(os.path.dirname(__file__), "s3_mocks")
    start_mock_servers(
        started_cluster,
        script_dir,
        [
            ("mocker_s3.py", "resolver", "8081"),
        ],
    )


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__, with_spark=True)
    try:
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/named_collections.xml",
                "configs/config.d/s3_headers.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        run_s3_mocks(cluster)
        yield cluster

    finally:
        cluster.shutdown()


CUSTOM_AUTH_TOKEN = "custom-auth-token"
CORRECT_TOKEN = "ValidToken1234"
INCORRECT_TOKEN = "InvalidToken1234"


@pytest.mark.parametrize(
    "table_name, engine, query_with_invalid_token_must_fail",
    [
        pytest.param(
            "test_access_header",
            "S3('http://resolver:8081/root/test_access_header.csv', 'CSV')",
            True,
            id="test_access_over_custom_header",
        ),
        pytest.param(
            "test_static_override",
            "S3('http://resolver:8081/root/test_static_override.csv', 'minio', 'minio123',  'CSV')",
            False,
            id="test_access_key_id_overrides_access_header",
        ),
        pytest.param(
            "test_named_colections",
            "S3(s3_mock, format='CSV')",
            False,
            id="test_named_coll_overrides_access_header",
        ),
    ],
)
def test_custom_access_header(
    started_cluster, table_name, engine, query_with_invalid_token_must_fail
):
    instance = started_cluster.instances["node1"]

    instance.query(
        f"""
        SET s3_truncate_on_insert=1;
        INSERT INTO FUNCTION s3('http://minio1:9001/root/{table_name}.csv', 'minio', 'minio123','CSV')
        SELECT number as a, toString(number) as b FROM numbers(3);
        """
    )
    instance.query(
        f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} (name String, value UInt32)
        ENGINE={engine};
        """
    )
    instance.query("SYSTEM DROP QUERY CACHE")

    assert instance.query(f"SELECT count(*) FROM {table_name}") == "3\n"

    config_path = "/etc/clickhouse-server/config.d/s3_headers.xml"

    instance.replace_in_config(
        config_path,
        f"<access_header>{CUSTOM_AUTH_TOKEN}: {CORRECT_TOKEN}",
        f"<access_header>{CUSTOM_AUTH_TOKEN}: {INCORRECT_TOKEN}",
    )
    instance.query("SYSTEM RELOAD CONFIG")

    if query_with_invalid_token_must_fail:
        instance.query_and_get_error(f"SELECT count(*) FROM {table_name}")

    else:
        assert instance.query(f"SELECT count(*) FROM {table_name}") == "3\n"

    instance.replace_in_config(
        config_path,
        f"<access_header>{CUSTOM_AUTH_TOKEN}: {INCORRECT_TOKEN}",
        f"<access_header>{CUSTOM_AUTH_TOKEN}: {CORRECT_TOKEN}",
    )

    instance.query("SYSTEM RELOAD CONFIG")
    assert instance.query(f"SELECT count(*) FROM {table_name}") == "3\n"
