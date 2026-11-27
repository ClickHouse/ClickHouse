import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/s3_endpoint.xml"],
    with_minio=True,
    with_remote_database_disk=False,
    stay_alive=True,
)
ENDPOINT = "http://minio1:9001/root/backups"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def first_s3_query_after_start(settings):
    # The endpoint settings are built by the first query that needs them after a start.
    node.restart_clickhouse()
    node.query(
        f"SELECT count() FROM s3('{ENDPOINT}/none/*', 'minio', '{minio_secret_key}', 'CSV', 'a String')",
        settings={**settings, "s3_throw_on_zero_files_match": 0},
    )


def backup_engine_settings(settings=None):
    backup_id = node.query(
        f"BACKUP TABLE data TO S3('{ENDPOINT}/{uuid.uuid4().hex}', 'minio', '{minio_secret_key}')",
        settings=settings,
    ).split()[0]
    return node.query(
        "SELECT engine_settings['max_inflight_parts_for_one_file'], engine_settings['min_upload_part_size'] "
        f"FROM system.backups WHERE id = '{backup_id}'"
    ).strip()


def test_backup_does_not_inherit_first_query_settings():
    first_s3_query_after_start(
        {"s3_max_inflight_parts_for_one_file": 1, "s3_min_upload_part_size": 5242880}
    )
    node.query("DROP TABLE IF EXISTS data SYNC")
    node.query("CREATE TABLE data (key UInt64) ENGINE = MergeTree ORDER BY key")
    node.query("INSERT INTO data SELECT number FROM numbers(10)")

    assert backup_engine_settings() == "20\t16777216"
    # The settings of the backup query itself still apply.
    assert (
        backup_engine_settings({"s3_max_inflight_parts_for_one_file": 3})
        == "3\t16777216"
    )


def test_valid_settings_are_not_rejected_after_first_query():
    first_s3_query_after_start({"s3_max_upload_part_size": 20971520})
    # Valid on its own (the default maximum part size is 5 GiB).
    node.query(
        f"INSERT INTO FUNCTION s3('{ENDPOINT}/{uuid.uuid4().hex}.csv', 'minio', '{minio_secret_key}', 'CSV', 'a String') SELECT 'a'",
        settings={"s3_min_upload_part_size": 25165824},
    )
