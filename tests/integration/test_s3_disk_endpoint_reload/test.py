import os
import uuid

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/s3.xml"],
    with_minio=True,
)

S3_ENDPOINT = "http://minio1:9001/root/endpoint_reload/"
S3_ENDPOINT_RELOAD_CONFIG = f"""        <endpoint_reload>
            <endpoint>{S3_ENDPOINT}</endpoint>
            <access_key_id>minio</access_key_id>
            <secret_access_key>ClickHouse_Minio_P@ssw0rd</secret_access_key>
        </endpoint_reload>
"""
S3_DISK_RELOAD_CREDENTIALS_CONFIG = """                <access_key_id>minio</access_key_id>
                <secret_access_key>ClickHouse_Minio_P@ssw0rd</secret_access_key>
"""


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def set_s3_reload_config(endpoint_enabled, disk_credentials_enabled):
    config_path = os.path.join(
        os.path.dirname(__file__),
        cluster.instances_dir_name,
        "node/configs/config.d/s3.xml",
    )
    with open(config_path, encoding="utf-8") as config:
        contents = config.read()

    contents = contents.replace(S3_ENDPOINT_RELOAD_CONFIG, "")
    if endpoint_enabled:
        contents = contents.replace(
            "    </s3>\n", S3_ENDPOINT_RELOAD_CONFIG + "    </s3>\n"
        )

    contents = contents.replace(S3_DISK_RELOAD_CREDENTIALS_CONFIG, "")
    if disk_credentials_enabled:
        disk_endpoint_config = f"                <endpoint>{S3_ENDPOINT}</endpoint>\n"
        contents = contents.replace(
            disk_endpoint_config,
            disk_endpoint_config + S3_DISK_RELOAD_CREDENTIALS_CONFIG,
        )

    with open(config_path, "w", encoding="utf-8") as config:
        config.write(contents)


def test_s3_disk_endpoint_credentials_are_revoked_on_reload():
    table_name = f"s3_endpoint_reload_{uuid.uuid4().hex}"

    try:
        node.query(
            f"""
            CREATE TABLE {table_name} (x UInt64)
            ENGINE = MergeTree ORDER BY tuple()
            SETTINGS storage_policy = 's3_endpoint_reload'
            """
        )
        node.query(f"INSERT INTO {table_name} VALUES (1)")

        set_s3_reload_config(endpoint_enabled=True, disk_credentials_enabled=False)
        node.query("SYSTEM RELOAD CONFIG")
        node.query(f"INSERT INTO {table_name} VALUES (2)")

        set_s3_reload_config(endpoint_enabled=False, disk_credentials_enabled=False)
        node.query("SYSTEM RELOAD CONFIG")

        with pytest.raises(QueryRuntimeException) as exc_info:
            node.query(f"INSERT INTO {table_name} VALUES (3)")
        assert "Access Denied" in str(exc_info.value)
    finally:
        set_s3_reload_config(endpoint_enabled=True, disk_credentials_enabled=True)
        node.query("SYSTEM RELOAD CONFIG")
        node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
