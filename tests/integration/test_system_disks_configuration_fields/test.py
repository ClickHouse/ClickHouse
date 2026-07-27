import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/config.d/storage_conf.xml",
    ],
    with_minio=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_system_disks_has_configuration_fields_column(started_cluster):
    result = node.query(
        """
        SELECT type
        FROM system.columns
        WHERE database = 'system'
          AND table = 'disks'
          AND name = 'configuration_fields'
        """
    )

    assert result.strip() == "Map(String, String)"


def test_default_disk_has_empty_configuration_fields(started_cluster):
    result = node.query(
        """
        SELECT empty(configuration_fields)
        FROM system.disks
        WHERE name = 'default'
        """
    )

    assert result.strip() == "1"


def test_system_disks_configuration_fields(started_cluster):
    result = node.query(
        """
        SELECT
            configuration_fields['s3_max_get_rps'],
            configuration_fields['s3_max_put_rps'],
            configuration_fields['max_connections'],
            configuration_fields['request_timeout_ms'],
            configuration_fields['metadata_keep_free_space_bytes'],
            mapContains(configuration_fields, 'endpoint'),
            mapContains(configuration_fields, 'access_key_id'),
            mapContains(configuration_fields, 'secret_access_key')
        FROM system.disks
        WHERE name = 's3_disk'
        """
    )

    assert result.strip() == "500\t100\t10000\t3600000\t104857600\t0\t0\t0"


def test_configuration_fields_reload(started_cluster):
    before = node.query(
        """
        SELECT configuration_fields['s3_max_get_rps']
        FROM system.disks
        WHERE name = 's3_disk'
        """
    )

    assert before.strip() == "500"

    new_config = """
<clickhouse>
    <storage_configuration>
        <disks>
            <s3_disk>
                <type>s3</type>
                <endpoint>http://minio1:9001/root/data/</endpoint>
                <access_key_id>minio</access_key_id>
                <secret_access_key>minio123</secret_access_key>

                <s3_max_get_rps>777</s3_max_get_rps>
                <s3_max_put_rps>222</s3_max_put_rps>
                <max_connections>9999</max_connections>
                <request_timeout_ms>123456</request_timeout_ms>
                <metadata_keep_free_space_bytes>104857600</metadata_keep_free_space_bytes>
            </s3_disk>
        </disks>
    </storage_configuration>
</clickhouse>
"""

    node.exec_in_container(
        [
            "bash",
            "-c",
            "cat > /etc/clickhouse-server/config.d/storage_conf.xml <<'EOF'\n"
            + new_config
            + "\nEOF",
        ]
    )

    node.query("SYSTEM RELOAD CONFIG")

    after = node.query(
        """
        SELECT
            configuration_fields['s3_max_get_rps'],
            configuration_fields['s3_max_put_rps'],
            configuration_fields['max_connections'],
            configuration_fields['request_timeout_ms']
        FROM system.disks
        WHERE name = 's3_disk'
        """
    )

    assert after.strip() == "777\t222\t9999\t123456"
