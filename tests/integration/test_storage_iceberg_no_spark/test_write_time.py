import pytest
from helpers.config_cluster import minio_secret_key, minio_access_key
from helpers.iceberg_utils import get_uuid_str


@pytest.mark.parametrize("time_type", ["Time", "Time64"])
def test_write_time(started_cluster_iceberg_no_spark, time_type):
    node = started_cluster_iceberg_no_spark.instances["node1"]

    if time_type == "Time64":
        time_type = "Time64(6)"

    TABLE_NAME = "test_partitioning_by_time_" + get_uuid_str()

    node.query(
        f"CREATE TABLE `{TABLE_NAME}` (key {time_type}, value String) ENGINE = IcebergLocal('/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}', 'Parquet') PARTITION BY key",
        settings={"allow_experimental_insert_into_iceberg": 1, 'write_full_path_in_iceberg_metadata': 1}
        )
    node.query(
        f"INSERT INTO `{TABLE_NAME}` VALUES ('12:00:00', 'test1'), ('13:00:00', 'test2'), ('14:00:00', 'test3');",
        settings={"allow_experimental_insert_into_iceberg": 1, 'write_full_path_in_iceberg_metadata': 1}
        )

    assert node.query(f"SELECT * FROM `{TABLE_NAME}` ORDER BY key") == "12:00:00.000000\ttest1\n13:00:00.000000\ttest2\n14:00:00.000000\ttest3\n"
    assert node.query(f"SELECT * FROM `{TABLE_NAME}` WHERE key = '13:00:00.000000' ORDER BY key") == "13:00:00.000000\ttest2\n"
    assert node.query(f"SELECT * FROM `{TABLE_NAME}` WHERE key >= '13:00:00.000000' ORDER BY key") == "13:00:00.000000\ttest2\n14:00:00.000000\ttest3\n"
    assert node.query(f"SELECT * FROM `{TABLE_NAME}` WHERE key <= '13:00:00.000000' ORDER BY key") == "12:00:00.000000\ttest1\n13:00:00.000000\ttest2\n"
