import glob
import json
import os

from pyiceberg.table import StaticTable

from helpers.iceberg_utils import default_download_directory, get_uuid_str


def test_empty_sort_order(started_cluster_iceberg_with_spark):
    cluster = started_cluster_iceberg_with_spark
    node1 = cluster.instances["node1"]
    spark = cluster.spark_session

    table_name = "test_empty_sort_order_" + get_uuid_str()
    table_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"

    settings = {
        "allow_insert_into_iceberg": 1,
        "write_full_path_in_iceberg_metadata": 1,
    }

    # ClickHouse creates an Iceberg table with an explicitly empty sorting key.
    node1.query(
        f"""
        CREATE TABLE {table_name} (id Int64)
        ENGINE = IcebergLocal(
            local, path='{table_dir}', format=Parquet
        )
        ORDER BY tuple()
        """,
        settings=settings,
    )

    node1.query(
        f"INSERT INTO {table_name} VALUES (42)",
        settings=settings,
    )

    # Copy ClickHouse-written files into the pytest runner's filesystem, where Spark can access them.
    default_download_directory(
        cluster,
        "local",
        f"{table_dir}/",
        f"{table_dir}/",
    )

    # Verify the latest Iceberg metadata describes an unsorted table.
    metadata_files = glob.glob(
        os.path.join(table_dir, "metadata", "v*.metadata.json")
    )
    assert metadata_files

    metadata_path = max(
        metadata_files,
        key=lambda path: int(os.path.basename(path).split(".")[0][1:]),
    )

    with open(metadata_path) as f:
        metadata = json.load(f)

    assert metadata["default-sort-order-id"] == 0
    assert metadata["sort-orders"][0]["order-id"] == 0
    assert metadata["sort-orders"][0]["fields"] == []

    table = StaticTable.from_metadata(metadata_path)
    assert table.sort_order().is_unsorted

    rows = spark.read.format("iceberg").load(table_dir).collect()
    assert [row.id for row in rows] == [42]
