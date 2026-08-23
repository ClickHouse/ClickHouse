import pytest

from helpers.iceberg_utils import (
    get_uuid_str,
)


def _rewrite_metadata(instance, metadata_path, rejected_arguments):
    """Replace the engine's `format` argument in the table's on-disk metadata
    with *rejected_arguments*, simulating a definition persisted before those
    arguments were rejected."""
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"""sed -i "s|, 'Parquet')|, {rejected_arguments})|" {metadata_path}""",
        ],
        user="root",
    )
    content = instance.exec_in_container(
        ["bash", "-c", f"cat {metadata_path}"], user="root"
    )
    # Guard against the on-disk `CREATE` serialization changing its formatting,
    # which would silently stop this test from exercising the compatibility path.
    assert f", {rejected_arguments})" in content, content


@pytest.mark.parametrize(
    "rejected_arguments",
    ["'Parquet', 'lzma'", "'Parquet', 'gzip'", "'RowBinary'"],
)
def test_attach_loads_metadata_with_rejected_argument(
    started_cluster_iceberg_no_spark, rejected_arguments
):
    """A data lake table persisted before `compression_method` and a non-lake
    `format` were rejected must still load. The rejection fires only for
    `LoadingStrictnessLevel::CREATE`, so a short `ATTACH TABLE name`, which
    replays the stored definition, is exempt."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]

    table_name = "test_datalake_compression_attach_" + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/"

    instance.query(
        f"CREATE TABLE {table_name} (c0 Int) ENGINE = IcebergLocal('{table_path}', 'Parquet')"
    )

    disk_path = instance.query(
        "SELECT path FROM system.disks WHERE name = 'default'"
    ).strip()
    relative_metadata_path = instance.query(
        f"SELECT metadata_path FROM system.tables "
        f"WHERE database = currentDatabase() AND name = '{table_name}'"
    ).strip()

    instance.query(f"DETACH TABLE {table_name}")
    _rewrite_metadata(instance, disk_path + relative_metadata_path, rejected_arguments)

    instance.query(f"ATTACH TABLE {table_name}")
    assert instance.query(f"EXISTS TABLE {table_name}").strip() == "1"

    instance.query(f"DROP TABLE {table_name}")
