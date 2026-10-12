import pytest

from helpers.database_disk import (
    get_database_disk_name,
    read_metadata,
    replace_text_in_metadata,
)
from helpers.iceberg_utils import (
    get_uuid_str,
)


def _rewrite_metadata(instance, metadata_path, rejected_arguments):
    """Replace the engine's `format` argument in the table's on-disk metadata
    with *rejected_arguments*, simulating a definition persisted before those
    arguments were rejected."""
    # The definition lives on the database disk, which is a remote object storage in the
    # "db disk" configuration, so it has to be edited through the `clickhouse disks` CLI
    # rather than by touching a path under `/var/lib/clickhouse` directly.
    old_value = ", 'Parquet')"
    new_value = f", {rejected_arguments})"
    # Fail closed on both sides: `str.replace` is a silent no-op once the on-disk `CREATE`
    # serialization drifts, which would leave the test asserting nothing.
    assert old_value in read_metadata(instance, metadata_path), (
        f"persisted metadata does not contain '{old_value}'; "
        "the test would silently stop exercising the compatibility path"
    )
    replace_text_in_metadata(instance, metadata_path, old_value, new_value)
    assert new_value in read_metadata(instance, metadata_path)


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

    metadata_path = instance.query(
        f"SELECT metadata_path FROM system.tables "
        f"WHERE database = currentDatabase() AND name = '{table_name}'"
    ).strip()

    instance.query(f"DETACH TABLE {table_name}")
    _rewrite_metadata(instance, metadata_path, rejected_arguments)

    # The rewrite bypassed the server, so its cached view of the file is stale and `ATTACH`
    # would replay the pre-rewrite definition.
    db_disk_name = get_database_disk_name(instance)
    if db_disk_name != "default":
        instance.query(f"SYSTEM CLEAR DISK METADATA CACHE {db_disk_name}")

    instance.query(f"ATTACH TABLE {table_name}")
    assert instance.query(f"EXISTS TABLE {table_name}").strip() == "1"

    instance.query(f"DROP TABLE {table_name}")
