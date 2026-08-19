import os

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def test_table_replaced_in_place(started_cluster_iceberg_no_spark):
    """An external writer drops and recreates the table at the same storage root.

    The recreated table has a new `table-uuid` and restarts the metadata numbering, so a
    reused `IcebergMetadata` object must not keep serving the previous table's
    `metadata.json` out of `IcebergMetadataFilesCache`, which is keyed by the table UUID.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]

    suffix = get_uuid_str()
    # The two names must be of equal length: the second incarnation's metadata is relocated
    # byte-for-byte, and the Avro manifests only survive a length-preserving rename.
    first_table = "test_table_replaced_in_place_a_" + suffix
    second_table = "test_table_replaced_in_place_b_" + suffix

    root = "/var/lib/clickhouse/user_files/iceberg_data/default"
    first_root = f"{root}/{first_table}"
    second_root = f"{root}/{second_table}"

    create_iceberg_table("local", instance, first_table, started_cluster_iceberg_no_spark, "(x String, y Int64)")
    instance.query(f"INSERT INTO {first_table} VALUES ('old', 1);")

    # Warm the metadata content cache under the first incarnation's table UUID.
    assert instance.query(f"SELECT x, y FROM {first_table} ORDER BY y").strip() == "old\t1"

    # Build the second incarnation as an independent Iceberg table: it gets its own
    # `table-uuid` and its own metadata numbering, restarted from the beginning.
    create_iceberg_table("local", instance, second_table, started_cluster_iceberg_no_spark, "(x String, y Int64)")
    instance.query(f"INSERT INTO {second_table} VALUES ('new', 2), ('new', 3);")
    instance.query(f"DROP TABLE {second_table}")

    # Move it onto the first table's root, the way an external writer replacing the table
    # in place would leave it. The first incarnation's data files are deliberately left
    # behind and only the metadata is replaced, so that reading the stale metadata still
    # resolves and returns the previous table's rows instead of raising.
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"set -e; "
            f"mv {second_root}/data/* {first_root}/data/; "
            f"rm -rf {first_root}/metadata; "
            f"mv {second_root}/metadata {first_root}/metadata; "
            f"rm -rf {second_root}; "
            f"LC_ALL=C grep -rla '{second_table}' {first_root} "
            f"| xargs -r sed -i 's/{second_table}/{first_table}/g'",
        ],
        user="root",
    )

    assert instance.query(f"SELECT x, y FROM {first_table} ORDER BY y").strip() == "new\t2\nnew\t3"

    # Control: with the metadata content cache off the stale key cannot be hit at all,
    # so this must hold both before and after the fix.
    assert (
        instance.query(
            f"SELECT x, y FROM {first_table} ORDER BY y",
            settings={"use_iceberg_metadata_files_cache": 0},
        ).strip()
        == "new\t2\nnew\t3"
    )
