import os

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    relocate_iceberg_table_in_place,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def test_execute_command_table_replaced_in_place(started_cluster_iceberg_no_spark):
    """`ALTER TABLE ... EXECUTE` must operate on the table that is in storage now.

    `ExecuteCommands` used to reach `IcebergMetadata::executeCommand` without any external
    metadata refresh, so on a table that an external writer had dropped and recreated at the
    same root the command still resolved `metadata.json` through the previous incarnation's
    `table-uuid`. For `remove_orphan_files` that means computing reachability from the previous
    incarnation and deleting the replacement's live files as orphans.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]

    suffix = get_uuid_str()
    # The two names must be of equal length: the second incarnation's metadata is relocated
    # byte-for-byte, and the Avro manifests only survive a length-preserving rename.
    first_table = "test_execute_replaced_in_place_a_" + suffix
    second_table = "test_execute_replaced_in_place_b_" + suffix

    root = "/var/lib/clickhouse/user_files/iceberg_data/default"

    create_iceberg_table("local", instance, first_table, started_cluster_iceberg_no_spark, "(x String, y Int64)")
    instance.query(f"INSERT INTO {first_table} VALUES ('old', 1);")

    # Warm the metadata content cache under the first incarnation's table UUID.
    assert instance.query(f"SELECT x, y FROM {first_table} ORDER BY y").strip() == "old\t1"

    # Build the second incarnation as an independent Iceberg table: it gets its own
    # `table-uuid` and its own metadata numbering, restarted from the beginning.
    create_iceberg_table("local", instance, second_table, started_cluster_iceberg_no_spark, "(x String, y Int64)")
    instance.query(f"INSERT INTO {second_table} VALUES ('new', 2), ('new', 3);")
    instance.query(f"DROP TABLE {second_table}")

    relocate_iceberg_table_in_place(instance, root, second_table, first_table)

    # The very first statement touching the replaced table is the `EXECUTE`, so nothing else
    # can have refreshed the metadata on its behalf.
    instance.query(
        f"ALTER TABLE {first_table} EXECUTE remove_orphan_files();",
        settings={
            "allow_insert_into_iceberg": 1,
            "allow_iceberg_remove_orphan_files": 1,
            # Everything in the replacement was written seconds ago, so the default three-day
            # threshold would put all of it out of reach of the command.
            "iceberg_orphan_files_older_than_seconds": 0,
        },
    )

    # The replacement's data files are reachable from its own metadata and must survive.
    assert instance.query(f"SELECT x, y FROM {first_table} ORDER BY y").strip() == "new\t2\nnew\t3"
