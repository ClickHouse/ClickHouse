import os

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    relocate_iceberg_table_in_place,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def test_materialized_view_table_replaced_in_place(started_cluster_iceberg_no_spark):
    """The external metadata refresh must reach an Iceberg table behind a materialized view.

    The interpreters refresh the external metadata of the outermost storage, which for a
    `TO` materialized view is the view itself. `StorageMaterializedView` used to inherit the
    no-op hook, so an `INSERT` that the view pushes into its target was still planned and
    validated against the previous incarnation of a target that an external writer had dropped
    and recreated at the same root.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]

    suffix = get_uuid_str()
    # The two names must be of equal length: the second incarnation's metadata is relocated
    # byte-for-byte, and the Avro manifests only survive a length-preserving rename.
    first_table = "test_mv_replaced_in_place_a_" + suffix
    second_table = "test_mv_replaced_in_place_b_" + suffix
    source_table = "test_mv_source_" + suffix
    view = "test_mv_view_" + suffix

    root = "/var/lib/clickhouse/user_files/iceberg_data/default"

    create_iceberg_table("local", instance, first_table, started_cluster_iceberg_no_spark, "(x String, y Int64)")
    instance.query(f"INSERT INTO {first_table} VALUES ('old', 1);")

    instance.query(f"CREATE TABLE {source_table} (x String, y Int64) ENGINE = MergeTree ORDER BY y")
    instance.query(
        f"CREATE MATERIALIZED VIEW {view} TO {first_table} AS SELECT x, y FROM {source_table}"
    )

    # Warm the metadata content cache under the first incarnation's table UUID.
    assert instance.query(f"SELECT x, y FROM {view} ORDER BY y").strip() == "old\t1"

    # Build the second incarnation as an independent Iceberg table: it gets its own
    # `table-uuid` and its own metadata numbering, restarted from the beginning.
    create_iceberg_table("local", instance, second_table, started_cluster_iceberg_no_spark, "(x String, y Int64)")
    instance.query(f"INSERT INTO {second_table} VALUES ('new', 2), ('new', 3);")
    instance.query(f"DROP TABLE {second_table}")

    relocate_iceberg_table_in_place(instance, root, second_table, first_table)

    # The insert goes through the view, so only the forwarded refresh can bring the target up
    # to the incarnation that is in storage now.
    instance.query(
        f"INSERT INTO {source_table} VALUES ('newer', 4);",
        settings={"allow_insert_into_iceberg": 1},
    )

    assert (
        instance.query(f"SELECT x, y FROM {view} ORDER BY y").strip()
        == "new\t2\nnew\t3\nnewer\t4"
    )
