import os

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    relocate_iceberg_table_in_place,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def test_table_replaced_with_different_schema(started_cluster_iceberg_no_spark):
    """The replacement may reuse the previous table's `schema-id` for a different schema.

    An Iceberg table that an external writer recreated at the same root restarts its own
    `schema-id` numbering, so its schema `0` describes different fields than the previous
    table's schema `0`. `IcebergSchemaProcessor` treats rebinding an existing `schema-id` to
    different fields as malformed metadata, so without dropping the per-table schema state on
    a confirmed replacement the first refreshed query fails with
    `ICEBERG_SPECIFICATION_VIOLATION` instead of reading the new table.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]

    suffix = get_uuid_str()
    # The two names must be of equal length: the second incarnation's metadata is relocated
    # byte-for-byte, and the Avro manifests only survive a length-preserving rename.
    first_table = "test_replaced_schema_a_" + suffix
    second_table = "test_replaced_schema_b_" + suffix

    root = "/var/lib/clickhouse/user_files/iceberg_data/default"

    create_iceberg_table("local", instance, first_table, started_cluster_iceberg_no_spark, "(x String, y Int64)")
    instance.query(f"INSERT INTO {first_table} VALUES ('old', 1);")

    # Warm the schema processor and the metadata content cache under the first incarnation.
    assert instance.query(f"SELECT x, y FROM {first_table} ORDER BY y").strip() == "old\t1"

    # The replacement is a different table with a different schema under the same `schema-id`.
    create_iceberg_table("local", instance, second_table, started_cluster_iceberg_no_spark, "(a Int64, b String, c Int64)")
    instance.query(f"INSERT INTO {second_table} VALUES (1, 'new', 10), (2, 'new', 20);")
    instance.query(f"DROP TABLE {second_table}")

    relocate_iceberg_table_in_place(instance, root, second_table, first_table)

    assert instance.query(f"SELECT a, b, c FROM {first_table} ORDER BY a").strip() == "1\tnew\t10\n2\tnew\t20"
