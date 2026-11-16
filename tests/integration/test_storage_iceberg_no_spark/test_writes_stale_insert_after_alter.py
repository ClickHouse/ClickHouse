import copy
import json
import os
import re
import tempfile

import pytest

from helpers.iceberg_utils import create_iceberg_table, get_uuid_str

TABLE_ROOT = "/var/lib/clickhouse/user_files/iceberg_data/default"
FORMATS = ["Parquet", "ORC", "Avro"]


def latest_metadata_file(instance, table_name):
    return instance.exec_in_container(
        ["bash", "-c", f"ls -v {TABLE_ROOT}/{table_name}/metadata/v*.metadata.json | tail -1"]
    ).strip()


def table_files(instance, table_name):
    return instance.exec_in_container(["bash", "-c", f"find {TABLE_ROOT}/{table_name} -type f | sort"])


def attach(cluster, table_name, format, metadata_file=""):
    """(Re)creates the ClickHouse table over the existing Iceberg table, optionally pinned to an older metadata file."""
    create_iceberg_table(
        "local",
        cluster.instances["node1"],
        table_name,
        cluster,
        format=format,
        explicit_metadata_path=os.path.relpath(metadata_file, f"{TABLE_ROOT}/{table_name}") if metadata_file else "",
    )


def insert_with_stale_structure(cluster, table_name, format, alter, insert):
    """Runs `alter`, then `insert` through the table as it was before it: the structure an INSERT
    planned before a concurrent ALTER writes. Returns the error and the table files before and after."""
    instance = cluster.instances["node1"]
    metadata_before_alter = latest_metadata_file(instance, table_name)
    alter()
    attach(cluster, table_name, format, metadata_before_alter)
    files_before = table_files(instance, table_name)
    error = instance.query_and_get_error(insert)
    files_after = table_files(instance, table_name)
    attach(cluster, table_name, format)
    return error, files_before, files_after


def create_table(cluster, table_name, format, schema="(a Int32, b Nullable(String), c Int64)"):
    instance = cluster.instances["node1"]
    create_iceberg_table("local", instance, table_name, cluster, schema, format=format)
    instance.query(f"INSERT INTO {table_name} SELECT number, NULL, number * 100 FROM numbers(3)")


def assert_refused(error, files_before, files_after, column):
    assert "INCORRECT_DATA" in error, error
    assert f"Column '{column}' has no field id in the Iceberg schema being written" in error, error
    assert files_after == files_before


def rename_nested_field(instance, table_name, column, old_name, new_name):
    """Publishes a new schema with `column.old_name` renamed to `new_name`, as another Iceberg writer
    would: ClickHouse cannot rename a nested field."""
    path = latest_metadata_file(instance, table_name)
    metadata = json.loads(instance.exec_in_container(["cat", path]))
    schema = copy.deepcopy(next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"]))
    schema["schema-id"] = max(s["schema-id"] for s in metadata["schemas"]) + 1
    struct = next(f for f in schema["fields"] if f["name"] == column)["type"]
    next(f for f in struct["fields"] if f["name"] == old_name)["name"] = new_name
    metadata["schemas"].append(schema)
    metadata["current-schema-id"] = schema["schema-id"]

    version = int(re.search(r"v(\d+)\.metadata\.json$", path).group(1))
    with tempfile.NamedTemporaryFile("w", suffix=".json") as f:
        json.dump(metadata, f)
        f.flush()
        instance.copy_file_to_container(f.name, f"{os.path.dirname(path)}/v{version + 1}.metadata.json")


@pytest.mark.parametrize("format", FORMATS)
def test_stale_insert_after_drop_column(started_cluster_iceberg_no_spark, format):
    cluster = started_cluster_iceberg_no_spark
    instance = cluster.instances["node1"]
    table_name = f"test_stale_insert_after_drop_column_{format}_{get_uuid_str()}"
    create_table(cluster, table_name, format)

    result = insert_with_stale_structure(
        cluster,
        table_name,
        format,
        lambda: instance.query(f"ALTER TABLE {table_name} DROP COLUMN b"),
        f"INSERT INTO {table_name} SELECT number + 10, 'x', 5000 + number FROM numbers(3)",
    )
    assert_refused(*result, "b")
    assert instance.query(f"SELECT count() FROM {table_name}") == "3\n"

    instance.query(f"INSERT INTO {table_name} SELECT number + 20, 6000 + number FROM numbers(3)")
    assert instance.query(f"SELECT count() FROM {table_name}") == "6\n"


@pytest.mark.parametrize("format", FORMATS)
def test_stale_insert_after_drop_and_add_column(started_cluster_iceberg_no_spark, format):
    """Same width as the latest schema: the stale `c` must not be written under the id of the new `d`."""
    cluster = started_cluster_iceberg_no_spark
    instance = cluster.instances["node1"]
    table_name = f"test_stale_insert_after_drop_and_add_column_{format}_{get_uuid_str()}"
    create_table(cluster, table_name, format)

    def alter():
        instance.query(f"ALTER TABLE {table_name} DROP COLUMN b")
        instance.query(f"ALTER TABLE {table_name} ADD COLUMN d Nullable(Int64)")

    result = insert_with_stale_structure(
        cluster,
        table_name,
        format,
        alter,
        f"INSERT INTO {table_name} SELECT number + 10, 'x', 5000 + number FROM numbers(3)",
    )
    assert_refused(*result, "b")
    assert instance.query(
        f"SELECT count() FROM {table_name} WHERE d IS NULL", settings={"use_iceberg_partition_pruning": 1}
    ) == "3\n"


@pytest.mark.parametrize("format", FORMATS)
def test_stale_insert_after_rename_column(started_cluster_iceberg_no_spark, format):
    cluster = started_cluster_iceberg_no_spark
    instance = cluster.instances["node1"]
    table_name = f"test_stale_insert_after_rename_column_{format}_{get_uuid_str()}"
    create_table(cluster, table_name, format)

    result = insert_with_stale_structure(
        cluster,
        table_name,
        format,
        lambda: instance.query(f"ALTER TABLE {table_name} RENAME COLUMN b TO b2"),
        f"INSERT INTO {table_name} SELECT number + 10, 'x', 5000 + number FROM numbers(3)",
    )
    assert_refused(*result, "b")
    assert instance.query(f"SELECT count() FROM {table_name}") == "3\n"


@pytest.mark.parametrize("format", FORMATS)
def test_stale_insert_after_nested_field_rename(started_cluster_iceberg_no_spark, format):
    """The tuple keeps its id, so only a check of every nested field refuses the stale `s.x`."""
    cluster = started_cluster_iceberg_no_spark
    instance = cluster.instances["node1"]
    table_name = f"test_stale_insert_after_nested_field_rename_{format}_{get_uuid_str()}"
    create_iceberg_table("local", instance, table_name, cluster, "(a Int32, s Tuple(x Int32))", format=format)
    instance.query(f"INSERT INTO {table_name} SELECT number, tuple(number) FROM numbers(3)")

    result = insert_with_stale_structure(
        cluster,
        table_name,
        format,
        lambda: rename_nested_field(instance, table_name, "s", "x", "y"),
        f"INSERT INTO {table_name} SELECT number + 10, tuple(number + 10) FROM numbers(3)",
    )
    assert_refused(*result, "s.x")
    assert instance.query(f"SELECT count() FROM {table_name}") == "3\n"


# Not Avro: reading an Avro data file written before ADD COLUMN fails with "Field d not found in Avro schema".
@pytest.mark.parametrize("format", ["Parquet", "ORC"])
def test_stale_insert_after_add_column(started_cluster_iceberg_no_spark, format):
    """A column added after planning is simply not written by the stale INSERT, which is accepted."""
    cluster = started_cluster_iceberg_no_spark
    instance = cluster.instances["node1"]
    table_name = f"test_stale_insert_after_add_column_{format}_{get_uuid_str()}"
    create_table(cluster, table_name, format)

    metadata_before_alter = latest_metadata_file(instance, table_name)
    instance.query(f"ALTER TABLE {table_name} ADD COLUMN d Nullable(Int64)")
    attach(cluster, table_name, format, metadata_before_alter)
    instance.query(f"INSERT INTO {table_name} SELECT number + 10, 'x', 5000 + number FROM numbers(3)")

    attach(cluster, table_name, format)
    assert instance.query(f"SELECT count(), countIf(d IS NULL) FROM {table_name}") == "6\t6\n"
