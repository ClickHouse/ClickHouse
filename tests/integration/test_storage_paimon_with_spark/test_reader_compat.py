"""Compatibility with tables written by other Paimon writers and versions.

The live test writer is Spark with Paimon 1.1.1; customers also run newer
versions and, most commonly, Flink. The checked-in fixtures (regeneration
recipes in fixtures/README.md) are uploaded under a unique name — Paimon
metadata stores relative paths, so table directories are relocatable — and
read back:

- fixtures/paimon_spark_132/: written by Spark with Paimon 1.3.2 (newer than
  the reader's proven 1.1.1 floor), partitioned, with a schema evolution.
- fixtures/paimon_flink_111/: written by Flink 1.19 with paimon-flink 1.1.1
  (the dominant real-world Paimon writer), with a schema evolution. A live
  Flink writer (CDC into primary-key tables) is a follow-up blocked on
  https://github.com/ClickHouse/ClickHouse/issues/113360.
"""

import os

import pytest

from helpers.paimon_utils import (
    get_creation_expression,
    get_schema_and_data,
    get_uuid_str,
    upload_paimon_dir,
)

FIXTURES_ROOT = os.path.join(os.path.dirname(os.path.realpath(__file__)), "fixtures")

FIXTURES = {
    "spark_132": (
        "paimon_spark_132/default.db/compat",
        [
            ["id", "Int32"],
            ["val", "Nullable(String)"],
            ["d", "Nullable(Date)"],
            ["part", "Nullable(Int32)"],
            ["extra", "Nullable(Int64)"],
        ],
        [
            ["1", "one", "2024-01-01", "0", "\\N"],
            ["2", "two", "2024-01-02", "1", "\\N"],
            ["3", "three", "2024-01-03", "0", "30"],
        ],
    ),
    "flink_111": (
        "paimon_flink_111/compat_flink",
        [
            ["id", "Int32"],
            ["val", "Nullable(String)"],
            ["dt", "Nullable(Date)"],
            ["extra", "Nullable(Int64)"],
        ],
        [
            ["1", "one", "2024-01-01", "\\N"],
            ["2", "two", "2024-01-02", "\\N"],
            ["3", "three", "2024-01-03", "30"],
        ],
    ),
}


def _upload_fixture(started_cluster, storage_type, fixture_subdir, table_name):
    uploader = {
        "s3": started_cluster.default_s3_uploader,
        "azure": started_cluster.default_azure_uploader,
    }[storage_type]
    remote_dir = f"/var/lib/clickhouse/user_files/paimon_data/default.db/{table_name}/"
    upload_paimon_dir(
        os.path.join(FIXTURES_ROOT, fixture_subdir),
        lambda local_file, rel_path: uploader.upload_file(
            local_file, remote_dir + rel_path
        ),
    )


@pytest.mark.parametrize("fixture", sorted(FIXTURES))
@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_read_foreign_writer_table(
    started_cluster_paimon_with_spark, storage_type, fixture
):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    fixture_subdir, expected_schema, expected_data = FIXTURES[fixture]
    table_name = f"test_compat_{fixture}_{storage_type}_" + get_uuid_str()

    _upload_fixture(started_cluster, storage_type, fixture_subdir, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    schema, data = get_schema_and_data(instance, table_expr, order_by="id")
    assert schema == expected_schema
    assert data == expected_data
