"""
Test that ClickHouse handles Iceberg format version upgrades by external tools.

When an external tool (e.g. Spark) upgrades the Iceberg format version
(e.g. from v1 to v2), the cached format_version in PersistentTableComponents
should be updated rather than triggering a logical error.
Regression test for https://github.com/ClickHouse/ClickHouse/issues/86776
"""

import json
import re
import time

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)


ICEBERG_SETTINGS = {"allow_insert_into_iceberg": 1}


def _read_iceberg_metadata(instance, table_name):
    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"
    latest = instance.exec_in_container(
        ["bash", "-c", f"ls -v {metadata_dir}/v*.metadata.json | tail -1"]
    ).strip()
    raw = instance.exec_in_container(["cat", latest])
    return json.loads(raw), latest


def _write_iceberg_metadata(instance, table_name, meta, prev_path):
    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"
    meta["last-updated-ms"] = int(time.time() * 1000)
    version_match = re.search(r"/v(\d+)[^/]*\.metadata\.json$", prev_path)
    new_version = int(version_match.group(1)) + 1
    new_path = f"{metadata_dir}/v{new_version}.metadata.json"
    new_content = json.dumps(meta, indent=4)
    instance.exec_in_container(
        ["bash", "-c", f"cat > {new_path} << 'JSONEOF'\n{new_content}\nJSONEOF"]
    )


@pytest.mark.parametrize("storage_type", ["local"])
def test_format_version_upgrade_v1_to_v2(started_cluster_iceberg_with_spark, storage_type):
    """Create a v1 table, insert data, upgrade metadata to v2, then read."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    table_name = f"test_fmt_upgrade_v1_to_v2_{get_uuid_str()}"

    create_iceberg_table(
        storage_type, instance, table_name,
        started_cluster_iceberg_with_spark, "(x Int)", format_version=1,
    )

    instance.query(
        f"INSERT INTO {table_name} VALUES (1), (2), (3);",
        settings=ICEBERG_SETTINGS,
    )

    # Verify initial read works
    result = instance.query(f"SELECT sum(x) FROM {table_name}")
    assert result.strip() == "6"

    # Simulate external tool upgrading format version from 1 to 2
    meta, prev_path = _read_iceberg_metadata(instance, table_name)
    assert meta["format-version"] == 1
    meta["format-version"] = 2
    _write_iceberg_metadata(instance, table_name, meta, prev_path)

    # Reading after format version upgrade should work without exception
    result = instance.query(f"SELECT sum(x) FROM {table_name}")
    assert result.strip() == "6"

    instance.query(f"DROP TABLE {table_name}")
