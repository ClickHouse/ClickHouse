import json
import re

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)


def _metadata_dir(table_name):
    return f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"


def _read_latest_metadata(instance, table_name):
    metadata_dir = _metadata_dir(table_name)
    latest = instance.exec_in_container(
        ["bash", "-c", f"ls -v {metadata_dir}/v*.metadata.json | tail -1"]
    ).strip()
    raw = instance.exec_in_container(["cat", latest])
    return json.loads(raw), latest


def _write_next_metadata(instance, table_name, meta, prev_path):
    metadata_dir = _metadata_dir(table_name)
    version_match = re.search(r"/v(\d+)[^/]*\.metadata\.json$", prev_path)
    new_version = int(version_match.group(1)) + 1
    new_path = f"{metadata_dir}/v{new_version}.metadata.json"
    new_content = json.dumps(meta, indent=4)
    instance.exec_in_container(
        ["bash", "-c", f"cat > {new_path} << 'JSONEOF'\n{new_content}\nJSONEOF"]
    )


@pytest.mark.parametrize("format_version", [1, 2])
def test_iceberg_history_summary_overflow(started_cluster_iceberg_no_spark, format_version):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_iceberg_history_summary_overflow_" + get_uuid_str()

    create_iceberg_table(
        "local",
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x Int)",
        format_version,
    )
    instance.query(f"INSERT INTO {table_name} VALUES (1);")

    meta, prev = _read_latest_metadata(instance, table_name)
    assert meta.get("snapshots"), "snapshot must be present after INSERT"

    # The exact value from the bug report.
    huge = "6986350573"
    assert int(huge) > 2147483647
    for snap in meta["snapshots"]:
        summary = snap.setdefault("summary", {})
        summary["added-data-files"] = huge
        summary["added-records"] = huge
        summary["added-files-size"] = huge
        summary["changed-partition-count"] = huge

    _write_next_metadata(instance, table_name, meta, prev)

    count = instance.query(
        f"SELECT count() FROM system.iceberg_history "
        f"WHERE database = 'default' AND table = '{table_name}'"
    ).strip()
    assert count == "1", (
        f"system.iceberg_history must return the snapshot even when summary "
        f"values exceed INT32_MAX, got count={count}. See issue #94176."
    )
