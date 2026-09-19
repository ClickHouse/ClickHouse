import io

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)

LOCAL_TABLE_PREFIX = "/var/lib/clickhouse/user_files/iceberg_data/default"
S3_TABLE_PREFIX = "var/lib/clickhouse/user_files/iceberg_data/default"

CH_WRITE_SETTINGS = {"allow_insert_into_iceberg": 1}
COMPACTION_SETTINGS = {"allow_experimental_iceberg_compaction": 1}


def _list_files(cluster, instance, storage_type, table_name):
    """Every object under the table root, as storage-relative paths."""
    if storage_type == "local":
        table_dir = f"{LOCAL_TABLE_PREFIX}/{table_name}"
        output = instance.exec_in_container(
            ["bash", "-c", f"find {table_dir} -type f 2>/dev/null | sort"]
        ).strip()
        return sorted(output.split("\n")) if output else []
    prefix = f"{S3_TABLE_PREFIX}/{table_name}/"
    return sorted(
        obj.object_name
        for obj in cluster.minio_client.list_objects(
            cluster.minio_bucket, prefix=prefix, recursive=True
        )
    )


def _write_version_hint(cluster, instance, storage_type, table_name, version):
    if storage_type == "local":
        path = f"{LOCAL_TABLE_PREFIX}/{table_name}/metadata/version-hint.text"
        instance.exec_in_container(["bash", "-c", f"echo {version} > {path}"])
        return

    key = f"{S3_TABLE_PREFIX}/{table_name}/metadata/version-hint.text"
    payload = f"{version}\n".encode()
    cluster.minio_client.put_object(
        cluster.minio_bucket, key, io.BytesIO(payload), len(payload)
    )


def _newest_metadata_version(files):
    """Highest N over the `vN.metadata.json` files in a listing."""
    versions = []
    for path in files:
        name = path.rsplit("/", 1)[-1]
        if name.startswith("v") and name.endswith(".metadata.json"):
            candidate = name[1:-len(".metadata.json")]
            if candidate.isdigit():
                versions.append(int(candidate))
    assert versions, f"no vN.metadata.json in listing: {files}"
    return max(versions)


@pytest.mark.parametrize("storage_type", ["local", "s3"])
@pytest.mark.parametrize("hint_state", ["current", "stale", "absent"])
def test_optimize_never_deletes_reachable_files(
    started_cluster_iceberg_with_spark, storage_type, hint_state
):
    """`OPTIMIZE TABLE` must not delete a file that a retained snapshot still references.

    Data compaction used to rewrite the table rooted at whatever version the hint named and
    then delete a raw listing of `metadata/` and `data/`, which removed `version-hint.text`,
    every `vN.metadata.json` and the data files of snapshots kept in the `snapshots` array.
    The three hint states are the three measured shapes: with the hint behind the newest
    metadata the acked third insert is lost outright, and with any hint present the table
    stops reading at all.

    The outcome of `OPTIMIZE` itself is deliberately not asserted: the property below must
    hold whether the operation is refused or one day publishes its result correctly.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    table_name = f"test_optimize_reachable_{hint_state}_{storage_type}_{get_uuid_str()}"

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_with_spark,
        "(x Int)",
        use_version_hint=(hint_state != "absent"),
    )
    instance.query(f"INSERT INTO {table_name} VALUES (1);", settings=CH_WRITE_SETTINGS)
    instance.query(f"INSERT INTO {table_name} VALUES (2);", settings=CH_WRITE_SETTINGS)
    # A position delete file: `plan.need_optimize` is false without one, and then compaction
    # returns before reaching the deletion at all, which would make this test vacuous.
    instance.query(
        f"ALTER TABLE {table_name} DELETE WHERE x = 2;",
        settings={**CH_WRITE_SETTINGS, "mutations_sync": 2},
    )
    # Committed and acknowledged after the position delete. A rewrite rooted at an older
    # version does not carry this row forward.
    instance.query(f"INSERT INTO {table_name} VALUES (99);", settings=CH_WRITE_SETTINGS)

    assert instance.query(f"SELECT x FROM {table_name} ORDER BY x") == "1\n99\n"

    files_before = _list_files(
        started_cluster_iceberg_with_spark, instance, storage_type, table_name
    )
    hint_path = "metadata/version-hint.text"
    if hint_state == "absent":
        assert not any(p.endswith(hint_path) for p in files_before)
    else:
        assert any(p.endswith(hint_path) for p in files_before)

    newest_before = _newest_metadata_version(files_before)
    if hint_state == "stale":
        # An ordinary steady state, not a rare crash: an external engine committing a new
        # metadata version without touching ClickHouse's hint leaves the hint behind, and a
        # failed hint write is swallowed.
        _write_version_hint(
            started_cluster_iceberg_with_spark,
            instance,
            storage_type,
            table_name,
            newest_before - 1,
        )

    try:
        instance.query(
            f"OPTIMIZE TABLE {table_name};", settings=COMPACTION_SETTINGS
        )
    except Exception as exception:
        # Any failure is acceptable; silently destroying files is not.
        assert "Logical error" not in str(exception), str(exception)

    files_after = _list_files(
        started_cluster_iceberg_with_spark, instance, storage_type, table_name
    )
    removed = sorted(set(files_before) - set(files_after))
    assert not removed, f"OPTIMIZE deleted pre-existing files: {removed}"

    if hint_state == "stale":
        # Point the hint back at the version it named before it went stale. Reading through a
        # stale hint legitimately returns the older version, so the hint has to name the newest
        # one for the row assertion below to be about `OPTIMIZE` rather than about the hint.
        _write_version_hint(
            started_cluster_iceberg_with_spark,
            instance,
            storage_type,
            table_name,
            newest_before,
        )

    # The acked third insert must still be there, and the table must still be readable
    # through the version it was committed at.
    assert instance.query(f"SELECT x FROM {table_name} ORDER BY x") == "1\n99\n"
    assert int(instance.query(f"SELECT count() FROM {table_name}")) == 2

    instance.query(f"DROP TABLE {table_name} SYNC;")
