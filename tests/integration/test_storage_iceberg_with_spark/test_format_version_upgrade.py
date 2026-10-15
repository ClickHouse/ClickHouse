"""
Test that ClickHouse handles Iceberg format version upgrades by external tools.

When an external tool (e.g. Spark) upgrades the Iceberg format version
(e.g. from v1 to v2), reading the table should succeed rather than
triggering a logical error. Each manifest list / manifest file carries its
own format version in its Avro metadata, so v1 files left behind after an
upgrade remain readable alongside newer v2 metadata.

For backward compatibility with manifests written by older ClickHouse
versions (which did not include the `format-version` Avro metadata key),
`AvroForIcebergDeserializer::getFormatVersionFromManifestFileMetadata` falls
back to schema-based detection: presence of the `sequence_number` field at
the top level signals v2, its absence signals v1.

Regression test for https://github.com/ClickHouse/ClickHouse/issues/86776
"""

import json
import os
import re
import time

import pytest

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import avro.schema as avro_schema

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
    # Write to a temporary file and rename atomically. Concurrent readers (in
    # `test_format_version_upgrade_concurrent_reads`) must never observe a
    # partially-written metadata file.
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"cat > {new_path}.tmp << 'JSONEOF'\n{new_content}\nJSONEOF\nmv {new_path}.tmp {new_path}",
        ]
    )


def _rewrite_avro_without_format_version(local_in, local_out):
    """Read an Avro file and write it back without the `format-version` metadata key,
    preserving every other user-defined metadata entry, the schema, and the codec."""
    with open(local_in, "rb") as fin:
        reader = DataFileReader(fin, DatumReader())
        schema_str = reader.meta["avro.schema"].decode("utf-8")
        codec_bytes = reader.meta.get("avro.codec", b"null")
        codec = codec_bytes.decode("utf-8") if isinstance(codec_bytes, bytes) else codec_bytes
        user_meta = {
            k: v for k, v in reader.meta.items()
            if not k.startswith("avro.") and k != "format-version"
        }
        records = list(reader)
        reader.close()

    parsed_schema = avro_schema.parse(schema_str)
    with open(local_out, "wb") as fout:
        writer = DataFileWriter(fout, DatumWriter(), parsed_schema, codec=codec)
        for k, v in user_meta.items():
            writer.set_meta(k, v)
        for record in records:
            writer.append(record)
        writer.close()


def _strip_format_version_avro_metadata(instance, table_name, tmp_dir):
    """Strip the `format-version` Avro metadata key from every manifest list and manifest
    file of the table, simulating manifests written by an older ClickHouse version."""
    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"
    listing = instance.exec_in_container(
        ["bash", "-c", f"ls {metadata_dir}/*.avro 2>/dev/null || true"]
    ).strip()
    avro_files = [p for p in listing.split("\n") if p]
    assert avro_files, f"Expected at least one .avro manifest file under {metadata_dir}"

    for i, remote_path in enumerate(avro_files):
        local_in = os.path.join(tmp_dir, f"in_{i}.avro")
        local_out = os.path.join(tmp_dir, f"out_{i}.avro")
        instance.copy_file_from_container(remote_path, local_in)
        _rewrite_avro_without_format_version(local_in, local_out)
        instance.copy_file_to_container(local_out, remote_path)


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


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_after_external_upgrade(started_cluster_iceberg_with_spark, storage_type):
    """`EXECUTE remove_orphan_files` must consult the latest metadata for its v2 gate.

    A previous release used the cached `format_version` from `PersistentTableComponents`,
    which is captured when the table is opened. After an external v1 -> v2 upgrade between
    queries, the cached value remained `1` and `remove_orphan_files` was rejected with
    `requires Iceberg format version >= 2, but this table uses format version 1`. The fix
    re-reads the latest metadata file at command time.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    table_name = f"test_fmt_orphan_after_upgrade_{get_uuid_str()}"

    create_iceberg_table(
        storage_type, instance, table_name,
        started_cluster_iceberg_with_spark, "(x Int)", format_version=1,
    )

    instance.query(
        f"INSERT INTO {table_name} VALUES (1), (2), (3);",
        settings=ICEBERG_SETTINGS,
    )

    # Open the table on the read path so PersistentTableComponents is populated
    # with the v1 format version (the value the bug captures and never refreshes).
    assert instance.query(f"SELECT sum(x) FROM {table_name}").strip() == "6"

    # Simulate an external tool (e.g. Spark) upgrading the format version v1 -> v2.
    meta, prev_path = _read_iceberg_metadata(instance, table_name)
    assert meta["format-version"] == 1
    meta["format-version"] = 2
    _write_iceberg_metadata(instance, table_name, meta, prev_path)

    # `remove_orphan_files` must read the latest metadata for its v2 gate. Without the
    # fix this throws because the cached `format_version` is still 1.
    now_ts = time.strftime("%Y-%m-%d %H:%M:%S")
    instance.query(
        f"ALTER TABLE {table_name} EXECUTE remove_orphan_files(older_than = '{now_ts}', dry_run = 1);",
        settings={**ICEBERG_SETTINGS, "allow_iceberg_remove_orphan_files": 1},
    )

    instance.query(f"DROP TABLE {table_name}")


@pytest.mark.parametrize("storage_type", ["local"])
def test_format_version_upgrade_concurrent_reads(
    started_cluster_iceberg_with_spark, storage_type
):
    """Concurrent reads remain correct while another session upgrades the format version.

    `PersistentTableComponents` is shared across queries, so the previous design risked
    cross-query mis-parsing if its cached `format_version` was mutated mid-flight. The fix
    derives the parsing version from each manifest's own Avro `format-version` metadata,
    so concurrent readers no longer depend on shared mutable state. This test exercises
    that path: one writer upgrades the metadata file from v1 to v2 while many readers
    loop continuously, and every read must return the expected sum without exception.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    instance = started_cluster_iceberg_with_spark.instances["node1"]
    table_name = f"test_fmt_upgrade_concurrent_{get_uuid_str()}"

    create_iceberg_table(
        storage_type, instance, table_name,
        started_cluster_iceberg_with_spark, "(x Int)", format_version=1,
    )

    instance.query(
        f"INSERT INTO {table_name} VALUES (1), (2), (3);",
        settings=ICEBERG_SETTINGS,
    )

    expected_sum = "6"
    assert instance.query(f"SELECT sum(x) FROM {table_name}").strip() == expected_sum

    stop = {"flag": False}

    def reader_loop():
        iterations = 0
        while not stop["flag"]:
            result = instance.query(f"SELECT sum(x) FROM {table_name}").strip()
            assert result == expected_sum, f"Reader saw {result!r}, expected {expected_sum!r}"
            iterations += 1
        return iterations

    num_readers = 4
    with ThreadPoolExecutor(max_workers=num_readers + 1) as executor:
        reader_futures = [executor.submit(reader_loop) for _ in range(num_readers)]

        try:
            # Let readers warm up against the v1 metadata, then flip to v2 from another thread.
            time.sleep(0.5)
            meta, prev_path = _read_iceberg_metadata(instance, table_name)
            assert meta["format-version"] == 1
            meta["format-version"] = 2
            _write_iceberg_metadata(instance, table_name, meta, prev_path)

            # Keep readers running long enough to interleave with the upgraded metadata.
            time.sleep(1.5)
        finally:
            # Always stop the readers, even if the upgrade path above raises. Otherwise the
            # `ThreadPoolExecutor` shutdown on leaving the `with` block waits forever on the
            # infinite reader loops and the test hangs until the global CI timeout instead of
            # failing fast.
            stop["flag"] = True

        for fut in as_completed(reader_futures):
            iterations = fut.result()
            assert iterations > 0, "Reader thread completed without running any query"

    # Final read after all readers stop must also see the upgraded metadata correctly.
    assert instance.query(f"SELECT sum(x) FROM {table_name}").strip() == expected_sum

    instance.query(f"DROP TABLE {table_name}")


@pytest.mark.parametrize("storage_type", ["local"])
@pytest.mark.parametrize("format_version", [1, 2])
def test_format_version_avro_metadata_fallback(
    started_cluster_iceberg_with_spark, storage_type, format_version, tmp_path
):
    """Reads must succeed for manifests that lack the `format-version` Avro metadata key.

    Older ClickHouse versions wrote both manifest lists and manifest files without that
    key, and `AvroForIcebergDeserializer::getFormatVersionFromManifestFileMetadata` falls
    back to schema-based detection (presence of `sequence_number` at the top level
    ⇒ v2, absence ⇒ v1). This test covers both branches of that fallback.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    table_name = f"test_fmt_avro_fallback_v{format_version}_{get_uuid_str()}"

    create_iceberg_table(
        storage_type, instance, table_name,
        started_cluster_iceberg_with_spark, "(x Int)",
        format_version=format_version,
    )

    instance.query(
        f"INSERT INTO {table_name} VALUES (1), (2), (3);",
        settings=ICEBERG_SETTINGS,
    )

    # Sanity check: read works with the `format-version` Avro metadata key present.
    result = instance.query(f"SELECT sum(x) FROM {table_name}")
    assert result.strip() == "6"

    _strip_format_version_avro_metadata(instance, table_name, str(tmp_path))

    # The Iceberg metadata cache holds deserialized manifest files; drop it so the
    # next read picks up the on-disk files we just rewrote without the
    # `format-version` Avro metadata key.
    instance.query("SYSTEM DROP ICEBERG METADATA CACHE")

    # After stripping the key, reads must still succeed via schema-based fallback.
    result = instance.query(f"SELECT sum(x) FROM {table_name}")
    assert result.strip() == "6"

    instance.query(f"DROP TABLE {table_name}")
