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
def test_schema_id_rebound_by_newer_metadata(
    started_cluster_iceberg_no_spark, format_version
):
    """A newer metadata version that binds an existing schema-id to different fields must not
    make the table unreadable. The table metadata is the source of truth for the
    schema-id -> schema mapping and data files are resolved by field id, so the read applies the
    schema the table currently declares - also when the copy of that schema-id embedded in the
    already written manifest file still describes the previous one. This used to abort debug
    builds in `addIcebergTableSchema` and later to fail the read with
    `ICEBERG_SPECIFICATION_VIOLATION`. See issue #107316."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = (
        "test_schema_id_rebound_by_newer_metadata_"
        + str(format_version)
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        "local",
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(c0 Int32, c1 String)",
        format_version,
    )
    # The INSERT writes the manifest file that embeds a copy of this schema, so after the
    # rewrite below the manifest and the table metadata disagree about schema-id 0.
    instance.query(f"INSERT INTO {table_name} VALUES (1, 'a');")

    # Warm read: caches schema-id 0 -> {c0, c1} in the table's persistent schema processor, so
    # the rewrite is seen as a re-binding of an id that is already known.
    assert instance.query(f"SELECT count() FROM {table_name} FORMAT TSV") == "1\n"

    meta, prev = _read_latest_metadata(instance, table_name)
    # Rename c0 -> c9 in place, keeping the schema-id. `current-schema-id` is left alone, and a
    # v1 metadata file carries the same schema under the deprecated `schema` key as well.
    schemas = list(meta.get("schemas", []))
    if "schema" in meta:
        schemas.append(meta["schema"])
    renamed = 0
    for schema in schemas:
        for field in schema["fields"]:
            if field["name"] == "c0":
                field["name"] = "c9"
                renamed += 1
    assert renamed > 0, "no schema was re-bound: the fixture is vacuous"
    meta["last-updated-ms"] = meta.get("last-updated-ms", 0) + 60000

    # A NEW metadata version, never an in-place rewrite: metadata JSON is cached
    # (`use_iceberg_metadata_files_cache` defaults to 1), so an in-place edit of the already
    # read version is not re-read at all.
    _write_next_metadata(instance, table_name, meta, prev)

    # `manifest_file_metadata` additionally makes the manifest file bypass the metadata files
    # cache, so the copy of the schema it embeds is parsed again during this very read.
    query_id = f"{table_name}-after-rebinding"
    assert (
        instance.query(
            f"SELECT c9, c1 FROM {table_name} FORMAT TSV",
            query_id=query_id,
            settings={
                "iceberg_metadata_staleness_ms": 0,
                "iceberg_metadata_log_level": "manifest_file_metadata",
            },
        )
        == "1\ta\n"
    ), "the row must be returned under the name the newest metadata declares"

    instance.query("SYSTEM FLUSH LOGS iceberg_metadata_log")

    # The manifest file read by the query above still describes schema-id 0 as {c0, c1}: the two
    # copies really disagree, and the one from the table metadata is the one that was applied.
    embedded_schemas = instance.query(
        f"SELECT JSONExtractRaw(content, 'schema') FROM system.iceberg_metadata_log "
        f"WHERE query_id = '{query_id}' AND content_type = 'ManifestFileMetadata' FORMAT TSV"
    ).splitlines()
    assert embedded_schemas, "no manifest file was parsed by the query"
    for embedded_schema in embedded_schemas:
        # The logged Avro metadata is stripped of its escaping, so compare without quoting.
        normalized = re.sub(r'[\s"\\]', "", embedded_schema)
        assert (
            "name:c0" in normalized and "name:c9" not in normalized
        ), f"fixture requires the manifest copy to keep the old field name: {embedded_schema}"

    instance.query(f"DROP TABLE {table_name} SYNC;")
