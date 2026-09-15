import gzip
import json
import os
import shutil

import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_creation_expression,
    get_uuid_str,
)


DATABRICKS_TABLE_PREFIX = (
    "s3://ttd-unity-useast/env=test/__unitystorage/catalogs/"
    "c137d337-b630-4388-b072-9dc88ee567d6/tables/c771d2d4-eac8-401b-b26d-232b2b8fae5a"
)

FIXTURE_DIR = os.path.join(
    os.path.dirname(__file__), "data", "altinity_dv_puffin_repro_v3"
)

DV_BIN_NAME = "deletion_vector_3d169d0e-b938-4854-8cca-d9d38949f1b0.bin"
EXPECTED_LIVE_IDS = list(range(1001, 10001))


def get_int_array(query_result: str):
    return [int(x) for x in query_result.strip().split("\n") if x]


def upload_table(cluster, storage_type, table_name):
    default_upload_directory(
        cluster,
        storage_type,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )


def _replace_strings(obj, old, new):
    if isinstance(obj, str):
        value = obj.replace(old, new)
        value = value.replace("/_iceberg/metadata/", "/metadata/")
        if value.endswith("/_iceberg"):
            return value[: -len("/_iceberg")]
        return value
    if isinstance(obj, list):
        return [_replace_strings(item, old, new) for item in obj]
    if isinstance(obj, dict):
        return {key: _replace_strings(value, old, new) for key, value in obj.items()}
    return obj


def _rewrite_avro_paths(path, old, new):
    import avro.datafile
    import avro.io

    with open(path, "rb") as handle:
        reader = avro.datafile.DataFileReader(handle, avro.io.DatumReader())
        schema = reader.datum_reader.writers_schema
        meta = dict(reader.meta)
        codec = reader.codec
        records = [_replace_strings(record, old, new) for record in reader]
        reader.close()

    with open(path, "wb") as handle:
        writer = avro.datafile.DataFileWriter(handle, avro.io.DatumWriter(), schema, codec=codec)
        for key, value in meta.items():
            if key not in ("avro.schema", "avro.codec"):
                writer.set_meta(key, value)
        for record in records:
            writer.append(record)
        writer.flush()
        writer.close()


def prepare_databricks_uniform_table(dest_dir):
    """Copy the UniForm fixture and rewrite Databricks URIs onto `dest_dir`.

    Iceberg `location` is `_iceberg` while data files live on the parent table root.
    Flatten metadata to `metadata/` and set `location` to the parent so
    `IcebergPathResolver` can map both data files and the `.bin` DV.
    """
    if os.path.exists(dest_dir):
        shutil.rmtree(dest_dir)
    os.makedirs(dest_dir)

    shutil.copy2(os.path.join(FIXTURE_DIR, DV_BIN_NAME), os.path.join(dest_dir, DV_BIN_NAME))
    shutil.copytree(os.path.join(FIXTURE_DIR, "jk"), os.path.join(dest_dir, "jk"))
    shutil.copytree(
        os.path.join(FIXTURE_DIR, "_iceberg", "metadata"),
        os.path.join(dest_dir, "metadata"),
    )

    with open(os.path.join(dest_dir, DV_BIN_NAME), "rb") as handle:
        header = handle.read(4)
    assert header[:1] == b"\x01", header
    assert header != b"PFA1", header

    new_prefix = dest_dir.rstrip("/")
    metadata_dir = os.path.join(dest_dir, "metadata")
    for name in os.listdir(metadata_dir):
        path = os.path.join(metadata_dir, name)
        if name.endswith(".gz.metadata.json"):
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                text = handle.read()
            text = text.replace(DATABRICKS_TABLE_PREFIX, new_prefix)
            text = text.replace("/_iceberg/metadata/", "/metadata/")
            metadata = json.loads(text)
            location = metadata.get("location", "")
            if location.endswith("/_iceberg"):
                metadata["location"] = location[: -len("/_iceberg")]
            with gzip.open(path, "wt", encoding="utf-8") as handle:
                json.dump(metadata, handle, separators=(",", ":"))
        elif name.endswith(".avro"):
            _rewrite_avro_paths(path, DATABRICKS_TABLE_PREFIX, new_prefix)

    return dest_dir


@pytest.mark.parametrize("run_on_cluster", [False, True])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_deletion_vectors_databricks_uniform_bin(
    started_cluster_iceberg_with_spark, storage_type, run_on_cluster
):
    """Read a Databricks UniForm Iceberg v3 table whose DVs are Delta `.bin` files.

    Spark Iceberg writes Puffin; this fixture is the customer layout: `file_format=PUFFIN`,
    `content_offset=1`, object bytes `0x01` + deletion-vector-v1 envelope.
    """
    if storage_type == "local" and run_on_cluster:
        pytest.skip("Local storage with cluster execution is not supported")

    instance = started_cluster_iceberg_with_spark.instances["node1"]
    table_name = "altinity_dv_puffin_repro_v3_" + storage_type + "_" + get_uuid_str()
    dest_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    prepare_databricks_uniform_table(dest_dir)
    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)

    expression = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        run_on_cluster=run_on_cluster,
        table_function=True,
        format_version=3,
    )
    settings = {"use_iceberg_metadata_files_cache": 0, "use_puffin_files_cache": 0}

    assert int(instance.query(f"SELECT count() FROM {expression}", settings=settings)) == 9000
    assert (
        int(instance.query(f"SELECT min(toInt64(Id)) FROM {expression}", settings=settings))
        == 1001
    )
    assert (
        int(instance.query(f"SELECT max(toInt64(Id)) FROM {expression}", settings=settings))
        == 10000
    )
    assert (
        int(
            instance.query(
                f"SELECT countIf(toInt64(Id) <= 1000) FROM {expression}",
                settings=settings,
            )
        )
        == 0
    )
    assert get_int_array(
        instance.query(f"SELECT toInt64(Id) FROM {expression} ORDER BY toInt64(Id)", settings=settings)
    ) == EXPECTED_LIVE_IDS
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {expression}",
                settings={**settings, "optimize_trivial_count_query": 1},
            )
        )
        == 9000
    )
