import io
import json

import avro.datafile
import avro.io
import avro.schema
import pytest

from helpers.iceberg_utils import create_iceberg_table, get_uuid_str

LOCAL_PREFIX = "/var/lib/clickhouse/user_files/iceberg_data/default"
S3_PREFIX = "var/lib/clickhouse/user_files/iceberg_data/default"

HINT = "metadata/version-hint.text"


def _read(cluster, instance, storage_type, table_name, rel_path):
    if storage_type == "local":
        return instance.exec_in_container(
            ["bash", "-c", f"cat {LOCAL_PREFIX}/{table_name}/{rel_path} 2>/dev/null || true"]
        ).strip()
    key = f"{S3_PREFIX}/{table_name}/{rel_path}"
    try:
        response = cluster.minio_client.get_object(cluster.minio_bucket, key)
        return response.read().decode().strip()
    finally:
        try:
            response.close()
            response.release_conn()
        except Exception:
            pass


def _write(cluster, instance, storage_type, table_name, rel_path, content):
    if storage_type == "local":
        instance.exec_in_container(
            ["bash", "-c", f"printf '%s' '{content}' > {LOCAL_PREFIX}/{table_name}/{rel_path}"]
        )
        return
    key = f"{S3_PREFIX}/{table_name}/{rel_path}"
    data = content.encode()
    cluster.minio_client.put_object(cluster.minio_bucket, key, io.BytesIO(data), len(data))


def _read_bytes(cluster, instance, storage_type, table_name, rel_path):
    if storage_type == "local":
        return bytes.fromhex(
            instance.exec_in_container(
                ["bash", "-c", f"xxd -p {LOCAL_PREFIX}/{table_name}/{rel_path} | tr -d '\\n'"]
            ).strip()
        )
    response = cluster.minio_client.get_object(
        cluster.minio_bucket, f"{S3_PREFIX}/{table_name}/{rel_path}"
    )
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def _write_bytes(cluster, instance, storage_type, table_name, rel_path, data):
    if storage_type == "local":
        instance.exec_in_container(
            [
                "bash",
                "-c",
                f"printf '%s' '{data.hex()}' | xxd -r -p > {LOCAL_PREFIX}/{table_name}/{rel_path}",
            ]
        )
        return
    cluster.minio_client.put_object(
        cluster.minio_bucket,
        f"{S3_PREFIX}/{table_name}/{rel_path}",
        io.BytesIO(data),
        len(data),
    )


def _rewrite_manifest_list(avro_bytes, patch_schema, patch_record):
    """Rewrite every entry of a manifest list, and its schema if needed.

    `patch_schema` receives the parsed schema and must report whether it found
    anything to change, so a fixture that silently matched nothing fails loudly
    instead of producing an undamaged list. The file's own Avro metadata is carried
    over because `format-version` governs how the list is parsed.
    """
    reader = avro.datafile.DataFileReader(io.BytesIO(avro_bytes), avro.io.DatumReader())
    schema = json.loads(str(reader.datum_reader.writers_schema))
    metadata = dict(reader.meta)
    records = list(reader)
    reader.close()

    assert patch_schema(schema), "Manifest list has no field for this fixture to patch"
    for record in records:
        patch_record(record)

    out = io.BytesIO()
    writer = avro.datafile.DataFileWriter(
        out, avro.io.DatumWriter(), avro.schema.parse(json.dumps(schema))
    )
    for key, value in metadata.items():
        if not key.startswith("avro."):
            writer.set_meta(key, value)
    for record in records:
        writer.append(record)
    writer.flush()
    result = out.getvalue()
    writer.close()
    return result


def _null_out_added_snapshot_id(avro_bytes):
    """Rewrite a manifest list so `added_snapshot_id` is a null union branch.

    ClickHouse cannot produce this shape: it declares the field as a non-nullable
    `long` (`AvroSchema.h`) and refuses to carry one forward
    (`IcebergWrites.cpp`, ICEBERG_SPECIFICATION_VIOLATION). It comes from an
    externally written list, which is why the field's type is rewritten here
    rather than only its value: the pre-apache/iceberg#11626 iceberg-spark schema
    for it was `["null", "long"]`.
    """

    def patch_schema(schema):
        patched = False
        for field in schema["fields"]:
            if field["name"] == "added_snapshot_id":
                field["type"] = ["null", "long"]
                patched = True
        return patched

    def patch_record(record):
        record["added_snapshot_id"] = None

    return _rewrite_manifest_list(avro_bytes, patch_schema, patch_record)


def _negate_manifest_length(avro_bytes):
    """Make `manifest_length` negative.

    The manifest-list read path rejects a negative one with
    ICEBERG_SPECIFICATION_VIOLATION. ClickHouse only ever stores a real file size,
    so only the value is rewritten here and the schema keeps its plain `long`.
    """

    def patch_schema(schema):
        return any(field["name"] == "manifest_length" for field in schema["fields"])

    def patch_record(record):
        record["manifest_length"] = -1

    return _rewrite_manifest_list(avro_bytes, patch_schema, patch_record)


def _drop_partition_spec_id(avro_bytes):
    """Remove `partition_spec_id` from the list's schema entirely.

    The read path requires the field to be present and rejects a list without it.
    ClickHouse always writes it (`AvroSchema.h`), so removing it from the schema is
    what an externally written list has to do to reach this shape.
    """

    def patch_schema(schema):
        before = len(schema["fields"])
        schema["fields"] = [f for f in schema["fields"] if f["name"] != "partition_spec_id"]
        return len(schema["fields"]) != before

    def patch_record(record):
        record.pop("partition_spec_id", None)

    return _rewrite_manifest_list(avro_bytes, patch_schema, patch_record)


def _retype_sequence_number(avro_bytes):
    """Declare `sequence_number` as a string instead of a long.

    A v2 manifest list carrying the field is read with an exact type, so a
    wrong-typed one is refused even though the field itself is optional.
    """

    def patch_schema(schema):
        patched = False
        for field in schema["fields"]:
            if field["name"] == "sequence_number":
                field["type"] = "string"
                patched = True
        return patched

    def patch_record(record):
        record["sequence_number"] = "not-a-long"

    return _rewrite_manifest_list(avro_bytes, patch_schema, patch_record)


def _drop_snapshot_schema_id(metadata_json):
    """Remove `schema-id` from the document's current snapshot.

    Constructing a snapshot requires the field and throws
    ICEBERG_SPECIFICATION_VIOLATION without it, before any manifest or data file is
    opened. ClickHouse always writes it (`MetadataGenerator.cpp`).
    """
    metadata = json.loads(metadata_json)
    snapshot_id = metadata["current-snapshot-id"]
    patched = False
    for snapshot in metadata["snapshots"]:
        if snapshot["snapshot-id"] == snapshot_id and "schema-id" in snapshot:
            del snapshot["schema-id"]
            patched = True
    assert patched, "Current snapshot has no schema-id for this fixture to drop"
    return json.dumps(metadata)


def _delete(cluster, instance, storage_type, table_name, rel_path):
    if storage_type == "local":
        instance.exec_in_container(
            ["bash", "-c", f"rm -f {LOCAL_PREFIX}/{table_name}/{rel_path}"]
        )
        return
    cluster.minio_client.remove_object(
        cluster.minio_bucket, f"{S3_PREFIX}/{table_name}/{rel_path}"
    )


def _exists(cluster, instance, storage_type, table_name, rel_path):
    if storage_type == "local":
        return (
            instance.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"test -e {LOCAL_PREFIX}/{table_name}/{rel_path} && echo yes || echo no",
                ]
            ).strip()
            == "yes"
        )
    try:
        cluster.minio_client.stat_object(
            cluster.minio_bucket, f"{S3_PREFIX}/{table_name}/{rel_path}"
        )
        return True
    except Exception:
        return False


def _list_manifests(cluster, instance, storage_type, table_name):
    """Every `metadata/*.avro` that is not a manifest list, as table-relative paths."""
    if storage_type == "local":
        listing = instance.exec_in_container(
            [
                "bash",
                "-c",
                f"ls {LOCAL_PREFIX}/{table_name}/metadata/*.avro 2>/dev/null || true",
            ]
        )
        names = [line.rsplit("/", 1)[-1] for line in listing.split() if line]
    else:
        prefix = f"{S3_PREFIX}/{table_name}/metadata/"
        names = [
            obj.object_name.rsplit("/", 1)[-1]
            for obj in cluster.minio_client.list_objects(
                cluster.minio_bucket, prefix=prefix, recursive=True
            )
            if obj.object_name.endswith(".avro")
        ]
    return {f"metadata/{name}" for name in names if not name.startswith("snap-")}


def _list_data_files(cluster, instance, storage_type, table_name):
    """Every file outside `metadata/`, i.e. the data and delete files, table-relative."""
    if storage_type == "local":
        listing = instance.exec_in_container(
            [
                "bash",
                "-c",
                f"cd {LOCAL_PREFIX}/{table_name} 2>/dev/null && "
                f"find . -type f ! -path './metadata/*' || true",
            ]
        )
        return {line.lstrip("./") for line in listing.split() if line}
    prefix = f"{S3_PREFIX}/{table_name}/"
    return {
        obj.object_name[len(prefix) :]
        for obj in cluster.minio_client.list_objects(
            cluster.minio_bucket, prefix=prefix, recursive=True
        )
        if not obj.object_name[len(prefix) :].startswith("metadata/")
    }


def _current_snapshot_manifest_list(metadata_json):
    """The manifest list of the document's current snapshot, as a table-relative path."""
    metadata = json.loads(metadata_json)
    snapshot_id = metadata["current-snapshot-id"]
    for snapshot in metadata["snapshots"]:
        if snapshot["snapshot-id"] == snapshot_id:
            raw = snapshot["manifest-list"]
            # Metadata stores the table location as a prefix; the helpers are
            # table-relative, so keep only the metadata/... tail.
            return raw[raw.index("metadata/") :]
    raise AssertionError(f"snapshot {snapshot_id} absent from its own snapshot list")


def _attach_pinned_to_v1(cluster, storage_type, table_name):
    """CREATE over an existing Iceberg table, pinned to v1 and without a schema."""
    settings = (
        "SETTINGS iceberg_metadata_file_path = 'metadata/v1.metadata.json', "
        "iceberg_use_version_hint = true"
    )
    if storage_type == "local":
        engine = f"IcebergLocal(local, path = '{LOCAL_PREFIX}/{table_name}', format=Parquet)"
    else:
        engine = (
            f"IcebergS3(s3, filename = '{S3_PREFIX}/{table_name}/', format=Parquet, "
            f"url = 'http://minio1:9001/{cluster.minio_bucket}/')"
        )
    return f"CREATE TABLE {table_name} ENGINE={engine} {settings}"


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_version_hint_crash_window_does_not_wedge_writes(
    started_cluster_iceberg_no_spark, storage_type
):
    """A commit interrupted between its two writes must not wedge the table.

    The post-crash state needs no crash to construct: it is exactly
    "vN.metadata.json durable, version-hint.text names N-1". Asserts that a
    later INSERT succeeds and that the interrupted commit's row is visible.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_version_hint_crash_window_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        use_version_hint=True,
    )
    instance.query(f"INSERT INTO {table_name} VALUES (1)")
    instance.query(f"INSERT INTO {table_name} VALUES (2)")
    assert instance.query(f"SELECT x FROM {table_name} ORDER BY x") == "1\n2\n"

    # Guard against a fixture that never wrote a hint: the whole scenario is
    # about the hint being consulted, so without this the test would pass on a
    # binary that ignores it entirely.
    hint = _read(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT
    )
    assert hint.isdigit(), f"Fixture did not write a numeric {HINT}, got {hint!r}"
    committed_version = int(hint)
    assert committed_version >= 3

    # Roll the hint back one version: byte-identical to a crash between the
    # metadata write of the last INSERT and its hint advance.
    _write(
        started_cluster_iceberg_no_spark,
        instance,
        storage_type,
        table_name,
        HINT,
        str(committed_version - 1),
    )
    assert (
        _read(started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT)
        == str(committed_version - 1)
    ), "The hint rollback did not take effect"

    # The interrupted transaction's row is invisible while the hint is behind.
    assert instance.query(f"SELECT x FROM {table_name} ORDER BY x") == "1\n"

    # Before the fix this fails with DATALAKE_DATABASE_ERROR after exhausting
    # every retry, and keeps failing forever.
    instance.query(f"INSERT INTO {table_name} VALUES (3)")

    # The hint must have moved past the version it was stuck on.
    healed = _read(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT
    )
    assert healed.isdigit() and int(healed) > committed_version, (
        f"version-hint.text did not advance past {committed_version}, got {healed!r}"
    )

    # The row committed by the interrupted transaction is visible again: the
    # orphaned metadata file was adopted, not discarded. This is what separates
    # healing the hint from merely letting writes through.
    assert instance.query(f"SELECT x FROM {table_name} ORDER BY x") == "1\n2\n3\n"


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_version_hint_not_rewritten_on_an_ordinary_lost_race(
    started_cluster_iceberg_no_spark, storage_type
):
    """Healing must be monotonic: a lost race may never lower the hint.

    The hint is parked above every version this scenario can reach, because
    asserting its final value would be blind: the retry that eventually succeeds
    rewrites it anyway. Parked, any write by either convergence site shows up as
    a smaller number.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_version_hint_lost_race_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        use_version_hint=True,
    )
    instance.query(f"INSERT INTO {table_name} VALUES (1)")
    instance.query(f"INSERT INTO {table_name} VALUES (2)")
    instance.query(f"DROP TABLE {table_name}")

    hint = _read(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT
    )
    assert hint.isdigit(), f"Fixture did not write a numeric {HINT}, got {hint!r}"

    parked = "99"
    _write(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT, parked
    )
    assert (
        _read(started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT)
        == parked
    ), "Could not park the hint above every reachable version"

    # Re-attach to the existing table pinned to v1, so the first commit attempt
    # targets v2, which is already on disk. The schema is intentionally omitted:
    # with one, this would be a fresh table creation and would be refused
    # because the table already exists on storage.
    instance.query(_attach_pinned_to_v1(started_cluster_iceberg_no_spark, storage_type, table_name))
    instance.query(f"INSERT INTO {table_name} VALUES (3)")

    assert (
        _read(started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT)
        == parked
    ), "A lost race lowered version-hint.text"


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_version_hint_not_advanced_to_an_unflushed_metadata_file(
    started_cluster_iceberg_no_spark, storage_type
):
    """The hint may only name a metadata file a reader can actually follow.

    The zero-byte `vN.metadata.json` stands in for a backend that publishes a
    path before committing its content, as ADLS Gen2 does between `Create` and
    `Flush`. Neither `local` nor `s3` behaves that way, so the state is built by
    hand and this is not ADLS coverage.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_version_hint_unflushed_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        use_version_hint=True,
    )
    instance.query(f"INSERT INTO {table_name} VALUES (1)")
    instance.query(f"INSERT INTO {table_name} VALUES (2)")
    assert instance.query(f"SELECT x FROM {table_name} ORDER BY x") == "1\n2\n"

    hint = _read(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT
    )
    assert hint.isdigit(), f"Fixture did not write a numeric {HINT}, got {hint!r}"
    committed_version = int(hint)

    # Create the version the next commit will target, with no content, and leave
    # the hint where it is: a writer created its target and never flushed it.
    unflushed = committed_version + 1
    unflushed_path = f"metadata/v{unflushed}.metadata.json"
    _write(
        started_cluster_iceberg_no_spark,
        instance,
        storage_type,
        table_name,
        unflushed_path,
        "",
    )
    assert (
        _read(
            started_cluster_iceberg_no_spark,
            instance,
            storage_type,
            table_name,
            unflushed_path,
        )
        == ""
    ), "The unflushed metadata file was not created empty"

    # The commit cannot tell whether the other writer will ever flush, so it
    # refuses: a refused write is recoverable, an unreadable table is not.
    instance.query_and_get_error(f"INSERT INTO {table_name} VALUES (3)")

    assert (
        _read(started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT)
        == str(committed_version)
    ), f"version-hint.text was advanced to the unflushed v{unflushed}"

    assert instance.query(f"SELECT x FROM {table_name} ORDER BY x") == "1\n2\n"


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_version_hint_not_advanced_to_a_snapshot_whose_manifest_list_is_gone(
    started_cluster_iceberg_no_spark, storage_type
):
    """A parseable metadata file is not necessarily a followable one.

    A commit whose conditional write reaches storage but reports failure locally
    leaves `vN.metadata.json` behind and then runs its own cleanup, which deletes
    that attempt's manifest entries and manifest list. The retry regenerates the
    same version, meets its own orphan at the existence fence, and must not
    publish it: the document parses, but a reader following its current snapshot
    would look for a manifest list that no longer exists.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_version_hint_no_manifest_list_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        use_version_hint=True,
    )
    instance.query(f"INSERT INTO {table_name} VALUES (1)")
    instance.query(f"INSERT INTO {table_name} VALUES (2)")
    assert instance.query(f"SELECT x FROM {table_name} ORDER BY x") == "1\n2\n"

    hint = _read(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT
    )
    assert hint.isdigit(), f"Fixture did not write a numeric {HINT}, got {hint!r}"
    committed_version = int(hint)

    # Delete the manifest list of the latest metadata file's current snapshot,
    # standing in for the cleanup that a lost-then-retried commit performs.
    metadata_path = f"metadata/v{committed_version}.metadata.json"
    manifest_list = _current_snapshot_manifest_list(
        _read(
            started_cluster_iceberg_no_spark,
            instance,
            storage_type,
            table_name,
            metadata_path,
        )
    )
    _delete(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, manifest_list
    )
    assert not _exists(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, manifest_list
    ), f"Fixture did not delete {manifest_list}"

    # Roll the hint back so the next commit targets the version whose manifest
    # list is now missing and hits the existence fence.
    _write(
        started_cluster_iceberg_no_spark,
        instance,
        storage_type,
        table_name,
        HINT,
        str(committed_version - 1),
    )
    assert (
        _read(started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT)
        == str(committed_version - 1)
    ), "The hint rollback did not take effect"

    # Whether the statement succeeds is not the property under test; not adopting
    # the unfollowable version is.
    try:
        instance.query(f"INSERT INTO {table_name} VALUES (3)")
    except Exception:
        pass

    healed = _read(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT
    )
    assert healed.isdigit(), f"version-hint.text is no longer numeric: {healed!r}"
    assert int(healed) != committed_version, (
        f"version-hint.text was advanced to v{committed_version}, whose manifest "
        f"list is missing"
    )

    # The table still reads. Without the check it does not: the hint names a
    # snapshot whose manifest list cannot be opened.
    instance.query(f"SELECT x FROM {table_name} ORDER BY x")


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_version_hint_not_advanced_to_a_snapshot_whose_manifests_are_gone(
    started_cluster_iceberg_no_spark, storage_type
):
    """A present manifest list does not mean its manifests are still there.

    A commit's cleanup deletes the manifests it wrote before the list that names
    them, so between the two deletions the list survives and points at manifests
    that do not. A retry meeting its own orphan at the existence fence must not
    publish it: the document parses and its list opens, yet a reader following it
    fails on the first manifest.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_version_hint_manifests_gone_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        use_version_hint=True,
    )
    instance.query(f"INSERT INTO {table_name} VALUES (1)")

    # Only the last commit's own manifests may be deleted: cleanup() iterates the
    # manifests that attempt wrote, never an ancestor's. Deleting an ancestor's
    # too would make the version the hint falls back to unreadable, and a broken
    # fixture would then be indistinguishable from a failed guard.
    before = _list_manifests(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name
    )
    instance.query(f"INSERT INTO {table_name} VALUES (2)")
    after = _list_manifests(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name
    )
    own_manifests = after - before
    assert own_manifests, "The last commit added no manifest, so there is nothing to delete"

    hint = _read(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT
    )
    assert hint.isdigit(), f"Fixture did not write a numeric {HINT}, got {hint!r}"
    committed_version = int(hint)

    for manifest in own_manifests:
        _delete(
            started_cluster_iceberg_no_spark, instance, storage_type, table_name, manifest
        )
        assert not _exists(
            started_cluster_iceberg_no_spark, instance, storage_type, table_name, manifest
        ), f"Fixture did not delete {manifest}"

    # The list itself must survive, otherwise this is the already-covered
    # missing-list case and says nothing about the manifests.
    manifest_list = _current_snapshot_manifest_list(
        _read(
            started_cluster_iceberg_no_spark,
            instance,
            storage_type,
            table_name,
            f"metadata/v{committed_version}.metadata.json",
        )
    )
    assert _exists(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, manifest_list
    ), f"Fixture deleted {manifest_list}, which is the missing-list case instead"

    _write(
        started_cluster_iceberg_no_spark,
        instance,
        storage_type,
        table_name,
        HINT,
        str(committed_version - 1),
    )
    assert (
        _read(started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT)
        == str(committed_version - 1)
    ), "The hint rollback did not take effect"

    # Control: the version the hint now names is intact, so a later unreadable
    # table can only come from adopting the damaged one.
    assert instance.query(f"SELECT x FROM {table_name} ORDER BY x") == "1\n"

    # Whether the statement succeeds is not the property under test; not adopting
    # the unfollowable version is.
    try:
        instance.query(f"INSERT INTO {table_name} VALUES (3)")
    except Exception:
        pass

    healed = _read(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT
    )
    assert healed.isdigit(), f"version-hint.text is no longer numeric: {healed!r}"
    assert int(healed) != committed_version, (
        f"version-hint.text was advanced to v{committed_version}, whose manifests "
        f"are missing"
    )

    # Without the check the hint names that version and the read fails on its
    # first manifest.
    instance.query(f"SELECT x FROM {table_name} ORDER BY x")


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_version_hint_not_advanced_to_a_snapshot_whose_data_files_are_gone(
    started_cluster_iceberg_no_spark, storage_type
):
    """Present manifests do not mean the files they name are still there.

    A mutation's cleanup deletes its delete files, then its data files, then its
    manifest entries, then its manifest list. Between the second and third
    deletion every manifest is intact and names files that are gone, so a probe
    stopping at the manifests would publish a snapshot whose first data file a
    reader cannot open. The mutation path is the one whose cleanup ordering makes
    the state reachable, so the fixture goes through DELETE, not INSERT.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_version_hint_data_files_gone_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        use_version_hint=True,
    )
    instance.query(f"INSERT INTO {table_name} VALUES (1)")
    instance.query(f"INSERT INTO {table_name} VALUES (2)")

    # Only the mutation's OWN data and delete files may be deleted: cleanup()
    # iterates the files that attempt wrote, never an ancestor's. Deleting an
    # ancestor's too would make the version the hint falls back to unreadable,
    # and a broken fixture would then be indistinguishable from a failed guard.
    before_files = _list_data_files(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name
    )
    before_manifests = _list_manifests(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name
    )
    instance.query(f"DELETE FROM {table_name} WHERE x = 1")
    own_files = (
        _list_data_files(
            started_cluster_iceberg_no_spark, instance, storage_type, table_name
        )
        - before_files
    )
    own_manifests = (
        _list_manifests(
            started_cluster_iceberg_no_spark, instance, storage_type, table_name
        )
        - before_manifests
    )
    assert own_files, "The mutation wrote no data or delete file, so there is nothing to delete"
    assert own_manifests, "The mutation added no manifest, so the manifests-gone case is untested"

    hint = _read(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT
    )
    assert hint.isdigit(), f"Fixture did not write a numeric {HINT}, got {hint!r}"
    committed_version = int(hint)

    for data_file in own_files:
        _delete(
            started_cluster_iceberg_no_spark, instance, storage_type, table_name, data_file
        )
        assert not _exists(
            started_cluster_iceberg_no_spark, instance, storage_type, table_name, data_file
        ), f"Fixture did not delete {data_file}"

    # The manifests and their list must survive, otherwise this is the
    # already-covered missing-manifest case and says nothing about data files.
    for manifest in own_manifests:
        assert _exists(
            started_cluster_iceberg_no_spark, instance, storage_type, table_name, manifest
        ), f"Fixture deleted {manifest}, which is the missing-manifest case instead"
    manifest_list = _current_snapshot_manifest_list(
        _read(
            started_cluster_iceberg_no_spark,
            instance,
            storage_type,
            table_name,
            f"metadata/v{committed_version}.metadata.json",
        )
    )
    assert _exists(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, manifest_list
    ), f"Fixture deleted {manifest_list}, which is the missing-list case instead"

    _write(
        started_cluster_iceberg_no_spark,
        instance,
        storage_type,
        table_name,
        HINT,
        str(committed_version - 1),
    )
    assert (
        _read(started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT)
        == str(committed_version - 1)
    ), "The hint rollback did not take effect"

    # Control: the version the hint now names is intact, so a later unreadable
    # table can only come from adopting the damaged one.
    assert instance.query(f"SELECT x FROM {table_name} ORDER BY x") == "1\n2\n"

    # Whether the statement succeeds is not the property under test; not adopting
    # the unfollowable version is.
    try:
        instance.query(f"INSERT INTO {table_name} VALUES (3)")
    except Exception:
        pass

    healed = _read(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT
    )
    assert healed.isdigit(), f"version-hint.text is no longer numeric: {healed!r}"
    assert int(healed) != committed_version, (
        f"version-hint.text was advanced to v{committed_version}, whose data "
        f"files are missing"
    )

    # Without the check the hint names that version and the read fails on its
    # first data file.
    instance.query(f"SELECT x FROM {table_name} ORDER BY x")


@pytest.mark.parametrize("storage_type", ["local", "s3"])
@pytest.mark.parametrize(
    "damage,label,damages_metadata_document",
    [
        (_null_out_added_snapshot_id, "null_adder", False),
        (_negate_manifest_length, "negative_manifest_length", False),
        (_drop_partition_spec_id, "no_partition_spec_id", False),
        (_retype_sequence_number, "wrong_typed_sequence_number", False),
        (_drop_snapshot_schema_id, "no_snapshot_schema_id", True),
    ],
)
def test_version_hint_not_advanced_to_a_snapshot_a_reader_refuses(
    started_cluster_iceberg_no_spark, storage_type, damage, label, damages_metadata_document
):
    """Every object being present does not mean a reader can follow the snapshot.

    Before opening any object a reader requires the snapshot to carry a `schema-id`,
    and requires its manifest list to have `added_snapshot_id`, a non-negative
    `manifest_length` and a present `partition_spec_id`, reading the fields it takes
    with an exact type. Breaking any of that throws
    ICEBERG_SPECIFICATION_VIOLATION however intact the files are, so publishing such
    a version would replace a readable v(N-1) with a vN refused at the first read.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_version_hint_" + label + "_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        use_version_hint=True,
    )
    instance.query(f"INSERT INTO {table_name} VALUES (1)")
    instance.query(f"INSERT INTO {table_name} VALUES (2)")

    hint = _read(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT
    )
    assert hint.isdigit(), f"Fixture did not write a numeric {HINT}, got {hint!r}"
    committed_version = int(hint)

    # Only the last commit's own document and list are damaged, so the version the
    # hint falls back to keeps a readable pair of its own. Damaging that one too would
    # make a broken fixture indistinguishable from a failed guard.
    metadata_rel_path = f"metadata/v{committed_version}.metadata.json"
    metadata_json = _read(
        started_cluster_iceberg_no_spark,
        instance,
        storage_type,
        table_name,
        metadata_rel_path,
    )
    if damages_metadata_document:
        _write(
            started_cluster_iceberg_no_spark,
            instance,
            storage_type,
            table_name,
            metadata_rel_path,
            damage(metadata_json),
        )
    else:
        manifest_list = _current_snapshot_manifest_list(metadata_json)
        _write_bytes(
            started_cluster_iceberg_no_spark,
            instance,
            storage_type,
            table_name,
            manifest_list,
            damage(
                _read_bytes(
                    started_cluster_iceberg_no_spark,
                    instance,
                    storage_type,
                    table_name,
                    manifest_list,
                )
            ),
        )

    # Everything the snapshot names must still exist, otherwise this is one of the
    # already-covered missing-object cases and says nothing about the field rules.
    for rel_path in _list_manifests(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name
    ) | _list_data_files(started_cluster_iceberg_no_spark, instance, storage_type, table_name):
        assert _exists(
            started_cluster_iceberg_no_spark, instance, storage_type, table_name, rel_path
        ), f"Fixture removed {rel_path}, which is a missing-object case instead"

    _write(
        started_cluster_iceberg_no_spark,
        instance,
        storage_type,
        table_name,
        HINT,
        str(committed_version - 1),
    )
    assert (
        _read(started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT)
        == str(committed_version - 1)
    ), "The hint rollback did not take effect"

    # Control: the version the hint now names is readable, so a later unreadable
    # table can only come from adopting the damaged one.
    assert instance.query(f"SELECT x FROM {table_name} ORDER BY x") == "1\n"

    # Whether the statement succeeds is not the property under test; not adopting
    # the unfollowable version is.
    try:
        instance.query(f"INSERT INTO {table_name} VALUES (3)")
    except Exception:
        pass

    healed = _read(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name, HINT
    )
    assert healed.isdigit(), f"version-hint.text is no longer numeric: {healed!r}"
    assert int(healed) != committed_version, (
        f"version-hint.text was advanced to v{committed_version}, which a reader "
        f"refuses ({label})"
    )

    # Without the check the hint names that version and the read is refused with
    # ICEBERG_SPECIFICATION_VIOLATION.
    instance.query(f"SELECT x FROM {table_name} ORDER BY x")
