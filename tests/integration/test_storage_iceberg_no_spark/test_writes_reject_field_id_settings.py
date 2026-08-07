import uuid

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_creation_expression,
    get_uuid_str,
)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local"])
def test_writes_reject_field_id_settings(
    started_cluster_iceberg_no_spark, format_version, storage_type
):
    """Regression coverage for rejecting user Parquet field-id settings on Iceberg tables.

    When writing to an Iceberg table the datalake metadata (the `column_mapper`)
    is the authoritative source for Parquet `field_id`s. Letting the settings
    `output_format_parquet_column_field_ids` /
    `output_format_parquet_auto_assign_field_ids` override that mapping would
    emit `field_id`s that no longer match the table metadata, breaking
    subsequent reads.

    An object-storage engine freezes its `FormatSettings` from the `CREATE
    TABLE ... SETTINGS` clause and ignores per-`INSERT` session settings, so
    the only way these settings can reach an Iceberg engine write is through
    the table definition — and such a table would be permanently unwritable
    (every `INSERT` rejected by the write-time guard in
    `ParquetBlockOutputFormat`). `createStorageObjectStorage` therefore
    rejects the definition up front: `CREATE TABLE ... ENGINE = Iceberg*
    ... SETTINGS output_format_parquet_column_field_ids = ...` (or
    `output_format_parquet_auto_assign_field_ids = 1`) fails with
    `BAD_ARGUMENTS` at `CREATE` time. The write-time guard stays as defense in
    depth for tables attached from existing metadata.

    This path is not reachable from a stateless (`file()` / `clickhouse-local`)
    test because no Iceberg engine table is involved there.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]

    def make_table_name(suffix):
        return (
            "test_writes_reject_field_id_settings_"
            + storage_type
            + "_"
            + suffix
            + "_"
            + get_uuid_str()
        )

    # A plain CREATE + INSERT works — the datalake column-id mapping is used.
    ok_table = make_table_name("ok")
    create_iceberg_table(
        storage_type,
        instance,
        ok_table,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        format_version,
    )
    instance.query(f"INSERT INTO {ok_table} VALUES (1), (2);")
    assert instance.query(f"SELECT * FROM {ok_table} ORDER BY ALL") == "1\n2\n"

    # A user-issued ATTACH query with a full table definition introduces a
    # fresh definition just like CREATE, so it is rejected the same way.
    attach_table = make_table_name("attach")
    attach_uuid = str(uuid.uuid4())
    error = instance.query_and_get_error(
        f"""
        ATTACH TABLE {attach_table} UUID '{attach_uuid}' (x Int32)
        ENGINE=IcebergLocal(local, path = '/var/lib/clickhouse/user_files/iceberg_data/default/{ok_table}', format=Parquet)
        SETTINGS output_format_parquet_auto_assign_field_ids = 1, iceberg_format_version = {format_version}
        """
    )
    assert "BAD_ARGUMENTS" in error
    assert "column-id mapping" in error
    assert instance.query(f"EXISTS TABLE {attach_table}") == "0\n"

    # A short ATTACH replays the definition stored in this server's metadata,
    # which was already validated at CREATE time, so it keeps working.
    instance.query(f"DETACH TABLE {ok_table}")
    instance.query(f"ATTACH TABLE {ok_table}")
    assert instance.query(f"SELECT * FROM {ok_table} ORDER BY ALL") == "1\n2\n"

    # Baking the auto-assign setting into the table definition is rejected at
    # CREATE time — accepting it would freeze the setting into the engine's
    # FormatSettings and make every subsequent INSERT fail.
    auto_table = make_table_name("auto")
    error = instance.query_and_get_error(
        get_creation_expression(
            storage_type,
            auto_table,
            started_cluster_iceberg_no_spark,
            "(x Int32)",
            format_version,
            additional_settings=["output_format_parquet_auto_assign_field_ids = 1"],
        )
    )
    assert "BAD_ARGUMENTS" in error
    assert "column-id mapping" in error
    assert instance.query(f"EXISTS TABLE {auto_table}") == "0\n"

    # An explicit field-id override map in the table definition is rejected too.
    explicit_table = make_table_name("explicit")
    error = instance.query_and_get_error(
        get_creation_expression(
            storage_type,
            explicit_table,
            started_cluster_iceberg_no_spark,
            "(x Int32)",
            format_version,
            additional_settings=[
                "output_format_parquet_column_field_ids = {'x': '1'}"
            ],
        )
    )
    assert "BAD_ARGUMENTS" in error
    assert "column-id mapping" in error
    assert instance.query(f"EXISTS TABLE {explicit_table}") == "0\n"

    # The same settings coming from the session (or a profile) rather than from
    # the table definition are ignored for Iceberg engines: the table metadata
    # is the source of truth, and rejecting them would make every existing
    # Iceberg table unusable for such a user.
    ambient_table = make_table_name("ambient")
    ambient_settings = {
        "output_format_parquet_auto_assign_field_ids": 1,
        "output_format_parquet_column_field_ids": "{'x': '1'}",
    }
    create_iceberg_table(
        storage_type,
        instance,
        ambient_table,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        format_version,
        settings=ambient_settings,
    )
    instance.query(
        f"INSERT INTO {ambient_table} VALUES (1);", settings=ambient_settings
    )
    assert (
        instance.query(
            f"SELECT * FROM {ambient_table} ORDER BY ALL", settings=ambient_settings
        )
        == "1\n"
    )

    # The same ambient settings must not break writes through the Iceberg
    # table functions either. A table function has no definition that could
    # express an intent to override the ids, so every value reaching the write
    # is ambient (server/profile/session) and is ignored — the datalake
    # column-id mapping stays authoritative.
    table_function_expr = get_creation_expression(
        storage_type,
        ambient_table,
        started_cluster_iceberg_no_spark,
        table_function=True,
    )
    instance.query(
        f"INSERT INTO FUNCTION {table_function_expr} VALUES (2);",
        settings={"allow_insert_into_iceberg": 1, **ambient_settings},
    )
    assert (
        instance.query(
            f"SELECT * FROM {table_function_expr} ORDER BY ALL",
            settings=ambient_settings,
        )
        == "1\n2\n"
    )

    # The two settings are tracked independently: naming one of them in the
    # table definition (here with its default value) must not stop the other
    # one's ambient value from being ignored. The ambient override map would
    # otherwise be treated as definition-supplied and reject this valid CREATE.
    partial_table = make_table_name("partial")
    create_iceberg_table(
        storage_type,
        instance,
        partial_table,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        format_version,
        settings=ambient_settings,
        additional_settings=["output_format_parquet_auto_assign_field_ids = 0"],
    )
    instance.query(
        f"INSERT INTO {partial_table} VALUES (1);", settings=ambient_settings
    )
    assert instance.query(f"SELECT * FROM {partial_table} ORDER BY ALL") == "1\n"

    # Ignoring the ambient settings also covers values that are malformed: they
    # are reset before the `FormatSettings` are built, so they are never parsed.
    # Otherwise a user with a broken value in their profile would still be
    # unable to create, load or write an Iceberg table.
    malformed_settings = {
        "output_format_parquet_column_field_ids": "{'x': 'not_an_integer'}"
    }
    malformed_table = make_table_name("malformed")
    create_iceberg_table(
        storage_type,
        instance,
        malformed_table,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        format_version,
        settings=malformed_settings,
    )
    instance.query(
        f"INSERT INTO {malformed_table} VALUES (1);", settings=malformed_settings
    )
    malformed_table_function_expr = get_creation_expression(
        storage_type,
        malformed_table,
        started_cluster_iceberg_no_spark,
        table_function=True,
    )
    instance.query(
        f"INSERT INTO FUNCTION {malformed_table_function_expr} VALUES (2);",
        settings={"allow_insert_into_iceberg": 1, **malformed_settings},
    )
    # The malformed value is not passed to the `SELECT`: the format of the
    # query result is built from the ambient settings as well, and rejecting a
    # malformed value there is the intended behaviour.
    assert instance.query(f"SELECT * FROM {malformed_table} ORDER BY ALL") == "1\n2\n"

    # A legacy table created before the definition-time guard existed can still
    # carry these settings in its stored definition. Replaying such a stored
    # definition (short ATTACH, server startup, replica recovery, RESTORE) must
    # keep working — reads included — while the write-time guard in
    # `ParquetBlockOutputFormat` rejects every INSERT into it.
    legacy_table = make_table_name("legacy")
    create_iceberg_table(
        storage_type,
        instance,
        legacy_table,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        format_version,
    )
    instance.query(f"INSERT INTO {legacy_table} VALUES (1);")
    metadata_path = f"/var/lib/clickhouse/metadata/default/{legacy_table}.sql"
    # The stored definition can only be patched when the database disk is the
    # local filesystem. With a remote `database_disk` (the `db disk` CI job) the
    # metadata lives in object storage and there is no file to edit, so the rest
    # of this scenario is not reproducible there.
    metadata_is_local_file = (
        instance.exec_in_container(
            ["bash", "-c", f"test -f {metadata_path} && echo yes || echo no"]
        ).strip()
        == "yes"
    )
    if not metadata_is_local_file:
        return

    instance.query(f"DETACH TABLE {legacy_table}")
    # Simulate the pre-guard table by injecting the setting into the stored
    # metadata, then replay it with a short ATTACH.
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"sed -i --follow-symlinks "
            f"'s/^SETTINGS /SETTINGS output_format_parquet_auto_assign_field_ids = 1, /' "
            f"{metadata_path}",
        ]
    )
    instance.query(f"ATTACH TABLE {legacy_table}")
    assert instance.query(f"SELECT * FROM {legacy_table} ORDER BY ALL") == "1\n"
    error = instance.query_and_get_error(f"INSERT INTO {legacy_table} VALUES (2);")
    assert "BAD_ARGUMENTS" in error
    assert "column-id mapping" in error
    # The table stays readable after the rejected write.
    assert instance.query(f"SELECT * FROM {legacy_table} ORDER BY ALL") == "1\n"


def test_plain_object_storage_validates_field_ids_in_definition(
    started_cluster_iceberg_no_spark,
):
    """A plain (non-Iceberg) object-storage table freezes its `FormatSettings` from the
    `CREATE TABLE ... SETTINGS` clause too, so an invalid definition-supplied
    `output_format_parquet_column_field_ids` map used to be accepted at CREATE time
    and then fail every later `INSERT` — an accepted but permanently unwritable
    table. Such a definition is validated up front now and rejected at DDL time,
    while ambient (session/profile) values keep failing at write time only.
    """
    from helpers.config_cluster import minio_access_key, minio_secret_key

    cluster = started_cluster_iceberg_no_spark
    instance = cluster.instances["node1"]
    base_url = f"http://{cluster.minio_host}:{cluster.minio_port}/{cluster.minio_bucket}/plain_field_ids_{get_uuid_str()}"

    def creation_expression(table_name, columns, settings):
        columns_clause = f" ({columns})" if columns else ""
        return (
            f"CREATE TABLE {table_name}{columns_clause} "
            f"ENGINE = S3('{base_url}/{table_name}.parquet', "
            f"'{minio_access_key}', '{minio_secret_key}', 'Parquet') "
            f"SETTINGS {settings}"
        )

    def make_table_name(suffix):
        return f"test_plain_object_storage_field_ids_{suffix}_{get_uuid_str()}"

    # A map referencing a column the table does not have is rejected at CREATE time.
    unknown_table = make_table_name("unknown")
    error = instance.query_and_get_error(
        creation_expression(
            unknown_table,
            "x Int32",
            "output_format_parquet_column_field_ids = {'missing': '1'}",
        )
    )
    assert "BAD_ARGUMENTS" in error
    assert "unknown column" in error
    assert instance.query(f"EXISTS TABLE {unknown_table}") == "0\n"

    # A value that does not parse as an integer is rejected at CREATE time.
    malformed_table = make_table_name("malformed")
    error = instance.query_and_get_error(
        creation_expression(
            malformed_table,
            "x Int32",
            "output_format_parquet_column_field_ids = {'x': 'oops'}",
        )
    )
    assert "BAD_ARGUMENTS" in error
    assert "not an integer" in error
    assert instance.query(f"EXISTS TABLE {malformed_table}") == "0\n"

    # With auto-assign off the map must cover every column.
    partial_table = make_table_name("partial")
    error = instance.query_and_get_error(
        creation_expression(
            partial_table,
            "x Int32, y Int32",
            "output_format_parquet_column_field_ids = {'x': '1'}",
        )
    )
    assert "BAD_ARGUMENTS" in error
    assert "does not cover" in error
    assert instance.query(f"EXISTS TABLE {partial_table}") == "0\n"

    # The header-independent checks run even when the definition has no column
    # list (the schema would be inferred from the data).
    inferred_table = make_table_name("inferred")
    error = instance.query_and_get_error(
        creation_expression(
            inferred_table,
            None,
            "output_format_parquet_column_field_ids = {'x': 'oops'}",
        )
    )
    assert "BAD_ARGUMENTS" in error
    assert "not an integer" in error
    assert instance.query(f"EXISTS TABLE {inferred_table}") == "0\n"

    # A valid definition works end to end, and replaying it (short ATTACH) is
    # not re-validated.
    ok_table = make_table_name("ok")
    instance.query(
        creation_expression(
            ok_table,
            "x Int32, y Int32",
            "output_format_parquet_column_field_ids = {'x': '5', 'y': '7'}",
        )
    )
    instance.query(f"INSERT INTO {ok_table} VALUES (1, 2)")
    assert instance.query(f"SELECT * FROM {ok_table} ORDER BY ALL") == "1\t2\n"
    instance.query(f"DETACH TABLE {ok_table}")
    instance.query(f"ATTACH TABLE {ok_table}")
    assert instance.query(f"SELECT * FROM {ok_table} ORDER BY ALL") == "1\t2\n"

    # A definition without a column list resolves its schema from the existing
    # Parquet object during CREATE; the header-dependent checks rerun against
    # that inferred schema, so an unknown column is still rejected at CREATE
    # time rather than on the first INSERT.
    existing_object_url = f"{base_url}/{ok_table}.parquet"

    def inferred_creation_expression(table_name, settings, explicit_format=True):
        format_clause = ", 'Parquet'" if explicit_format else ""
        return (
            f"CREATE TABLE {table_name} "
            f"ENGINE = S3('{existing_object_url}', "
            f"'{minio_access_key}', '{minio_secret_key}'{format_clause}) "
            f"SETTINGS {settings}"
        )

    inferred_unknown_table = make_table_name("inferred_unknown")
    error = instance.query_and_get_error(
        inferred_creation_expression(
            inferred_unknown_table,
            "output_format_parquet_column_field_ids = {'missing': '1'}",
        )
    )
    assert "BAD_ARGUMENTS" in error
    assert "unknown column" in error
    assert instance.query(f"EXISTS TABLE {inferred_unknown_table}") == "0\n"

    # Same for a map that does not cover the whole inferred schema.
    inferred_partial_table = make_table_name("inferred_partial")
    error = instance.query_and_get_error(
        inferred_creation_expression(
            inferred_partial_table,
            "output_format_parquet_column_field_ids = {'x': '1'}",
        )
    )
    assert "BAD_ARGUMENTS" in error
    assert "does not cover" in error
    assert instance.query(f"EXISTS TABLE {inferred_partial_table}") == "0\n"

    # And when the format itself is inferred (no explicit format argument).
    inferred_format_table = make_table_name("inferred_format")
    error = instance.query_and_get_error(
        inferred_creation_expression(
            inferred_format_table,
            "output_format_parquet_column_field_ids = {'missing': '1'}",
            explicit_format=False,
        )
    )
    assert "BAD_ARGUMENTS" in error
    assert "unknown column" in error
    assert instance.query(f"EXISTS TABLE {inferred_format_table}") == "0\n"

    # A valid map over the inferred schema still works end to end.
    inferred_ok_table = make_table_name("inferred_ok")
    instance.query(
        inferred_creation_expression(
            inferred_ok_table,
            "output_format_parquet_column_field_ids = {'x': '5', 'y': '7'}",
        )
    )
    assert (
        instance.query(f"SELECT * FROM {inferred_ok_table} ORDER BY ALL") == "1\t2\n"
    )
