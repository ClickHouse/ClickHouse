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
