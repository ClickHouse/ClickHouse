"""A projection whose definition cannot be analyzed is skipped at `LoadingStrictnessLevel::FORCE_ATTACH` or above.

Only the server's own metadata load reaches that, so restarting a real server is the
only way to reach the skip: an explicit `ATTACH TABLE` runs one level lower and throws instead of
skipping, and `UNDROP TABLE` throws earlier still, while parsing the stored statement. Hence an
integration test rather than a stateless one.

`enable_positional_arguments_for_projections` defaults to false. A projection body written with
positional arguments can be accepted while the setting is on and then cannot be analyzed at a
later startup. That is the state a server upgrade leaves behind.
"""

import os
import re
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.database_disk import read_metadata, write_metadata
from helpers.test_tools import assert_eq_with_retry

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
POSITIONAL_XML = "/etc/clickhouse-server/users.d/positional.xml"
POSITIONAL = {"enable_positional_arguments_for_projections": 1}

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    stay_alive=True,
    with_zookeeper=True,
    main_configs=["configs/backups_disk.xml", "configs/remote_servers.xml"],
    external_dirs=["/backups/"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def replace_codec_in_backup(backup_name, database, old_codec, new_codec):
    """Change one codec spelling in isolated backup metadata, keeping its byte length."""
    assert len(old_codec) == len(new_codec)
    metadata_path = f"/backups/{backup_name}/metadata/{database}/source.sql"
    original = node.exec_in_container(["cat", metadata_path], privileged=True, user="root")
    assert old_codec in original, original
    node.exec_in_container(
        ["sed", "-i", f"s/{old_codec}/{new_codec}/", metadata_path],
        privileged=True,
        user="root",
    )
    assert new_codec in node.exec_in_container(
        ["cat", metadata_path], privileged=True, user="root"
    )


def projections(table):
    return node.query(
        f"SELECT count() FROM system.projections WHERE database = 'dl' AND table = '{table}'"
    ).strip()


def active_projection_parts(table):
    return node.query(
        "SELECT count() FROM system.projection_parts"
        f" WHERE database = 'dl' AND table = '{table}' AND active"
    ).strip()


def declarations_on_disk(table):
    """How many projections the table's stored statement declares.

    The statement lives on the database disk, which is not always the node's own filesystem, so it
    is read through `clickhouse disks`. A path that holds nothing reads back as an empty string at
    exit code 0, and counting projections in that would be vacuous, so it is rejected first.
    """
    metadata_path = node.query(
        f"SELECT metadata_path FROM system.tables WHERE database = 'dl' AND name = '{table}'"
    ).strip()
    statement = read_metadata(node, metadata_path)
    assert (
        "ATTACH TABLE" in statement
    ), f"read no stored statement for `{table}` from {metadata_path!r}: {statement!r}"
    return statement.count("PROJECTION ")


def part_types(table):
    return node.query(
        "SELECT DISTINCT part_type FROM system.parts"
        f" WHERE database = 'dl' AND table = '{table}' AND active"
    ).strip()


def projection_dirs_in_active_parts(table):
    """Which `<name>.proj` directories the table's active parts hold on disk.

    `system.projection_parts` cannot answer this for a declaration that could not be analyzed: it
    dereferences the analyzed description, which does not exist.
    """
    paths = node.query(
        "SELECT path FROM system.parts"
        f" WHERE database = 'dl' AND table = '{table}' AND active"
    ).split()
    found = []
    for path in paths:
        found += node.exec_in_container(
            ["bash", "-c", f"ls -1 {path} | grep '\\.proj$' || true"]
        ).split()
    return sorted(found)


def broken_projection_parts(table):
    return node.query(
        "SELECT count() FROM system.projection_parts"
        f" WHERE database = 'dl' AND table = '{table}' AND active AND is_broken"
    ).strip()


def check_table(table):
    return node.query(
        f"CHECK TABLE dl.{table}", settings={"check_query_single_value_result": 1}
    ).strip()


def event_value(event):
    """A ProfileEvent counter, 0 when the event has not fired since this server started."""
    return int(
        node.query(
            f"SELECT sum(value) FROM system.events WHERE event = '{event}'"
        ).strip()
    )


def test_restore_part_default_codec_requires_untyped_byte_stream_compatibility(started_cluster):
    database = "codec_part_default_restore"
    node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    node.query(f"CREATE DATABASE {database}")
    node.query(
        f"CREATE TABLE {database}.source "
        "(k UInt64, x Float64 CODEC(NONE), "
        "PROJECTION p (x CODEC(Default)) AS (SELECT k, x ORDER BY k)) "
        "ENGINE = MergeTree ORDER BY k SETTINGS default_compression_codec = 'LZ4'"
    )
    backup = f"part_default_codec_{uuid.uuid4().hex}"
    node.query(f"BACKUP TABLE {database}.source TO Disk('backups', '{backup}')")
    restore = f"RESTORE TABLE {database}.source AS {database}.restored FROM Disk('backups', '{backup}')"

    replace_codec_in_backup(
        backup, database, "default_compression_codec = 'LZ4'", "default_compression_codec = 'SZ3'"
    )
    error = node.query_and_get_error(restore, settings={"enable_sz3_codec": 1})
    assert "Codec SZ3 is lossy" in error, error
    assert node.query(f"EXISTS TABLE {database}.restored").strip() == "0"

    replace_codec_in_backup(
        backup, database, "default_compression_codec = 'SZ3'", "default_compression_codec = 'T64'"
    )
    error = node.query_and_get_error(restore)
    assert "Cannot validate codec T64 without a column type" in error, error
    assert node.query(f"EXISTS TABLE {database}.restored").strip() == "0"

    replace_codec_in_backup(
        backup, database, "default_compression_codec = 'T64'", "default_compression_codec = 'LZ4'"
    )
    node.query(restore)
    node.query(f"INSERT INTO {database}.restored (k, x) VALUES (1, 1.125)")
    assert node.query(f"SELECT x FROM {database}.restored").strip() == "1.125"
    assert node.query(
        "SELECT count() FROM system.projection_parts "
        f"WHERE database = '{database}' AND table = 'restored' AND name = 'p' AND active"
    ).strip() == "1"


def test_restore_recompression_ttl_requires_untyped_byte_stream_compatibility(started_cluster):
    database = "codec_ttl_restore"
    node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    node.query(f"CREATE DATABASE {database}")
    node.query(
        f"CREATE TABLE {database}.source (dt DateTime, k UInt64, x String CODEC(NONE)) "
        "ENGINE = MergeTree ORDER BY k TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(LZ4)"
    )
    backup = f"part_ttl_codec_{uuid.uuid4().hex}"
    node.query(f"BACKUP TABLE {database}.source TO Disk('backups', '{backup}')")
    restore = f"RESTORE TABLE {database}.source AS {database}.restored FROM Disk('backups', '{backup}')"

    replace_codec_in_backup(backup, database, "RECOMPRESS CODEC(LZ4)", "RECOMPRESS CODEC(T64)")
    error = node.query_and_get_error(restore)
    assert "Cannot validate codec T64 without a column type" in error, error
    assert node.query(f"EXISTS TABLE {database}.restored").strip() == "0"

    replace_codec_in_backup(backup, database, "RECOMPRESS CODEC(T64)", "RECOMPRESS CODEC(LZ4)")
    node.query(restore)
    node.query(f"INSERT INTO {database}.restored VALUES (now() - INTERVAL 1 DAY, 1, 'a')")
    node.query(f"INSERT INTO {database}.restored VALUES (now() - INTERVAL 1 DAY, 2, 'b')")
    node.query(f"OPTIMIZE TABLE {database}.restored FINAL")
    assert node.query(f"SELECT x FROM {database}.restored ORDER BY k") == "a\nb\n"


def test_restore_unavailable_projection_validates_codec_output_type(started_cluster):
    node.query("DROP DATABASE IF EXISTS codec_restore SYNC")
    node.query("CREATE DATABASE codec_restore")
    node.query(
        "CREATE TABLE codec_restore.source (a UInt64, b UInt64) "
        "ENGINE = MergeTree ORDER BY a"
    )
    node.query(
        "ALTER TABLE codec_restore.source ADD PROJECTION pp (b CODEC(Gorilla)) "
        "AS (SELECT b, a GROUP BY 1, 2)",
        settings={**POSITIONAL, "allow_suspicious_codecs": 1},
    )

    backup = f"unavailable_projection_gorilla_{uuid.uuid4().hex}"
    node.query(f"BACKUP TABLE codec_restore.source TO Disk('backups', '{backup}')")
    restore_query = (
        "RESTORE TABLE codec_restore.source AS codec_restore.restored "
        f"FROM Disk('backups', '{backup}')"
    )
    unavailable = {"enable_positional_arguments_for_projections": 0}
    error = node.query_and_get_error(restore_query, settings=unavailable)
    assert "suspicious" in error.lower(), error
    assert node.query("EXISTS TABLE codec_restore.restored").strip() == "0"

    node.query(
        restore_query,
        settings={**unavailable, "allow_suspicious_codecs": 1},
    )
    assert node.query(
        "SELECT count() FROM system.projections "
        "WHERE database = 'codec_restore' AND table = 'restored'"
    ).strip() == "0"
    assert "CODEC(Gorilla)" in node.query("SHOW CREATE TABLE codec_restore.restored")


def test_restore_codec_projection_with_missing_dictionary(started_cluster):
    node.query("DROP DATABASE IF EXISTS codec_restore_missing_dict SYNC")
    node.query("CREATE DATABASE codec_restore_missing_dict")
    node.query(
        "CREATE TABLE codec_restore_missing_dict.lookup_source "
        "(id UInt64, value UInt64) ENGINE = Memory"
    )
    node.query(
        "CREATE DICTIONARY codec_restore_missing_dict.lookup "
        "(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() "
        "DB 'codec_restore_missing_dict' TABLE 'lookup_source')) "
        "LAYOUT(FLAT()) LIFETIME(0)"
    )
    node.query(
        "CREATE TABLE codec_restore_missing_dict.source "
        "(a UInt64, PROJECTION pp (a CODEC(ZSTD)) AS "
        "(SELECT a, dictGet('codec_restore_missing_dict.lookup', 'value', a) AS d ORDER BY a)) "
        "ENGINE = MergeTree ORDER BY a"
    )
    node.query(
        "CREATE TABLE codec_restore_missing_dict.suspicious_source "
        "(a UInt64, PROJECTION pp (a CODEC(Gorilla)) AS "
        "(SELECT a, dictGet('codec_restore_missing_dict.lookup', 'value', a) AS d ORDER BY a)) "
        "ENGINE = MergeTree ORDER BY a",
        settings={"allow_suspicious_codecs": 1},
    )
    node.query(
        "CREATE TABLE codec_restore_missing_dict.typed_source "
        "(k UInt64, a Float64, PROJECTION pp (a Float64 CODEC(Gorilla)) AS "
        "(SELECT a, dictGet('codec_restore_missing_dict.lookup', 'value', k) AS d ORDER BY a)) "
        "ENGINE = MergeTree ORDER BY k"
    )
    node.query(
        "DROP DICTIONARY codec_restore_missing_dict.lookup "
        "SETTINGS check_table_dependencies = 0"
    )
    node.restart_clickhouse()
    assert (
        node.query(
            "SELECT count() FROM system.projections "
            "WHERE database = 'codec_restore_missing_dict' AND table = 'source'"
        ).strip()
        == "0"
    )

    backup = f"unavailable_projection_missing_dict_{uuid.uuid4().hex}"
    node.query(
        f"BACKUP TABLE codec_restore_missing_dict.source TO Disk('backups', '{backup}')"
    )
    node.query(
        "RESTORE TABLE codec_restore_missing_dict.source "
        f"AS codec_restore_missing_dict.restored FROM Disk('backups', '{backup}')"
    )
    assert (
        node.query(
            "SELECT count() FROM system.projections "
            "WHERE database = 'codec_restore_missing_dict' AND table = 'restored'"
        ).strip()
        == "0"
    )
    assert "CODEC(ZSTD(" in node.query(
        "SHOW CREATE TABLE codec_restore_missing_dict.restored"
    )

    suspicious_backup = f"unavailable_projection_suspicious_dict_{uuid.uuid4().hex}"
    node.query(
        "BACKUP TABLE codec_restore_missing_dict.suspicious_source "
        f"TO Disk('backups', '{suspicious_backup}')"
    )
    restore_suspicious = (
        "RESTORE TABLE codec_restore_missing_dict.suspicious_source "
        f"AS codec_restore_missing_dict.suspicious_restored FROM Disk('backups', '{suspicious_backup}')"
    )
    error = node.query_and_get_error(restore_suspicious)
    assert "non-floating-point" in error, error
    assert (
        node.query(
            "EXISTS TABLE codec_restore_missing_dict.suspicious_restored"
        ).strip()
        == "0"
    )

    node.query(restore_suspicious, settings={"allow_suspicious_codecs": 1})
    assert "CODEC(Gorilla)" in node.query(
        "SHOW CREATE TABLE codec_restore_missing_dict.suspicious_restored"
    )

    typed_backup = f"unavailable_projection_typed_dict_{uuid.uuid4().hex}"
    node.query(
        "BACKUP TABLE codec_restore_missing_dict.typed_source "
        f"TO Disk('backups', '{typed_backup}')"
    )
    node.query(
        "RESTORE TABLE codec_restore_missing_dict.typed_source "
        f"AS codec_restore_missing_dict.typed_restored FROM Disk('backups', '{typed_backup}')"
    )
    assert "CODEC(Gorilla(8))" in node.query(
        "SHOW CREATE TABLE codec_restore_missing_dict.typed_restored"
    )


@pytest.mark.parametrize("projection_column", ["x Float64", "x"])
def test_restore_unavailable_projection_rejects_lossy_codec(started_cluster, projection_column):
    node.query("DROP DATABASE IF EXISTS codec_restore_lossy SYNC")
    node.query("CREATE DATABASE codec_restore_lossy")
    fresh_error = node.query_and_get_error(
        "CREATE TABLE codec_restore_lossy.fresh "
        f"(k UInt64, x Float64, PROJECTION pp ({projection_column} CODEC(SZ3)) AS "
        "(SELECT k, x ORDER BY k)) ENGINE = MergeTree ORDER BY k",
        settings={"enable_sz3_codec": 1, "allow_suspicious_codecs": 1},
    )
    assert "cannot use lossy codec" in fresh_error, fresh_error
    node.query(
        "CREATE TABLE codec_restore_lossy.lookup_source "
        "(id UInt64, value UInt64) ENGINE = Memory"
    )
    node.query(
        "CREATE DICTIONARY codec_restore_lossy.lookup "
        "(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() "
        "DB 'codec_restore_lossy' TABLE 'lookup_source')) "
        "LAYOUT(FLAT()) LIFETIME(0)"
    )
    node.query(
        "CREATE TABLE codec_restore_lossy.source "
        f"(k UInt64, x Float64, PROJECTION pp ({projection_column} CODEC(LZ4)) AS "
        "(SELECT x, dictGet('codec_restore_lossy.lookup', 'value', k) AS d ORDER BY x)) "
        "ENGINE = MergeTree ORDER BY k"
    )

    # Simulate a backup from older metadata with a codec that current projection DDL rejects.
    metadata_path = node.query(
        "SELECT metadata_path FROM system.tables "
        "WHERE database = 'codec_restore_lossy' AND name = 'source'"
    ).strip()
    node.query("DETACH TABLE codec_restore_lossy.source")
    metadata = read_metadata(node, metadata_path)
    assert "CODEC(LZ4)" in metadata, metadata
    write_metadata(node, metadata_path, metadata.replace("CODEC(LZ4)", "CODEC(SZ3)"))
    node.query(
        "DROP DICTIONARY codec_restore_lossy.lookup SETTINGS check_table_dependencies = 0"
    )
    node.restart_clickhouse()
    assert node.query(
        "SELECT count() FROM system.projections "
        "WHERE database = 'codec_restore_lossy' AND table = 'source'"
    ).strip() == "0"

    backup = f"unavailable_projection_lossy_{uuid.uuid4().hex}"
    node.query(f"BACKUP TABLE codec_restore_lossy.source TO Disk('backups', '{backup}')")
    restore_query = (
        "RESTORE TABLE codec_restore_lossy.source AS codec_restore_lossy.restored "
        f"FROM Disk('backups', '{backup}')"
    )
    error = node.query_and_get_error(
        restore_query,
        settings={"enable_sz3_codec": 1, "allow_suspicious_codecs": 1},
    )
    assert "lossy" in error.lower(), error
    assert node.query("EXISTS TABLE codec_restore_lossy.restored").strip() == "0"


@pytest.mark.parametrize(
    ("projection_column", "select_expression", "codec", "declared_type", "source_type"),
    [
        ("x", "x", "T64", "", "String"),
        ("x", "x", "T64", "", "UInt64"),
        ("`toFloat64(k)`", "toFloat64(k)", "T64", "", "String"),
        ("`toFloat64(k)`", "toFloat64(k)", "Delta", "", "String"),
        ("`toFloat64(k)`", "toFloat64(k)", "GCD", "", "String"),
        ("`toFloat64(k)`", "toFloat64(k)", "Quantized('int8', 8)", "", "String"),
        ("`toFloat64(k)`", "toFloat64(k)", "LZ4", "", "String"),
        ("`toFloat64(k)`", "toFloat64(k)", "T64", "UInt64", "String"),
    ],
)
def test_restore_unavailable_projection_requires_proven_codec_output_type(
    started_cluster, projection_column, select_expression, codec, declared_type, source_type
):
    node.query("DROP DATABASE IF EXISTS codec_restore_untyped SYNC")
    node.query("CREATE DATABASE codec_restore_untyped")
    node.query(
        "CREATE TABLE codec_restore_untyped.lookup_source "
        "(id UInt64, value UInt64) ENGINE = Memory"
    )
    node.query(
        "CREATE DICTIONARY codec_restore_untyped.lookup "
        "(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() "
        "DB 'codec_restore_untyped' TABLE 'lookup_source')) "
        "LAYOUT(FLAT()) LIFETIME(0)"
    )
    lookup = "dictGet('codec_restore_untyped.lookup', 'value', k)"
    node.query(
        "CREATE TABLE codec_restore_untyped.source "
        f"(k UInt64, x {source_type}, PROJECTION pp ({projection_column} CODEC(LZ4)) AS "
        f"(SELECT {select_expression}, {lookup} AS d "
        f"GROUP BY {select_expression}, {lookup})) "
        "ENGINE = MergeTree ORDER BY k"
    )

    # Start with valid metadata, then substitute a codec to exercise restore admission.
    metadata_path = node.query(
        "SELECT metadata_path FROM system.tables "
        "WHERE database = 'codec_restore_untyped' AND name = 'source'"
    ).strip()
    node.query("DETACH TABLE codec_restore_untyped.source")
    metadata = read_metadata(node, metadata_path)
    assert "CODEC(LZ4)" in metadata, metadata
    replacement = f"{declared_type} CODEC({codec})" if declared_type else f"CODEC({codec})"
    write_metadata(node, metadata_path, metadata.replace("CODEC(LZ4)", replacement))
    node.query(
        "DROP DICTIONARY codec_restore_untyped.lookup SETTINGS check_table_dependencies = 0"
    )
    node.restart_clickhouse()
    assert node.query(
        "SELECT count() FROM system.projections "
        "WHERE database = 'codec_restore_untyped' AND table = 'source'"
    ).strip() == "0"

    backup = f"unavailable_projection_untyped_{uuid.uuid4().hex}"
    node.query(f"BACKUP TABLE codec_restore_untyped.source TO Disk('backups', '{backup}')")
    restore_query = (
        "RESTORE TABLE codec_restore_untyped.source AS codec_restore_untyped.restored "
        f"FROM Disk('backups', '{backup}')"
    )
    if codec == "LZ4" or (codec == "T64" and source_type == "UInt64"):
        node.query(restore_query)
        assert f"CODEC({codec})" in node.query(
            "SHOW CREATE TABLE codec_restore_untyped.restored"
        )
    else:
        settings = {"enable_quantized_codec": 1} if codec.startswith("Quantized") else {}
        error = node.query_and_get_error(restore_query, settings=settings)
        if declared_type:
            assert "cannot verify" in error.lower(), error
        else:
            assert codec.split("(")[0] in error, error
        if projection_column != "x" and not declared_type:
            assert "without a column type" in error, error
        assert node.query("EXISTS TABLE codec_restore_untyped.restored").strip() == "0"


@pytest.mark.parametrize("part_offset_expression", ["_part_offset", "_part_offset AS parent_offset"])
def test_restore_unavailable_projection_checks_destination_requirements(started_cluster, part_offset_expression):
    node.query("DROP DATABASE IF EXISTS restore_unavailable_gate SYNC")
    node.query("CREATE DATABASE restore_unavailable_gate")
    node.query(
        "CREATE TABLE restore_unavailable_gate.lookup_source "
        "(id UInt64, value UInt64) ENGINE = Memory"
    )
    node.query(
        "CREATE DICTIONARY restore_unavailable_gate.lookup "
        "(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() "
        "DB 'restore_unavailable_gate' TABLE 'lookup_source')) "
        "LAYOUT(FLAT()) LIFETIME(0)"
    )
    node.query(
        "CREATE TABLE restore_unavailable_gate.source "
        f"(a UInt64, PROJECTION pp (SELECT a, {part_offset_expression}, "
        "dictGet('restore_unavailable_gate.lookup', 'value', a) AS d ORDER BY a)) "
        "ENGINE = MergeTree ORDER BY a "
        "SETTINGS allow_part_offset_column_in_projections = 1",
    )

    # Simulate a legacy backup whose table setting no longer permits the stored projection.
    metadata_path = node.query(
        "SELECT metadata_path FROM system.tables "
        "WHERE database = 'restore_unavailable_gate' AND name = 'source'"
    ).strip()
    node.query("DETACH TABLE restore_unavailable_gate.source")
    metadata = read_metadata(node, metadata_path)
    assert "allow_part_offset_column_in_projections = 1" in metadata
    updated_metadata = metadata.replace(
        "allow_part_offset_column_in_projections = 1",
        "allow_part_offset_column_in_projections = 0",
    )
    write_metadata(node, metadata_path, updated_metadata)
    assert read_metadata(node, metadata_path) == updated_metadata
    node.query("ATTACH TABLE restore_unavailable_gate.source")
    node.query(
        "DROP DICTIONARY restore_unavailable_gate.lookup "
        "SETTINGS check_table_dependencies = 0"
    )
    node.restart_clickhouse()
    assert node.query(
        "SELECT count() FROM system.projections "
        "WHERE database = 'restore_unavailable_gate' AND table = 'source'"
    ).strip() == "0"

    backup = f"restore_unavailable_gate_{uuid.uuid4().hex}"
    node.query(
        f"BACKUP TABLE restore_unavailable_gate.source TO Disk('backups', '{backup}')"
    )
    error = node.query_and_get_error(
        "RESTORE TABLE restore_unavailable_gate.source "
        f"AS restore_unavailable_gate.restored FROM Disk('backups', '{backup}')",
    )
    assert "allow_part_offset_column_in_projections" in error, error
    assert node.query("EXISTS TABLE restore_unavailable_gate.restored").strip() == "0"


def test_aliased_block_virtual_columns_require_projection_gates(started_cluster):
    node.query("DROP DATABASE IF EXISTS aliased_block_gate SYNC")
    node.query("CREATE DATABASE aliased_block_gate")
    projection = (
        "PROJECTION pp (SELECT a, _block_number AS bn, _block_offset AS bo ORDER BY a)"
    )
    for setting in (
        "allow_commit_order_projection",
        "enable_block_number_column",
        "enable_block_offset_column",
    ):
        settings = {
            "allow_commit_order_projection": 1,
            "enable_block_number_column": 1,
            "enable_block_offset_column": 1,
        }
        settings[setting] = 0
        settings_sql = ", ".join(f"{name} = {value}" for name, value in settings.items())
        error = node.query_and_get_error(
            f"CREATE TABLE aliased_block_gate.{setting} (a UInt64, {projection}) "
            f"ENGINE = MergeTree ORDER BY a SETTINGS {settings_sql}"
        )
        assert setting in error, error
        assert node.query(
            f"EXISTS TABLE aliased_block_gate.{setting}"
        ).strip() == "0"


def test_restore_unavailable_aliased_block_columns_checks_gates(started_cluster):
    node.query("DROP DATABASE IF EXISTS restore_aliased_block_gate SYNC")
    node.query("CREATE DATABASE restore_aliased_block_gate")
    node.query(
        "CREATE TABLE restore_aliased_block_gate.lookup_source "
        "(id UInt64, value UInt64) ENGINE = Memory"
    )
    node.query(
        "CREATE DICTIONARY restore_aliased_block_gate.lookup "
        "(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() "
        "DB 'restore_aliased_block_gate' TABLE 'lookup_source')) "
        "LAYOUT(FLAT()) LIFETIME(0)"
    )
    node.query(
        "CREATE TABLE restore_aliased_block_gate.source "
        "(a UInt64, PROJECTION pp (SELECT a, _block_number AS bn, "
        "_block_offset AS bo, "
        "dictGet('restore_aliased_block_gate.lookup', 'value', a) AS d ORDER BY a)) "
        "ENGINE = MergeTree ORDER BY a SETTINGS allow_commit_order_projection = 1, "
        "enable_block_number_column = 1, enable_block_offset_column = 1"
    )

    metadata_path = node.query(
        "SELECT metadata_path FROM system.tables "
        "WHERE database = 'restore_aliased_block_gate' AND name = 'source'"
    ).strip()
    node.query("DETACH TABLE restore_aliased_block_gate.source")
    metadata = read_metadata(node, metadata_path)
    assert "allow_commit_order_projection = 1" in metadata
    write_metadata(
        node,
        metadata_path,
        metadata.replace("allow_commit_order_projection = 1", "allow_commit_order_projection = 0"),
    )
    node.query("ATTACH TABLE restore_aliased_block_gate.source")
    node.query(
        "DROP DICTIONARY restore_aliased_block_gate.lookup "
        "SETTINGS check_table_dependencies = 0"
    )
    node.restart_clickhouse()
    assert node.query(
        "SELECT count() FROM system.projections "
        "WHERE database = 'restore_aliased_block_gate' AND table = 'source'"
    ).strip() == "0"

    backup = f"restore_aliased_block_gate_{uuid.uuid4().hex}"
    node.query(
        f"BACKUP TABLE restore_aliased_block_gate.source TO Disk('backups', '{backup}')"
    )
    error = node.query_and_get_error(
        "RESTORE TABLE restore_aliased_block_gate.source "
        f"AS restore_aliased_block_gate.restored FROM Disk('backups', '{backup}')"
    )
    assert "allow_commit_order_projection" in error, error
    assert node.query("EXISTS TABLE restore_aliased_block_gate.restored").strip() == "0"

    for setting in ("enable_block_number_column", "enable_block_offset_column"):
        error = node.query_and_get_error(
            "ALTER TABLE restore_aliased_block_gate.source "
            f"MODIFY SETTING {setting} = 0"
        )
        assert setting in error, error


def test_unavailable_projection_is_not_deleted_by_alter(started_cluster):
    node.query("DROP DATABASE IF EXISTS dl SYNC")
    node.query("CREATE DATABASE dl")
    node.query("CREATE TABLE dl.t (a UInt64, b String) ENGINE = MergeTree ORDER BY a")
    node.query("CREATE TABLE dl.t2 (a UInt64, b String) ENGINE = MergeTree ORDER BY a")

    # The setting is what makes these declarations analyzable at all, so the fixture cannot be built
    # without it and this test cannot go vacuously green if it is ever retired.
    error = node.query_and_get_error(
        "ALTER TABLE dl.t ADD PROJECTION pp (SELECT b, a GROUP BY 1, 2)"
    )
    assert "not under aggregate function and not in GROUP BY keys" in error

    node.query(
        "ALTER TABLE dl.t ADD PROJECTION pp (SELECT b, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    node.query(
        "ALTER TABLE dl.t2 ADD PROJECTION pp (SELECT b, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    node.query(
        "ALTER TABLE dl.t2 ADD PROJECTION qq (SELECT a, b GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    node.query("INSERT INTO dl.t SELECT number, toString(number) FROM numbers(100)")
    node.query("INSERT INTO dl.t2 SELECT number, toString(number) FROM numbers(100)")

    # A wide part, so a mutation that touches one column hardlinks the files it was not told to skip
    # instead of rewriting the whole part.
    node.query(
        "CREATE TABLE dl.t3 (a UInt64, b String, c String) ENGINE = MergeTree ORDER BY a"
        " SETTINGS min_bytes_for_wide_part = 0"
    )
    node.query(
        "ALTER TABLE dl.t3 ADD PROJECTION pp (SELECT b, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    node.query(
        "INSERT INTO dl.t3 SELECT number, toString(number), '' FROM numbers(100)"
    )

    # Anti-vacuity for the mutation oracles below: a Compact part, or any full rewrite, drops `pp.proj`
    # for a reason this PR does not own.
    assert part_types("t3") == "Wide"

    # `t4` pairs an unanalyzable declaration with an analyzable one, so `DROP PROJECTION qq` is
    # allowed while `pp` is unavailable. Wide, so that mutation hardlinks what it was not told to skip.
    node.query(
        "CREATE TABLE dl.t4 (a UInt64, b String) ENGINE = MergeTree ORDER BY a"
        " SETTINGS min_bytes_for_wide_part = 0"
    )
    node.query(
        "ALTER TABLE dl.t4 ADD PROJECTION pp (SELECT b, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    # `qq` uses no positional arguments, so it can be analyzed without the setting; the setting is on
    # here only because this ALTER re-derives `pp` too (`AlterCommands::apply`), as `t2`'s do.
    node.query(
        "ALTER TABLE dl.t4 ADD PROJECTION qq (SELECT a, b GROUP BY a, b)",
        settings=POSITIONAL,
    )
    node.query("INSERT INTO dl.t4 SELECT number, toString(number) FROM numbers(100)")

    # `t5` is `t2`'s fixture on a Wide part, so a mutation that runs no writer takes the hardlink path
    # and both obligations of `CLEAR PROJECTION` are observable at once.
    node.query(
        "CREATE TABLE dl.t5 (a UInt64, b String) ENGINE = MergeTree ORDER BY a"
        " SETTINGS min_bytes_for_wide_part = 0"
    )
    node.query(
        "ALTER TABLE dl.t5 ADD PROJECTION pp (SELECT b, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    node.query(
        "ALTER TABLE dl.t5 ADD PROJECTION qq (SELECT a, b GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    node.query("INSERT INTO dl.t5 SELECT number, toString(number) FROM numbers(100)")

    # The positional expression makes this declaration unavailable after restart. Its accepted
    # codec must not require the old session's setting when the source column type changes.
    node.query("CREATE TABLE dl.t6 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t6 ADD PROJECTION pp (b CODEC(Delta, Delta)) "
        "AS (SELECT b, a GROUP BY 1, 2)",
        settings={**POSITIONAL, "allow_suspicious_codecs": 1},
    )
    node.query("INSERT INTO dl.t6 SELECT number, number FROM numbers(100)")

    # Unlike `t6`'s untyped codec, `t7` pins the output type of `b`. Its declaration
    # must remain valid when an ALTER runs while the projection cannot be analyzed.
    node.query("CREATE TABLE dl.t7 (a UInt64, b UInt64, d UInt64, c UInt64) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t7 ADD PROJECTION pp (b UInt64 CODEC(ZSTD)) "
        "AS (SELECT b, d, a GROUP BY 1, 2, 3)",
        settings=POSITIONAL,
    )

    # Projection output names ignore SELECT aliases and normalize `[b]` to `array(b)`.
    node.query("CREATE TABLE dl.t8 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t8 ADD PROJECTION pp (`array(b)` Array(UInt64) CODEC(ZSTD)) "
        "AS (SELECT [b] AS arr, a GROUP BY 2, 1)",
        settings=POSITIONAL,
    )

    # ReplicatedMergeTree analyzes CREATE definitions in the server context and replays metadata
    # ALTERs in a background context. Give both the positional setting while introducing t9.
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/users.d/positional.xml"), POSITIONAL_XML
    )
    node.restart_clickhouse()
    assert node.query(
        "SELECT value FROM system.settings "
        "WHERE name = 'enable_positional_arguments_for_projections'"
    ) == "1\n"

    # Keep an unavailable declaration on a replicated table, before an available one, so
    # metadata rewrites have to serialize both in their original order. Declare both at CREATE:
    # a later ReplicatedMergeTree ALTER_METADATA replay does not carry the positional setting,
    # so it cannot introduce this projection before it has become a known unavailable one.
    node.query(
        "CREATE TABLE dl.t9 (a UInt64, b UInt64, "
        "PROJECTION pp_unavailable (b CODEC(ZSTD)) AS (SELECT b, a GROUP BY 1, 2), "
        "PROJECTION qq_available (SELECT a ORDER BY a)) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/dl/t9', 'r1') ORDER BY a",
        settings={**POSITIONAL, "allow_projection_column_list_in_replicated_metadata": 1},
    )
    node.query(
        "CREATE TABLE dl.t9_peer (a UInt64, b UInt64, "
        "PROJECTION pp_unavailable (b CODEC(ZSTD)) AS (SELECT b, a GROUP BY 1, 2), "
        "PROJECTION qq_available (SELECT a ORDER BY a)) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/dl/t9', 'r2') ORDER BY a",
        settings={**POSITIONAL, "allow_projection_column_list_in_replicated_metadata": 1},
    )

    node.query("CREATE TABLE dl.t10 (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t10 ADD PROJECTION pp (b UInt64 CODEC(ZSTD)) "
        "AS (WITH b AS source_value, source_value AS x SELECT x, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )

    node.query("CREATE TABLE dl.t11 (a UInt64, b UInt64, x ALIAS b, c UInt64) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t11 ADD PROJECTION pp (b UInt64 CODEC(ZSTD)) "
        "AS (SELECT x, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )

    node.query("CREATE TABLE dl.t12 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t12 ADD PROJECTION pp (b UInt64 CODEC(ZSTD)) "
        "AS (SELECT COLUMNS('^b$'), a GROUP BY b, 2)",
        settings=POSITIONAL,
    )

    node.query("CREATE TABLE dl.t13 (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY c")
    node.query(
        "ALTER TABLE dl.t13 ADD PROJECTION pp "
        "(SELECT * EXCEPT (b) REPLACE (b AS a) GROUP BY 1, 2)",
        settings=POSITIONAL,
    )

    node.query("CREATE TABLE dl.t14 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t14 ADD PROJECTION pp (b UInt64 CODEC(ZSTD)) "
        "AS (SELECT plus(b AS x, 1) AS y, x, a GROUP BY 1, 2, 3)",
        settings=POSITIONAL,
    )

    node.query("CREATE TABLE dl.t15 (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY c")
    node.query(
        "ALTER TABLE dl.t15 ADD PROJECTION pp "
        "(SELECT * EXCEPT (b) APPLY (x -> x + b) GROUP BY 1, 2)",
        settings=POSITIONAL,
    )

    node.query("CREATE TABLE dl.t16 (a UInt64, b UInt64, x Tuple(y UInt64)) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t16 ADD PROJECTION pp "
        "(SELECT arrayMap(x -> x.1, [tuple(b)]) AS y, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )

    node.query("CREATE TABLE dl.t17 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t17 ADD PROJECTION pp "
        "(SELECT COLUMNS('^b$') GROUP BY 1)",
        settings=POSITIONAL,
    )

    node.query("CREATE TABLE dl.t18 (a UInt64, t Tuple(x UInt64, y UInt64)) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t18 ADD PROJECTION pp "
        "(SELECT a, t, t.x GROUP BY 1, 2, 3)",
        settings=POSITIONAL,
    )

    node.query("CREATE TABLE dl.t19 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t19 ADD PROJECTION pp (`identity(b)` UInt64 CODEC(ZSTD)) "
        "AS (SELECT COLUMNS('^b$') APPLY identity GROUP BY 1)",
        settings=POSITIONAL,
    )

    node.query("CREATE TABLE dl.t20 (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY c")
    node.query(
        "ALTER TABLE dl.t20 ADD PROJECTION pp "
        "(SELECT * EXCEPT STRICT (b) GROUP BY 1, 2)",
        settings=POSITIONAL,
    )

    node.query("CREATE TABLE dl.t21 (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY c")
    node.query(
        "ALTER TABLE dl.t21 ADD PROJECTION pp "
        "(SELECT * EXCEPT (a) REPLACE STRICT (a AS b) GROUP BY 1, 2)",
        settings=POSITIONAL,
    )

    node.query("CREATE TABLE dl.t22 (a UInt64, t Tuple(x UInt64, y UInt64)) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t22 ADD PROJECTION pp "
        "(SELECT t, tupleElement(t, 'x') GROUP BY 1, 2)",
        settings=POSITIONAL,
    )

    node.query("CREATE TABLE dl.t23 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t23 ADD PROJECTION pp (a UInt64 CODEC(ZSTD)) "
        "AS (SELECT a, COLUMNS('^b$') APPLY toString GROUP BY 1, 2)",
        settings=POSITIONAL,
    )

    node.query("CREATE TABLE dl.t24 (a UInt64, t Tuple(x UInt64, y UInt64)) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t24 ADD PROJECTION pp "
        "(SELECT t, tupleElement(t, 2) GROUP BY 1, 2)",
        settings=POSITIONAL,
    )

    # A nested COLUMNS matcher supplies the arguments to plus(). Losing or gaining one
    # matching column would change that call while the projection cannot be analyzed.
    node.query(
        "CREATE TABLE dl.t25 (a UInt64, b UInt64, c UInt64, d UInt64) "
        "ENGINE = MergeTree ORDER BY c"
    )
    node.query(
        "ALTER TABLE dl.t25 ADD PROJECTION pp (`plus(a, b)` UInt64 CODEC(ZSTD)) "
        "AS (SELECT plus(COLUMNS('^(a|b|b2)$')) AS x, c GROUP BY 1, 2)",
        settings=POSITIONAL,
    )

    # A direct matcher may shrink, but stored positional GROUP BY references must still
    # resolve to the same outputs.
    node.query("CREATE TABLE dl.t26 (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY c")
    node.query(
        "ALTER TABLE dl.t26 ADD PROJECTION pp "
        "(SELECT COLUMNS('^(a|b)$'), c GROUP BY 1, 2, 3)",
        settings=POSITIONAL,
    )
    node.query("CREATE TABLE dl.t27 (a UInt64, b UInt64, c UInt64, d UInt64) ENGINE = MergeTree ORDER BY d")
    node.query(
        "ALTER TABLE dl.t27 ADD PROJECTION pp "
        "(SELECT d, COLUMNS('^(a|b)$'), sum(c) GROUP BY 1, COLUMNS('^(a|b)$'))",
        settings=POSITIONAL,
    )

    # Nested `*` also supplies function arguments; `count(*)` is the exception because it
    # counts rows rather than expanding into the table's columns.
    node.query("CREATE TABLE dl.t28 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t28 ADD PROJECTION pp (`plus(a, b)` UInt64 CODEC(ZSTD)) "
        "AS (SELECT plus(*) AS x, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    node.query("CREATE TABLE dl.t29 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t29 ADD PROJECTION pp (`count()` AggregateFunction(count) CODEC(ZSTD)) "
        "AS (SELECT COUNT(*) AS x, a GROUP BY 2)",
        settings=POSITIONAL,
    )
    node.query(
        "ALTER TABLE dl.t29 ADD PROJECTION qq "
        "(SELECT countIf(COLUMNS('^b$'), a > 0) AS x, a GROUP BY 2)",
        settings=POSITIONAL,
    )
    node.query("CREATE TABLE dl.t30 (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY c")
    node.query(
        "ALTER TABLE dl.t30 ADD PROJECTION pp (`plus(a, b)` UInt64 CODEC(ZSTD)) "
        "AS (WITH plus(COLUMNS('^(a|b)$')) AS x SELECT x, c GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    node.query("CREATE TABLE dl.t31 (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY c")
    node.query(
        "ALTER TABLE dl.t31 ADD PROJECTION pp "
        "(SELECT COLUMNS('^(a|b)$'), sum(c) GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    # An unused WITH alias cannot affect the projection's SELECT output.
    node.query("CREATE TABLE dl.t32 (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY c")
    node.query(
        "ALTER TABLE dl.t32 ADD PROJECTION pp "
        "(WITH plus(COLUMNS('^(a|b|b2)$')) AS x SELECT c GROUP BY 1)",
        settings=POSITIONAL,
    )

    # A stored untyped codec must be checked against the new SELECT output type even while the
    # positional GROUP BY makes its projection unavailable.
    node.query("CREATE TABLE dl.t33 (a UInt64, b Float64) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t33 ADD PROJECTION pp (b CODEC(FPC)) "
        "AS (SELECT b, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )

    # b affects the filter, but not the FPC-encoded SELECT output.
    node.query("CREATE TABLE dl.t34 (a UInt64, b Float64) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t34 ADD PROJECTION pp (`toFloat64(a)` CODEC(FPC)) "
        "AS (SELECT toFloat64(a), a WHERE b > 0 GROUP BY 1, 2)",
        settings=POSITIONAL,
    )

    node.query("CREATE TABLE dl.t35 (a UInt64, b Float64) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t35 ADD PROJECTION pp (b CODEC(FPC)) "
        "AS (WITH b AS source_value, source_value AS x SELECT x, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )
    node.query("CREATE TABLE dl.t36 (a UInt64, b Float64, x ALIAS b) ENGINE = MergeTree ORDER BY a")
    node.query(
        "ALTER TABLE dl.t36 ADD PROJECTION pp (b CODEC(FPC)) "
        "AS (SELECT x, a GROUP BY 1, 2)",
        settings=POSITIONAL,
    )

    # A missing dictionary makes these sorting projections unavailable on restart. After the
    # dictionary is recreated, CREATE AS can analyze a temporary copy and check the destination's
    # requirements without making the published declarations available before another restart.
    node.query("CREATE TABLE dl.projection_lookup_source (id UInt64, value UInt64) ENGINE = Memory")
    dictionary_ddl = (
        "CREATE DICTIONARY dl.projection_lookup (id UInt64, value UInt64 DEFAULT 0) "
        "PRIMARY KEY id SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() "
        "DB 'dl' TABLE 'projection_lookup_source')) LAYOUT(FLAT()) LIFETIME(0)"
    )
    node.query(dictionary_ddl)
    node.query(
        "CREATE TABLE dl.t37 (a UInt64, "
        "PROJECTION pp (SELECT a, _part_offset, "
        "dictGet('dl.projection_lookup', 'value', a) AS d ORDER BY a)) "
        "ENGINE = MergeTree ORDER BY a SETTINGS allow_part_offset_column_in_projections = 1"
    )
    node.query(
        "CREATE TABLE dl.t38 (a UInt64, "
        "PROJECTION pp (SELECT a, _block_number, _block_offset, "
        "dictGet('dl.projection_lookup', 'value', a) AS d ORDER BY a)) "
        "ENGINE = MergeTree ORDER BY a "
        "SETTINGS allow_commit_order_projection = 1, "
        "enable_block_number_column = 1, enable_block_offset_column = 1"
    )
    node.query(
        "CREATE TABLE dl.t39 (a UInt64, "
        "PROJECTION pp (SELECT a, dictGet('dl.projection_lookup', 'value', a) AS d ORDER BY a) "
        "WITH SETTINGS (index_granularity = 1024)) ENGINE = MergeTree ORDER BY a"
    )

    # Armed: every declaration is analyzed and materialized.
    assert projections("t") == "1"
    assert projections("t2") == "2"
    assert projections("t3") == "1"
    assert active_projection_parts("t") == "1"
    assert active_projection_parts("t2") == "2"
    assert active_projection_parts("t3") == "1"
    assert projections("t4") == "2"
    assert active_projection_parts("t4") == "2"
    assert part_types("t4") == "Wide"
    assert projections("t5") == "2"
    assert active_projection_parts("t5") == "2"
    assert part_types("t5") == "Wide"
    assert projections("t6") == "1"
    assert active_projection_parts("t6") == "1"
    assert projections("t7") == "1"
    assert projections("t8") == "1"
    assert projections("t9") == "2"
    assert projections("t9_peer") == "2"
    assert projections("t10") == "1"
    assert projections("t11") == "1"
    assert projections("t12") == "1"
    assert projections("t13") == "1"
    assert projections("t14") == "1"
    assert projections("t15") == "1"
    assert projections("t16") == "1"
    assert projections("t17") == "1"
    assert projections("t18") == "1"
    assert projections("t19") == "1"
    assert projections("t20") == "1"
    assert projections("t21") == "1"
    assert projections("t22") == "1"
    assert projections("t23") == "1"
    assert projections("t24") == "1"
    assert projections("t25") == "1"
    assert projections("t26") == "1"
    assert projections("t27") == "1"
    assert projections("t28") == "1"
    assert projections("t29") == "2"
    assert projections("t30") == "1"
    assert projections("t31") == "1"
    assert projections("t32") == "1"
    assert projections("t33") == "1"
    assert projections("t34") == "1"
    assert projections("t35") == "1"
    assert projections("t36") == "1"
    assert projections("t37") == "1"
    assert projections("t38") == "1"
    assert projections("t39") == "1"
    assert "CODEC(Delta, Delta)" in node.query("SHOW CREATE TABLE dl.t6")

    node.exec_in_container(["rm", "-f", POSITIONAL_XML])
    node.query("DROP DICTIONARY dl.projection_lookup SETTINGS check_table_dependencies = 0")
    node.restart_clickhouse()

    # The skip fired, the server still started, and reads still work. This is also the in-range control
    # for the recovery assertions at the end.
    assert projections("t") == "0"
    assert projections("t2") == "0"
    assert projections("t3") == "0"
    assert projections("t4") == "1"  # only `qq` can be analyzed without the setting
    assert projections("t5") == "0"
    assert projections("t6") == "0"
    assert projections("t7") == "0"
    assert projections("t8") == "0"
    assert projections("t9") == "1"
    assert projections("t9_peer") == "1"
    assert projections("t10") == "0"
    assert projections("t11") == "0"
    assert projections("t12") == "0"
    assert projections("t13") == "0"
    assert projections("t14") == "0"
    assert projections("t15") == "0"
    assert projections("t16") == "0"
    assert projections("t17") == "0"
    assert projections("t18") == "0"
    assert projections("t19") == "0"
    assert projections("t20") == "0"
    assert projections("t21") == "0"
    assert projections("t22") == "0"
    assert projections("t23") == "0"
    assert projections("t24") == "0"
    assert projections("t25") == "0"
    assert projections("t26") == "0"
    assert projections("t27") == "0"
    assert projections("t28") == "0"
    assert projections("t29") == "0"
    assert projections("t30") == "0"
    assert projections("t31") == "0"
    assert projections("t32") == "0"
    assert projections("t33") == "0"
    assert projections("t34") == "0"
    assert projections("t35") == "0"
    assert projections("t36") == "0"
    assert projections("t37") == "0"
    assert projections("t38") == "0"
    assert projections("t39") == "0"
    assert node.query("SELECT count() FROM dl.t").strip() == "100"
    assert node.query("SELECT count() FROM dl.t2").strip() == "100"

    # These declarations are unavailable because the dictionary is missing. ALTER must still
    # protect the physical projection layout and the settings needed when they become available.
    for table, setting in (
        ("t37", "allow_part_offset_column_in_projections"),
        ("t38", "allow_commit_order_projection"),
        ("t38", "enable_block_number_column"),
        ("t38", "enable_block_offset_column"),
    ):
        error = node.query_and_get_error(
            f"ALTER TABLE dl.{table} MODIFY SETTING {setting} = 0"
        )
        assert setting in error and "unavailable projection" in error, error

    error = node.query_and_get_error(
        "ALTER TABLE dl.t38 RESET SETTING allow_commit_order_projection"
    )
    assert (
        "allow_commit_order_projection" in error
        and "unavailable projection" in error
    ), error

    for column in ("_part_offset", "_part_index", "_parent_part_offset"):
        error = node.query_and_get_error(
            f"ALTER TABLE dl.t37 ADD COLUMN {column} UInt64"
        )
        assert column in error and "unavailable projection" in error, error

    error = node.query_and_get_error(
        "ALTER TABLE dl.t39 MODIFY SETTING index_granularity_bytes = 0"
    )
    assert "adaptive granularity" in error and "unavailable projection" in error, error

    backup = f"unavailable_projection_{uuid.uuid4().hex}"
    node.query(f"BACKUP TABLE dl.t37 TO Disk('backups', '{backup}')")
    node.query(
        f"RESTORE TABLE dl.t37 AS dl.t37_restored FROM Disk('backups', '{backup}')"
    )
    assert projections("t37_restored") == "0"
    assert declarations_on_disk("t37_restored") == 1

    codec_backup = f"unavailable_projection_codec_{uuid.uuid4().hex}"
    node.query(f"BACKUP TABLE dl.t6 TO Disk('backups', '{codec_backup}')")
    error = node.query_and_get_error(
        f"RESTORE TABLE dl.t6 AS dl.t6_restored FROM Disk('backups', '{codec_backup}')"
    )
    assert "suspicious" in error.lower(), error
    assert node.query("EXISTS TABLE dl.t6_restored").strip() == "0"
    node.query(
        f"RESTORE TABLE dl.t6 AS dl.t6_restored FROM Disk('backups', '{codec_backup}')",
        settings={"allow_suspicious_codecs": 1},
    )
    assert projections("t6_restored") == "0"
    assert declarations_on_disk("t6_restored") == 1

    # `CREATE TABLE ... AS` copies unavailable projection declarations too. Reject them before
    # publishing the copied column list to replicated metadata or a format-1 DDL queue.
    error = node.query_and_get_error(
        "CREATE TABLE dl.t6_replicated_copy AS dl.t6 "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t6_replicated_copy', 'r1') "
        "ORDER BY a"
    )
    assert "Projection column lists in replicated metadata require setting" in error

    old_format = {
        "distributed_ddl_entry_format_version": 1,
        "distributed_ddl_task_timeout": 60,
        "distributed_ddl_output_mode": "throw",
    }
    error = node.query_and_get_error(
        "CREATE TABLE dl.t6_cluster_list_copy ON CLUSTER test_shard_localhost AS dl.t6 "
        "ENGINE = MergeTree ORDER BY a",
        settings=old_format,
    )
    assert "Projection column lists in replicated metadata require setting" in error

    error = node.query_and_get_error(
        "CREATE TABLE dl.t6_cluster_copy ON CLUSTER test_shard_localhost AS dl.t6 "
        "ENGINE = MergeTree ORDER BY a",
        settings={
            **old_format,
            # Format 1 does not carry settings to the worker; format 2 does, but still expands
            # `AS dl.t6` there, where the unavailable declaration must be rejected.
            "distributed_ddl_entry_format_version": 2,
            "allow_projection_column_list_in_replicated_metadata": 1,
            "allow_suspicious_codecs": 1,
        },
    )
    assert "Cannot copy unavailable projection declarations" in error
    error = node.query_and_get_error(
        "CREATE TABLE dl.t6_cluster_copy_v3 ON CLUSTER test_shard_localhost AS dl.t6 "
        "ENGINE = MergeTree ORDER BY a",
        settings={
            "distributed_ddl_entry_format_version": 3,
            "allow_projection_column_list_in_replicated_metadata": 1,
        },
    )
    assert "Cannot copy unavailable projection declarations" in error
    assert node.query(
        "SELECT count() FROM system.tables WHERE database = 'dl' "
        "AND name IN ('t6_replicated_copy', 't6_cluster_list_copy', "
        "'t6_cluster_copy', 't6_cluster_copy_v3')"
    ).strip() == "0"

    # Storage construction sees only analyzed projections. A copied unavailable declaration
    # must still count for CREATE-time restrictions before the destination is published.
    error = node.query_and_get_error(
        "CREATE TABLE dl.t6_unique_copy AS dl.t6 "
        "ENGINE = MergeTree ORDER BY a UNIQUE KEY (a)",
        settings={"enable_unique_key": 1},
    )
    assert "Projections are not supported on tables with UNIQUE KEY" in error
    error = node.query_and_get_error(
        "CREATE TABLE dl.t9_limit_copy AS dl.t9 "
        "ENGINE = MergeTree ORDER BY a SETTINGS max_projections = 1"
    )
    assert "Maximum limit of 1 projection(s) exceeded" in error
    error = node.query_and_get_error(
        f"ATTACH TABLE dl.t6_unique_attach UUID '{uuid.uuid4()}' "
        "ENGINE = MergeTree ORDER BY a UNIQUE KEY (a) AS dl.t6",
        settings={"enable_unique_key": 1},
    )
    assert "Projections are not supported on tables with UNIQUE KEY" in error
    error = node.query_and_get_error(
        f"ATTACH TABLE dl.t9_limit_attach UUID '{uuid.uuid4()}' "
        "ENGINE = MergeTree ORDER BY a SETTINGS max_projections = 1 AS dl.t9"
    )
    assert "Maximum limit of 1 projection(s) exceeded" in error
    assert node.query(
        "SELECT count() FROM system.tables WHERE database = 'dl' "
        "AND name IN ('t6_unique_copy', 't9_limit_copy', "
        "'t6_unique_attach', 't9_limit_attach')"
    ).strip() == "0"

    error = node.query_and_get_error(
        "CREATE TABLE dl.t37_missing_dictionary_copy AS dl.t37 ENGINE = MergeTree ORDER BY a "
        "SETTINGS allow_part_offset_column_in_projections = 0"
    )
    assert "Cannot copy unavailable projection `pp` without validating its destination requirements" in error
    assert "Dictionary" in error
    node.query(dictionary_ddl)
    assert projections("t37") == "0"

    for destination, source, settings, expected in (
        (
            "t37_rejected_copy",
            "t37",
            "allow_part_offset_column_in_projections = 0",
            "allow_part_offset_column_in_projections",
        ),
        (
            "t38_rejected_copy",
            "t38",
            "allow_commit_order_projection = 0, "
            "enable_block_number_column = 1, enable_block_offset_column = 1",
            "allow_commit_order_projection",
        ),
        (
            "t38_missing_block_number_copy",
            "t38",
            "allow_commit_order_projection = 1, "
            "enable_block_number_column = 0, enable_block_offset_column = 1",
            "enable_block_number_column",
        ),
        (
            "t38_missing_block_offset_copy",
            "t38",
            "allow_commit_order_projection = 1, "
            "enable_block_number_column = 1, enable_block_offset_column = 0",
            "enable_block_offset_column",
        ),
        (
            "t39_rejected_copy",
            "t39",
            "index_granularity_bytes = 0",
            "parent table uses fixed granularity",
        ),
    ):
        error = node.query_and_get_error(
            f"CREATE TABLE dl.{destination} AS dl.{source} "
            f"ENGINE = MergeTree ORDER BY a SETTINGS {settings}"
        )
        assert expected in error, error
        assert node.query(
            "SELECT count() FROM system.tables WHERE database = 'dl' "
            f"AND name = '{destination}'"
        ).strip() == "0"

    # The same declarations remain copyable when the destination can support them.
    for destination, source, settings in (
        ("t37_allowed_copy", "t37", "allow_part_offset_column_in_projections = 1"),
        (
            "t38_allowed_copy",
            "t38",
            "allow_commit_order_projection = 1, "
            "enable_block_number_column = 1, enable_block_offset_column = 1",
        ),
        ("t39_allowed_copy", "t39", "index_granularity_bytes = 10485760"),
    ):
        node.query(
            f"CREATE TABLE dl.{destination} AS dl.{source} "
            f"ENGINE = MergeTree ORDER BY a SETTINGS {settings}"
        )
        assert projections(destination) == "0"
        assert declarations_on_disk(destination) == 1

    node.query(
        f"ATTACH TABLE dl.t6_valid_attach UUID '{uuid.uuid4()}' "
        "ENGINE = MergeTree ORDER BY a SETTINGS max_projections = 1 AS dl.t6"
    )
    assert projections("t6_valid_attach") == "0"
    assert declarations_on_disk("t6_valid_attach") == 1

    # A local `CREATE AS` must retain the declaration even though this server cannot analyze it
    # until the projection setting is restored. It is absent from `system.projections` meanwhile.
    node.query("CREATE TABLE dl.t6_local_copy AS dl.t6 ENGINE = MergeTree ORDER BY a")
    assert projections("t6_local_copy") == "0"
    assert declarations_on_disk("t6_local_copy") == 1
    assert "CODEC(Delta, Delta)" in node.query("SHOW CREATE TABLE dl.t6_local_copy")

    # The source declaration was admitted with the codec opt-in before it became unavailable.
    # The same fresh declaration is rejected by this session's codec policy, while the accepted
    # source copy above retains its declaration for reanalysis at startup.
    error = node.query_and_get_error(
        "CREATE TABLE dl.t6_fresh_rejected "
        "(a UInt64, b UInt64, PROJECTION pp (b CODEC(Delta, Delta)) "
        "AS (SELECT b, a GROUP BY 1, 2)) ENGINE = MergeTree ORDER BY a",
        settings=POSITIONAL,
    )
    assert "suspicious" in error.lower(), error
    assert node.query("EXISTS TABLE dl.t6_fresh_rejected").strip() == "0"

    # Each clickhouse-client call has its own temporary-table session. Compare both creation
    # paths in one session, using the source whose projection became unavailable at startup.
    temporary_creates = node.exec_in_container(
        [
            "clickhouse",
            "client",
            "--multiquery",
            "--format=TabSeparated",
            "--query="
            "CREATE TEMPORARY TABLE tmp_plain AS dl.t6 ENGINE = MergeTree ORDER BY a; "
            "SHOW CREATE TABLE tmp_plain; "
            "CREATE TEMPORARY TABLE tmp_replaced (a UInt64) ENGINE = Memory; "
            "CREATE OR REPLACE TEMPORARY TABLE tmp_replaced AS dl.t6 "
            "ENGINE = MergeTree ORDER BY a; "
            "SHOW CREATE TABLE tmp_replaced;",
        ]
    ).splitlines()
    assert len(temporary_creates) == 2, temporary_creates
    for definition in temporary_creates:
        assert "PROJECTION pp" in definition, definition
        assert "CODEC(Delta, Delta)" in definition, definition
        assert re.search(r"GROUP BY\s+1,\s+2\b", definition.replace("\\n", " ")), definition
    assert temporary_creates[0].replace("tmp_plain", "tmp_replaced") == temporary_creates[1]

    node.query(
        "CREATE TABLE dl.t6_limit_copy AS dl.t6 "
        "ENGINE = MergeTree ORDER BY a SETTINGS max_projections = 1"
    )
    assert projections("t6_limit_copy") == "0"
    assert declarations_on_disk("t6_limit_copy") == 1
    node.query("CREATE TABLE dl.t9_local_copy AS dl.t9 ENGINE = MergeTree ORDER BY a")
    assert projections("t9_local_copy") == "1"
    assert declarations_on_disk("t9_local_copy") == 2
    t9_local_copy_create = node.query("SHOW CREATE TABLE dl.t9_local_copy")
    assert t9_local_copy_create.index("pp_unavailable") < t9_local_copy_create.index("qq_available")

    # Metadata-only changes leave the source schema intact and must remain usable while a
    # projection declaration is unavailable.
    node.query("ALTER TABLE dl.t MODIFY COMMENT 'x'")
    assert node.query(
        "SELECT comment FROM system.tables WHERE database = 'dl' AND name = 't'"
    ).strip() == "x"
    node.query("ALTER TABLE dl.t6 MODIFY COLUMN b COMMENT 'safe'")
    node.query("ALTER TABLE dl.t6 MODIFY COLUMN b CODEC(ZSTD)")
    assert node.query(
        "SELECT comment FROM system.columns WHERE database = 'dl' AND table = 't6' AND name = 'b'"
    ).strip() == "safe"
    assert "CODEC(ZSTD" in node.query("SHOW CREATE TABLE dl.t6")
    assert "PROJECTION" in node.query("SHOW CREATE TABLE dl.t")
    assert declarations_on_disk("t") == 1

    # The type change rewrites the part and drops the unavailable projection data. The accepted
    # codec declaration survives, ready to be analyzed again with the new type after restart.
    node.query("ALTER TABLE dl.t6 MODIFY COLUMN b UInt32", settings={"mutations_sync": 2})
    assert node.query(
        "SELECT type FROM system.columns WHERE database = 'dl' AND table = 't6' AND name = 'b'"
    ).strip() == "UInt32"
    assert projection_dirs_in_active_parts("t6") == []
    assert declarations_on_disk("t6") == 1

    for statement, reason in (
        ("MODIFY COLUMN b UInt32", "declares an explicit column type"),
        ("DROP COLUMN b", "references it"),
        ("RENAME COLUMN b TO bb", "references it"),
    ):
        error = node.query_and_get_error(f"ALTER TABLE dl.t7 {statement}")
        assert "projection `pp`" in error and reason in error

    # A type change in an unpinned output and a change to an unused column remain legal.
    node.query("ALTER TABLE dl.t7 MODIFY COLUMN d UInt32")
    node.query("ALTER TABLE dl.t7 DROP COLUMN c")
    assert declarations_on_disk("t7") == 1

    error = node.query_and_get_error("ALTER TABLE dl.t33 MODIFY COLUMN b UInt64")
    assert "projection `pp`" in error and "incompatible codec" in error
    node.query("ALTER TABLE dl.t33 MODIFY COLUMN b Float32")
    assert declarations_on_disk("t33") == 1
    node.query("ALTER TABLE dl.t34 MODIFY COLUMN b UInt64")
    assert declarations_on_disk("t34") == 1
    error = node.query_and_get_error("ALTER TABLE dl.t35 MODIFY COLUMN b UInt64")
    assert "projection `pp`" in error and "incompatible codec" in error
    node.query("ALTER TABLE dl.t35 MODIFY COLUMN b Float32")
    # The table ALIAS keeps its Float64 type, but its SELECT output can change name when b changes.
    # The stored declaration would then fail to find output b after restart.
    error = node.query_and_get_error("ALTER TABLE dl.t36 MODIFY COLUMN b UInt64")
    assert "projection `pp`" in error and "table ALIAS output" in error
    assert node.query(
        "SELECT type FROM system.columns WHERE database = 'dl' AND table = 't36' AND name = 'x'"
    ).strip() == "Float64"

    error = node.query_and_get_error("ALTER TABLE dl.t8 MODIFY COLUMN b UInt32")
    assert "projection `pp`" in error and "declares an explicit column type" in error
    error = node.query_and_get_error("ALTER TABLE dl.t10 MODIFY COLUMN b UInt32")
    assert "projection `pp`" in error and "declares an explicit column type" in error
    node.query("ALTER TABLE dl.t10 MODIFY COLUMN c UInt32")
    assert declarations_on_disk("t10") == 1

    # A table `ALIAS` is expanded to its source name in the projection output.
    error = node.query_and_get_error("ALTER TABLE dl.t11 MODIFY COLUMN b UInt32")
    assert "projection `pp`" in error and "declares an explicit column type" in error
    node.query("ALTER TABLE dl.t11 MODIFY COLUMN c UInt32")
    assert declarations_on_disk("t11") == 1

    error = node.query_and_get_error("ALTER TABLE dl.t12 MODIFY COLUMN b UInt32")
    assert "projection `pp`" in error and "declares an explicit column type" in error
    error = node.query_and_get_error("ALTER TABLE dl.t13 DROP COLUMN b")
    assert "projection `pp`" in error and "references it" in error
    assert declarations_on_disk("t12") == 1
    assert declarations_on_disk("t13") == 1
    error = node.query_and_get_error("ALTER TABLE dl.t14 MODIFY COLUMN b UInt32")
    assert "projection `pp`" in error and "declares an explicit column type" in error
    error = node.query_and_get_error("ALTER TABLE dl.t15 DROP COLUMN b")
    assert "projection `pp`" in error and "references it" in error
    node.query("ALTER TABLE dl.t16 DROP COLUMN x")
    error = node.query_and_get_error("ALTER TABLE dl.t17 DROP COLUMN b")
    assert "projection `pp`" in error and "references it" in error
    error = node.query_and_get_error("ALTER TABLE dl.t18 MODIFY COLUMN t Tuple(y UInt64)")
    assert "projection `pp`" in error and "field that would no longer exist" in error
    error = node.query_and_get_error("ALTER TABLE dl.t19 MODIFY COLUMN b UInt32")
    assert "projection `pp`" in error and "declares an explicit column type" in error
    error = node.query_and_get_error("ALTER TABLE dl.t20 DROP COLUMN b")
    assert "projection `pp`" in error and "references it" in error
    error = node.query_and_get_error("ALTER TABLE dl.t21 DROP COLUMN b")
    assert "projection `pp`" in error and "references it" in error
    error = node.query_and_get_error("ALTER TABLE dl.t22 MODIFY COLUMN t Tuple(y UInt64)")
    assert "projection `pp`" in error and "field that would no longer exist" in error
    node.query("ALTER TABLE dl.t23 MODIFY COLUMN b UInt32")
    error = node.query_and_get_error("ALTER TABLE dl.t24 MODIFY COLUMN t Tuple(y UInt64)")
    assert "projection `pp`" in error and "field that would no longer exist" in error
    for statement in ("DROP COLUMN b", "ADD COLUMN b2 UInt64"):
        error = node.query_and_get_error(f"ALTER TABLE dl.t25 {statement}")
        assert "projection `pp`" in error and "nested matcher" in error
    error = node.query_and_get_error("ALTER TABLE dl.t25 MODIFY COLUMN b Int64")
    assert "projection `pp`" in error and "declares an explicit column type" in error
    node.query("ALTER TABLE dl.t25 MODIFY COLUMN d UInt32")
    error = node.query_and_get_error("ALTER TABLE dl.t26 DROP COLUMN b")
    assert "projection `pp`" in error and "positional GROUP BY" in error
    node.query("ALTER TABLE dl.t27 DROP COLUMN b")
    error = node.query_and_get_error("ALTER TABLE dl.t28 DROP COLUMN b")
    assert "projection `pp`" in error and "nested matcher" in error
    node.query("ALTER TABLE dl.t29 ADD COLUMN c UInt64")
    node.query("ALTER TABLE dl.t29 DROP COLUMN b")
    error = node.query_and_get_error("ALTER TABLE dl.t30 MODIFY COLUMN b Int64")
    assert "projection `pp`" in error and "declares an explicit column type" in error
    error = node.query_and_get_error("ALTER TABLE dl.t31 DROP COLUMN b")
    assert "projection `pp`" in error and "positional GROUP BY" in error
    node.query("ALTER TABLE dl.t32 ADD COLUMN b2 UInt64")
    node.query("ALTER TABLE dl.t32 DROP COLUMN b2")
    node.query("ALTER TABLE dl.t32 DROP COLUMN b")
    node.query("ALTER TABLE dl.t32 DROP COLUMN a")
    assert declarations_on_disk("t14") == 1
    assert declarations_on_disk("t15") == 1
    assert declarations_on_disk("t16") == 1
    assert declarations_on_disk("t17") == 1
    assert declarations_on_disk("t18") == 1
    assert declarations_on_disk("t19") == 1
    assert declarations_on_disk("t20") == 1
    assert declarations_on_disk("t21") == 1
    assert declarations_on_disk("t22") == 1
    assert declarations_on_disk("t23") == 1
    assert declarations_on_disk("t24") == 1
    assert declarations_on_disk("t25") == 1
    assert declarations_on_disk("t26") == 1
    assert declarations_on_disk("t27") == 1
    assert declarations_on_disk("t28") == 1
    assert declarations_on_disk("t29") == 2
    assert declarations_on_disk("t30") == 1
    assert declarations_on_disk("t31") == 1
    assert declarations_on_disk("t32") == 1

    zk_path = node.query(
        "SELECT zookeeper_path FROM system.replicas WHERE database = 'dl' AND table = 't9'"
    ).strip()
    node.query("ALTER TABLE dl.t9 MODIFY COMMENT 'safe'")
    zk_metadata = node.query(
        f"SELECT value FROM system.zookeeper WHERE path = '{zk_path}' AND name = 'metadata'"
    )
    assert "projections: pp_unavailable" in zk_metadata
    assert zk_metadata.index("pp_unavailable") < zk_metadata.index("qq_available")
    assert "CODEC(ZSTD(1))" in zk_metadata
    assert declarations_on_disk("t9") == 2
    t9_create = node.query("SHOW CREATE TABLE dl.t9")
    assert t9_create.index("pp_unavailable") < t9_create.index("qq_available")
    node.query("ALTER TABLE dl.t9 ADD COLUMN c UInt64 DEFAULT 0")
    node.query("SYSTEM SYNC REPLICA dl.t9_peer")
    assert declarations_on_disk("t9_peer") == 2
    assert projections("t9_peer") == "1"
    t9_peer_create = node.query("SHOW CREATE TABLE dl.t9_peer")
    assert t9_peer_create.index("pp_unavailable") < t9_peer_create.index("qq_available")
    node.query("ALTER TABLE dl.t9 ADD PROJECTION rr_available (SELECT b ORDER BY b)")
    node.query("SYSTEM SYNC REPLICA dl.t9_peer")
    assert declarations_on_disk("t9_peer") == 3
    assert projections("t9_peer") == "2"
    t9_peer_create = node.query("SHOW CREATE TABLE dl.t9_peer")
    assert t9_peer_create.index("pp_unavailable") < t9_peer_create.index("qq_available") < t9_peer_create.index("rr_available")

    # A mutation is not a metadata `ALTER`, so it is not refused. Nothing knows whether `pp`'s
    # materialized data still matches the rows it rewrites, so that data must be left out of the new
    # part rather than hardlinked into it and served as current after recovery.
    some_before, all_before = event_value("MutationSomePartColumns"), event_value(
        "MutationAllPartColumns"
    )
    node.query(
        "ALTER TABLE dl.t3 UPDATE b = 'z' WHERE a < 10", settings={"mutations_sync": 2}
    )
    assert event_value("MutationSomePartColumns") == some_before + 1
    assert event_value("MutationAllPartColumns") == all_before
    assert part_types("t3") == "Wide"

    # `DROP PROJECTION` is exempt, and dropping one projection rewrites no rows, so `pp`'s data must be
    # carried into the new part with its checksum entry intact: an entry whose directory is missing
    # registers a broken projection part, and one broken projection silences the whole data part's
    # consistency check on every later load.
    some_before, all_before = event_value("MutationSomePartColumns"), event_value(
        "MutationAllPartColumns"
    )
    node.query("ALTER TABLE dl.t4 DROP PROJECTION qq", settings={"mutations_sync": 2})
    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.mutations WHERE database = 'dl' AND table = 't4' AND NOT is_done",
        "0",
    )
    assert event_value("MutationSomePartColumns") == some_before + 1
    assert event_value("MutationAllPartColumns") == all_before
    assert declarations_on_disk("t4") == 1
    assert projections("t4") == "0"

    node.query("ALTER TABLE dl.t2 ADD PROJECTION rr (SELECT a GROUP BY a)")
    assert projections("t2") == "1"
    assert declarations_on_disk("t2") == 3
    node.query("ALTER TABLE dl.t2 DROP PROJECTION rr")
    assert declarations_on_disk("t2") == 2

    # Re-adding the same name hits the guard in `ProjectionsDescription::add`: on master this silently
    # replaced the declaration still on disk.
    error = node.query_and_get_error(
        "ALTER TABLE dl.t ADD PROJECTION pp (SELECT a GROUP BY a)"
    )
    assert "a projection with this name is declared but could not be analyzed" in error

    # `IF NOT EXISTS` is a no-op for an unavailable declaration just as it is for an analyzed one.
    # The replacement is deliberately invalid: neither validation nor application may inspect it.
    node.query(
        "ALTER TABLE dl.t ADD PROJECTION IF NOT EXISTS pp "
        "(missing UInt64 CODEC(NONE)) AS (SELECT missing)"
    )
    assert declarations_on_disk("t") == 1

    # The hypothetical form mirrors ADD PROJECTION and must short-circuit on the same reserved name.
    node.query(
        "CREATE HYPOTHETICAL PROJECTION IF NOT EXISTS pp ON dl.t "
        "(missing UInt64 CODEC(NONE)) AS (SELECT missing)"
    )
    assert (
        node.query(
            "SELECT count() FROM system.hypothetical_projections "
            "WHERE database = 'dl' AND table = 't' AND name = 'pp'"
        ).strip()
        == "0"
    )

    # `CLEAR PROJECTION` carries the same command type as `DROP PROJECTION` and is exempt on purpose:
    # `AlterCommands::apply` deliberately keeps the declaration when `clear` is set, so it changes
    # projection data and no metadata that the unanalyzable declaration could be validated against.
    # Both of `t5`'s declarations are still preserved, so one measurement covers both obligations of a
    # mutation that runs no writer: the named projection's data goes, the other one's is carried
    # forward.
    some_before, all_before = event_value("MutationSomePartColumns"), event_value(
        "MutationAllPartColumns"
    )
    assert projection_dirs_in_active_parts("t5") == ["pp.proj", "qq.proj"]
    node.query("ALTER TABLE dl.t5 CLEAR PROJECTION qq", settings={"mutations_sync": 2})
    # A Wide part with no writer takes the hardlink path, which is the arm that has to carry
    # `pp.proj` forward; a full rewrite would drop both directories for an unrelated reason, so the
    # counters pin which arm ran.
    assert event_value("MutationSomePartColumns") == some_before + 1
    assert event_value("MutationAllPartColumns") == all_before
    assert "PROJECTION qq" in node.query("SHOW CREATE TABLE dl.t5")
    assert projection_dirs_in_active_parts("t5") == ["pp.proj"]

    # Dropping is the way out, and it works one declaration at a time: the one that was not dropped is
    # still declared in the statement this ALTER rewrote.
    node.query("ALTER TABLE dl.t2 DROP PROJECTION pp")
    assert "PROJECTION qq" in node.query("SHOW CREATE TABLE dl.t2")
    assert declarations_on_disk("t2") == 1

    # The same exemption on a Compact part, whose mutation rewrites every column instead.
    node.query("ALTER TABLE dl.t2 CLEAR PROJECTION qq")
    assert "PROJECTION qq" in node.query("SHOW CREATE TABLE dl.t2")

    node.query("ALTER TABLE dl.t2 DROP PROJECTION qq")
    assert "PROJECTION" not in node.query("SHOW CREATE TABLE dl.t2")
    node.query("ALTER TABLE dl.t2 MODIFY COMMENT 'y'")

    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/users.d/positional.xml"), POSITIONAL_XML
    )
    node.restart_clickhouse()

    # `t` was never altered by the user, so its declaration is still there to be analyzed once the
    # setting is back, and the projection data materialized before the restart is used as it is.
    assert projections("t") == "1"
    assert active_projection_parts("t") == "1"
    assert projections("t6") == "1"
    assert projections("t6_restored") == "1"
    assert projections("t7") == "1"
    assert projections("t8") == "1"
    assert projections("t9") == "3"
    assert projections("t9_peer") == "3"
    assert projections("t9_local_copy") == "2"
    assert projections("t10") == "1"
    assert projections("t11") == "1"
    assert projections("t12") == "1"
    assert projections("t13") == "1"
    assert projections("t14") == "1"
    assert projections("t15") == "1"
    assert projections("t16") == "1"
    assert projections("t17") == "1"
    assert projections("t18") == "1"
    assert projections("t19") == "1"
    assert projections("t20") == "1"
    assert projections("t21") == "1"
    assert projections("t22") == "1"
    assert projections("t23") == "1"
    assert projections("t24") == "1"
    assert projections("t25") == "1"
    assert projections("t26") == "1"
    assert projections("t27") == "1"
    assert projections("t28") == "1"
    assert projections("t29") == "2"
    assert projections("t30") == "1"
    assert projections("t31") == "1"
    assert projections("t32") == "1"
    assert projections("t33") == "1"
    assert projections("t34") == "1"
    assert projections("t35") == "1"
    assert projections("t36") == "1"
    for table in (
        "t37",
        "t37_restored",
        "t38",
        "t39",
        "t37_allowed_copy",
        "t38_allowed_copy",
        "t39_allowed_copy",
    ):
        assert projections(table) == "1"
    assert active_projection_parts("t6") == "0"
    assert projections("t6_local_copy") == "1"

    # The declaration coming back is only half the claim: the projection data written before the
    # restart must be readable. `force_optimize_projection_name` fails the query if `pp` is not used.
    assert (
        node.query(
            "SELECT sum(a) FROM (SELECT b, a FROM dl.t GROUP BY b, a)",
            settings={"force_optimize_projection_name": "pp"},
        ).strip()
        == "4950"
    )

    # `t2` has no projections, because the user dropped those declarations.
    assert projections("t2") == "0"

    # `t3`'s declaration survived, but the mutation rewrote the part that held the projection data, so
    # the projection comes back with nothing materialized instead of with pre-mutation rows.
    assert projections("t3") == "1"
    assert active_projection_parts("t3") == "0"
    assert broken_projection_parts("t3") == "0"
    assert check_table("t3") == "1"
    assert node.query("SELECT countIf(b = 'z') FROM dl.t3").strip() == "10"
    error = node.query_and_get_error(
        "SELECT countIf(b = 'z') FROM (SELECT b, a FROM dl.t3 GROUP BY b, a)",
        settings={"force_optimize_projection_name": "pp"},
    )
    assert "not used" in error

    # `t4`'s mutation rewrote no rows, so `pp` comes back with the data it already had, and the part it
    # lives in is still consistent.
    assert projections("t4") == "1"
    assert active_projection_parts("t4") == "1"
    assert broken_projection_parts("t4") == "0"
    assert check_table("t4") == "1"
    assert (
        node.query(
            "SELECT sum(a) FROM (SELECT b, a FROM dl.t4 GROUP BY b, a)",
            settings={"force_optimize_projection_name": "pp"},
        ).strip()
        == "4950"
    )


def test_unavailable_projection_order_by_matcher_keeps_sorting_key(started_cluster):
    node.query("DROP DATABASE IF EXISTS order_matcher SYNC")
    node.query("CREATE DATABASE order_matcher")
    node.query("CREATE TABLE order_matcher.src (id UInt64, value UInt64) ENGINE = Memory")
    dictionary_ddl = (
        "CREATE DICTIONARY order_matcher.lookup (id UInt64, value UInt64 DEFAULT 0) "
        "PRIMARY KEY id SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() "
        "DB 'order_matcher' TABLE 'src')) LAYOUT(FLAT()) LIFETIME(0)"
    )
    node.query(dictionary_ddl)

    for table in ("regex_guard", "regex_safe"):
        node.query(
            f"CREATE TABLE order_matcher.{table} "
            "(a UInt64, b UInt64, c UInt64, "
            "PROJECTION p (SELECT *, dictGet('order_matcher.lookup', 'value', c) AS d "
            "ORDER BY COLUMNS('^(a|b)$'))) ENGINE = MergeTree ORDER BY tuple()"
        )
        node.query(f"INSERT INTO order_matcher.{table} VALUES (1, 2, 3)")

    node.query(
        "CREATE TABLE order_matcher.star_guard "
        "(a UInt64, b UInt64, c UInt64, "
        "PROJECTION p (SELECT *, dictGet('order_matcher.lookup', 'value', a) AS d "
        "ORDER BY *)) ENGINE = MergeTree ORDER BY tuple()"
    )
    node.query("INSERT INTO order_matcher.star_guard VALUES (1, 2, 3)")

    def sorting_key(table):
        return node.query(
            "SELECT sorting_key FROM system.projections "
            f"WHERE database = 'order_matcher' AND table = '{table}' AND name = 'p'"
        ).strip()

    assert sorting_key("regex_guard") == "['b']"
    assert sorting_key("regex_safe") == "['b']"
    assert sorting_key("star_guard") == "['c']"

    # Removing a dictionary with dependency checks disabled simulates an upgrade that can no
    # longer analyze the projection. The table and its stored declaration still load.
    node.query("DROP DICTIONARY order_matcher.lookup SETTINGS check_table_dependencies = 0")
    node.restart_clickhouse()
    for table in ("regex_guard", "regex_safe", "star_guard"):
        assert sorting_key(table) == ""
        assert "PROJECTION p" in node.query(f"SHOW CREATE TABLE order_matcher.{table}")

    error = node.query_and_get_error("ALTER TABLE order_matcher.regex_guard DROP COLUMN b")
    assert "ORDER BY matcher" in error and "different sorting key" in error
    error = node.query_and_get_error("ALTER TABLE order_matcher.star_guard DROP COLUMN c")
    assert "ORDER BY matcher" in error and "different sorting key" in error

    # Losing a non-key match leaves the last expanded ORDER BY expression unchanged.
    node.query(
        "ALTER TABLE order_matcher.regex_safe DROP COLUMN a",
        settings={"mutations_sync": 2},
    )
    assert "PROJECTION p" in node.query("SHOW CREATE TABLE order_matcher.regex_safe")

    node.query(dictionary_ddl)
    node.restart_clickhouse()
    assert sorting_key("regex_guard") == "['b']"
    assert sorting_key("regex_safe") == "['b']"
    assert sorting_key("star_guard") == "['c']"
    for table in ("regex_guard", "star_guard"):
        assert node.query(
            "SELECT count() FROM system.projection_parts "
            f"WHERE database = 'order_matcher' AND table = '{table}' AND active"
        ).strip() == "1"
