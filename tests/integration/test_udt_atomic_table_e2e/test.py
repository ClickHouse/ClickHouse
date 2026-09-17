"""table admission and physicalization real-server product journey for UDT-backed Atomic tables."""

import base64
import hashlib
import json
import os
import shlex
import threading
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["configs/udt.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)

ENABLED = {"allow_experimental_user_defined_types": 1}
DISABLED = {"allow_experimental_user_defined_types": 0}
CONFIG = "/etc/clickhouse-server/users.d/udt.xml"
SETTING_ON = "<allow_experimental_user_defined_types>1</allow_experimental_user_defined_types>"
SETTING_OFF = "<allow_experimental_user_defined_types>0</allow_experimental_user_defined_types>"
ALTER_PUBLICATION_FAILPOINT = (
    "udt_table_alter_pause_before_metadata_publication"
)
ALTER_PREPARED_FAILPOINT = (
    "udt_table_alter_pause_before_authority_publication"
)
TYPE_MUTATION_LOOKUP_FAILPOINT = "udt_lifecycle_pause_after_database_lookup"
MANIFEST_HASH_DOMAIN = b"ClickHouse UDT physicalization loss manifest V1"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def restart_server(started_cluster):
    # Sanitizers may spend most of the default minute draining and symbolizing system logs.
    wait_seconds = 300 if node.is_built_with_sanitizer() else 60

    def restart():
        node.restart_clickhouse(stop_start_wait_sec=wait_seconds)

    return restart


def q(sql, *, user="default", settings=ENABLED):
    return node.query(sql, user=user, settings=settings)


def error(sql, *, user="default", settings=ENABLED):
    result = node.query_and_get_error(sql, user=user, settings=settings)
    assert result, sql
    return result


def rows_json(sql, *, user="default", settings=ENABLED):
    output = q(f"{sql} FORMAT JSONEachRow", user=user, settings=settings)
    return [json.loads(line) for line in output.split("\n") if line]


def native_sha256(sql):
    command = (
        "/usr/bin/clickhouse client --query "
        + shlex.quote(f"{sql} FORMAT Native")
        + " | sha256sum | cut -d' ' -f1"
    )
    return node.exec_in_container(
        ["bash", "-o", "pipefail", "-c", command]
    ).strip()


def sql_string(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def encode_var_uint(value):
    result = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        result.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(result)


def database_metadata_snapshot(database):
    disk_root = q("SELECT path FROM system.disks WHERE name = 'default'").strip()
    assert os.path.isabs(disk_root), disk_root
    metadata_path = q(
        "SELECT metadata_path FROM system.databases "
        f"WHERE name = '{database}'"
    ).strip()
    if not os.path.isabs(metadata_path):
        metadata_path = os.path.join(disk_root, metadata_path)
    metadata_path = os.path.normpath(metadata_path)
    assert os.path.commonpath([os.path.normpath(disk_root), metadata_path]) == (
        os.path.normpath(disk_root)
    )

    # Atomic reports a shared data_path for the whole store, while its
    # metadata_path is the database-specific durable authority root. Snapshot
    # both names and file contents below that root, including empty directories.
    # The verification cursor is bounded scheduler progress, not authority
    # truth. It can advance asynchronously while a rejected DDL is being
    # observed, so exclude both its installed and atomic temporary image from
    # byte-for-byte durable-authority snapshots.
    command = (
        f"LC_ALL=C find {shlex.quote(metadata_path)} "
        "! -name '.udt_verification_cursor.bin' "
        "! -name '.udt_verification_cursor.bin.verification.tmp' "
        "-printf 'entry %y %P\\n' -type f -exec sha256sum -- {} + | sort"
    )
    return node.exec_in_container(["bash", "-o", "pipefail", "-c", command])


def udt_uuid(database, name):
    described = dict(
        row.split("\t", 1)
        for row in q(f"DESCRIBE TYPE {database}.{name} FORMAT TSV").splitlines()
    )
    return described["uuid"]


def type_identities(database, *, settings=ENABLED):
    return dict(
        row.split("\t", 1)
        for row in q(
            "SELECT name, toString(uuid) FROM system.user_defined_types "
            f"WHERE database = '{database}' ORDER BY name FORMAT TSV",
            settings=settings,
        ).splitlines()
    )


def normalize_type_whitespace(type_name):
    """Remove only pretty-print whitespace without changing type structure."""
    normalized = " ".join(type_name.split())
    return normalized.replace("( ", "(").replace(" )", ")").replace(" ,", ",")






def loss_summary_line(plan, type_name):
    matches = [
        line
        for line in plan["loss_summary"].splitlines()
        if line.startswith("TYPE ") and type_name in line
    ]
    assert len(matches) == 1, (type_name, plan["loss_summary"])
    return matches[0]


def test_introspection_waits_for_live_alter_publication(started_cluster):
    suffix = uuid.uuid4().hex[:8]
    database = f"udt_introspection_race_{suffix}"
    table = f"{database}.events"
    alter_outcome = {"returned": False, "error": None}
    alter_thread = None
    failpoint_enabled = False

    def alter_in_background():
        try:
            node.query(
                f"ALTER TABLE {table} MODIFY COLUMN id {database}.AccountId",
                settings=ENABLED,
                timeout=60,
            )
            alter_outcome["returned"] = True
        except BaseException as ex:  # noqa: BLE001 - surfaced in the main thread.
            alter_outcome["error"] = ex

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(f"CREATE TYPE {database}.AccountId AS UInt64")
        q(f"CREATE TABLE {table} (id {database}.UserId, note String) ENGINE = Memory")

        node.query(f"SYSTEM ENABLE FAILPOINT {ALTER_PUBLICATION_FAILPOINT}")
        failpoint_enabled = True
        alter_thread = threading.Thread(target=alter_in_background, daemon=True)
        alter_thread.start()

        node.query(
            f"SYSTEM WAIT FAILPOINT {ALTER_PUBLICATION_FAILPOINT} PAUSE",
            timeout=30,
        )
        assert alter_thread.is_alive(), "ALTER returned before its live metadata publication"

        # The authority root now describes AccountId while StorageMemory still
        # exposes the UserId metadata snapshot. Each presentation surface must
        # wait for the owning ALTER instead of reading that deliberately split state.
        paused_queries = (
            f"SHOW CREATE TABLE {table}",
            f"DESCRIBE TABLE {table}",
            "SELECT type, udt_declared_type FROM system.columns "
            f"WHERE database = '{database}' AND table = 'events' AND name = 'id'",
        )
        timeout_settings = {
            **ENABLED,
            "lock_acquire_timeout": 1,
        }
        for sql in paused_queries:
            started = time.monotonic()
            query_error = node.query_and_get_error(
                sql,
                settings=timeout_settings,
                timeout=10,
            )
            elapsed = time.monotonic() - started
            assert "DEADLOCK_AVOIDED" in query_error, query_error
            assert "timed out" in query_error.lower(), query_error
            assert elapsed < 8, (sql, elapsed, query_error)
            assert alter_thread.is_alive(), "ALTER resumed while its failpoint was enabled"

        node.query(f"SYSTEM DISABLE FAILPOINT {ALTER_PUBLICATION_FAILPOINT}")
        failpoint_enabled = False
        alter_thread.join(timeout=30)
        assert not alter_thread.is_alive(), "ALTER did not resume after disabling its failpoint"
        if alter_outcome["error"] is not None:
            raise alter_outcome["error"]
        assert alter_outcome["returned"]

        expected_type = f"{database}.AccountId"
        show_create = q(f"SHOW CREATE TABLE {table}")
        assert expected_type in show_create
        assert f"{database}.UserId" not in show_create

        described = rows_json(f"DESCRIBE TABLE {table}")
        assert next(row for row in described if row["name"] == "id")["type"] == expected_type
        assert rows_json(
            "SELECT type, udt_declared_type FROM system.columns "
            f"WHERE database = '{database}' AND table = 'events' AND name = 'id'"
        ) == [{"type": "UInt64", "udt_declared_type": expected_type}]
    finally:
        if failpoint_enabled:
            try:
                node.query(f"SYSTEM DISABLE FAILPOINT {ALTER_PUBLICATION_FAILPOINT}")
            except Exception:
                pass
        if alter_thread is not None:
            alter_thread.join(timeout=30)
        q(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_physical_introspection_fast_path_closes_initial_mapping_gap(
    started_cluster,
):
    suffix = uuid.uuid4().hex[:8]
    database = f"udt_physical_introspection_race_{suffix}"
    table = f"{database}.events"
    alter_outcome = {"returned": False, "error": None}
    alter_thread = None
    prepared_failpoint_enabled = False
    publication_failpoint_enabled = False

    def alter_in_background():
        try:
            node.query(
                f"ALTER TABLE {table} MODIFY COLUMN id {database}.AccountId",
                settings=ENABLED,
                timeout=60,
            )
            alter_outcome["returned"] = True
        except BaseException as ex:  # noqa: BLE001 - surfaced in the main thread.
            alter_outcome["error"] = ex

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.AccountId AS UInt64")
        q(f"CREATE TABLE {table} (id UInt64, note String) ENGINE = Memory")

        node.query(f"SYSTEM ENABLE FAILPOINT {ALTER_PREPARED_FAILPOINT}")
        prepared_failpoint_enabled = True
        node.query(f"SYSTEM ENABLE FAILPOINT {ALTER_PUBLICATION_FAILPOINT}")
        publication_failpoint_enabled = True
        alter_thread = threading.Thread(target=alter_in_background, daemon=True)
        alter_thread.start()

        node.query(
            f"SYSTEM WAIT FAILPOINT {ALTER_PREPARED_FAILPOINT} PAUSE",
            timeout=30,
        )
        assert alter_thread.is_alive(), "ALTER returned before authority admission"

        # The ALTER lock is held, but neither the live metadata nor the Atomic
        # authority owns a mapping yet. Physical SHOW must not wait for ALTER.
        started = time.monotonic()
        physical_create = node.query(
            f"SHOW CREATE TABLE {table}",
            settings={**ENABLED, "lock_acquire_timeout": 1},
            timeout=10,
        )
        elapsed = time.monotonic() - started
        assert "`id` UInt64" in physical_create
        assert f"{database}.AccountId" not in physical_create
        assert elapsed < 8, elapsed

        node.query(f"SYSTEM DISABLE FAILPOINT {ALTER_PREPARED_FAILPOINT}")
        prepared_failpoint_enabled = False
        node.query(
            f"SYSTEM WAIT FAILPOINT {ALTER_PUBLICATION_FAILPOINT} PAUSE",
            timeout=30,
        )
        assert alter_thread.is_alive(), "ALTER returned before live metadata publication"

        # The durable authority now owns the table UUID while its live snapshot
        # is still physical. The schema recheck must redirect SHOW to the ALTER
        # lock instead of exposing either half of that split image.
        query_error = node.query_and_get_error(
            f"SHOW CREATE TABLE {table}",
            settings={**ENABLED, "lock_acquire_timeout": 1},
            timeout=10,
        )
        assert "DEADLOCK_AVOIDED" in query_error, query_error
        assert "timed out" in query_error.lower(), query_error

        node.query(f"SYSTEM DISABLE FAILPOINT {ALTER_PUBLICATION_FAILPOINT}")
        publication_failpoint_enabled = False
        alter_thread.join(timeout=30)
        assert not alter_thread.is_alive(), "ALTER did not resume after disabling its failpoint"
        if alter_outcome["error"] is not None:
            raise alter_outcome["error"]
        assert alter_outcome["returned"]
        assert f"{database}.AccountId" in q(f"SHOW CREATE TABLE {table}")
    finally:
        if prepared_failpoint_enabled:
            try:
                node.query(f"SYSTEM DISABLE FAILPOINT {ALTER_PREPARED_FAILPOINT}")
            except Exception:
                pass
        if publication_failpoint_enabled:
            try:
                node.query(f"SYSTEM DISABLE FAILPOINT {ALTER_PUBLICATION_FAILPOINT}")
            except Exception:
                pass
        if alter_thread is not None:
            alter_thread.join(timeout=30)
        q(f"DROP DATABASE IF EXISTS {database} SYNC")








def test_wrapper_engine_matrix_introspection_and_restart(
    started_cluster, restart_server
):
    suffix = uuid.uuid4().hex[:8]
    database = f"udt_wrapper_matrix_{suffix}"

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(f"CREATE TYPE {database}.Label AS String")
        q(f"CREATE TYPE {database}.Code(N UInt16) AS FixedString(N)")

        user_id_uuid = udt_uuid(database, "UserId")
        label_uuid = udt_uuid(database, "Label")
        code_uuid = udt_uuid(database, "Code")
        logical_columns = f"""
            id {database}.UserId,
            code {database}.Code(3),
            tags Array({database}.Label),
            attributes Map({database}.Label, {database}.UserId),
            lookup LowCardinality({database}.Label),
            choice Variant({database}.Label, {database}.UserId),
            payload Nested(owner {database}.UserId, label {database}.Label)
        """
        physical_columns = """
            id UInt64,
            code FixedString(3),
            tags Array(String),
            attributes Map(String, UInt64),
            lookup LowCardinality(String),
            choice Variant(String, UInt64),
            payload Nested(owner UInt64, label String)
        """
        flatten_nested_settings = {**ENABLED, "flatten_nested": 1}
        q(
            f"CREATE TABLE {database}.logical_memory ({logical_columns}) "
            "ENGINE = Memory",
            settings=flatten_nested_settings,
        )
        q(
            f"CREATE TABLE {database}.logical_merge_tree ({logical_columns}) "
            "ENGINE = MergeTree ORDER BY id",
            settings=flatten_nested_settings,
        )
        q(
            f"CREATE TABLE {database}.physical_twin ({physical_columns}) "
            "ENGINE = MergeTree ORDER BY id",
            settings=flatten_nested_settings,
        )

        values = """
            (2, '002', ['b', 'bb'], map('x', 2), 'B', 2,
                [2, 20], ['owner-b', 'owner-bb']),
            (1, '001', ['a'], map('x', 1, 'y', 10), 'A', 'one',
                [1], ['owner-a'])
        """
        for table in ("logical_memory", "logical_merge_tree", "physical_twin"):
            q(f"INSERT INTO {database}.{table} VALUES {values}")

        runtime_query = (
            "SELECT id, length(code), arrayStringConcat(tags, '/'), "
            "attributes['x'], lower(lookup), variantType(choice), "
            "arraySum(payload.owner), arrayStringConcat(payload.label, '/') "
            "FROM {}.{} ORDER BY id FORMAT TSV"
        )
        expected_runtime = (
            "1\t3\ta\t1\ta\tString\t1\towner-a\n"
            "2\t3\tb/bb\t2\tb\tUInt64\t22\towner-b/owner-bb\n"
        )
        for table in ("logical_memory", "logical_merge_tree", "physical_twin"):
            assert q(runtime_query.format(database, table)) == expected_runtime

        type_names = q(
            "SELECT toTypeName(id), toTypeName(code), toTypeName(tags), "
            "toTypeName(attributes), toTypeName(lookup), toTypeName(choice), "
            "toTypeName(payload.owner), toTypeName(payload.label) "
            f"FROM {database}.logical_merge_tree LIMIT 1 FORMAT TSV"
        ).strip()
        assert type_names == (
            "UInt64\tFixedString(3)\tArray(String)\tMap(String, UInt64)\t"
            "LowCardinality(String)\tVariant(String, UInt64)\t"
            "Array(UInt64)\tArray(String)"
        )

        select_all = "SELECT * FROM {}.{} ORDER BY id"
        physical_hash = native_sha256(
            select_all.format(database, "physical_twin")
        )
        assert native_sha256(
            select_all.format(database, "logical_memory")
        ) == physical_hash
        assert native_sha256(
            select_all.format(database, "logical_merge_tree")
        ) == physical_hash

        columns = {
            row["name"]: row
            for row in rows_json(
                "SELECT name, type, udt_declared_type, udt_uuid, udt_revision, "
                "udt_definition_hash, udt_arguments, udt_instantiation_hash, "
                "udt_references FROM system.columns "
                f"WHERE database = '{database}' AND table = 'logical_merge_tree' "
                "ORDER BY position"
            )
        }
        assert columns["id"]["udt_declared_type"] == f"{database}.UserId"
        assert columns["id"]["udt_uuid"] == user_id_uuid
        assert columns["id"]["udt_arguments"] == []
        assert columns["code"]["udt_declared_type"] == f"{database}.Code(3)"
        assert columns["code"]["udt_uuid"] == code_uuid
        assert columns["code"]["udt_arguments"] == ["3"]
        for root in (columns["id"], columns["code"]):
            assert root["udt_revision"] > 0
            assert root["udt_definition_hash"]
            assert root["udt_instantiation_hash"]

        nested_expectations = {
            "tags": [([0], f"{database}.Label", label_uuid, "String")],
            "attributes": [
                ([0], f"{database}.Label", label_uuid, "String"),
                ([1], f"{database}.UserId", user_id_uuid, "UInt64"),
            ],
            "lookup": [([0], f"{database}.Label", label_uuid, "String")],
            "choice": [
                ([0], f"{database}.Label", label_uuid, "String"),
                ([1], f"{database}.UserId", user_id_uuid, "UInt64"),
            ],
            "payload.owner": [
                ([0], f"{database}.UserId", user_id_uuid, "UInt64")
            ],
            "payload.label": [
                ([0], f"{database}.Label", label_uuid, "String")
            ],
        }
        for column_name, expected in nested_expectations.items():
            column = columns[column_name]
            assert column["udt_declared_type"] == ""
            actual = [
                (
                    reference["path"],
                    reference["declared_type"],
                    reference["type_uuid"],
                    reference["physical_type"],
                )
                for reference in column["udt_references"]
            ]
            assert actual == expected
            for reference in column["udt_references"]:
                assert reference["type_revision"] > 0
                assert reference["type_definition_hash"]
                assert reference["type_instantiation_hash"]
                assert reference["storage_fingerprint"]

        table_uuid_before = q(
            "SELECT toString(uuid) FROM system.tables "
            f"WHERE database = '{database}' AND name = 'logical_merge_tree'"
        ).strip()
        identities_before_builtin_rename = type_identities(database)

        def logical_binding_snapshot():
            return rows_json(
                "SELECT table, name, udt_declared_type, udt_uuid, udt_references "
                "FROM system.columns "
                f"WHERE database = '{database}' "
                "AND table IN ('logical_memory', 'logical_merge_tree') "
                "ORDER BY table, position"
            )

        bindings_before_builtin_rename = logical_binding_snapshot()
        builtin_rename_error = error(
            f"ALTER TYPE {database}.Label RENAME TO Text"
        )
        assert "(BAD_ARGUMENTS)" in builtin_rename_error
        assert (
            "cannot use a registered built-in family or alias"
            in builtin_rename_error.lower()
        )
        assert type_identities(database) == identities_before_builtin_rename
        assert udt_uuid(database, "Label") == label_uuid
        assert logical_binding_snapshot() == bindings_before_builtin_rename

        q(f"ALTER TYPE {database}.UserId RENAME TO PrincipalId")
        q(f"ALTER TYPE {database}.Label RENAME TO EventLabel")
        q(
            f"RENAME TABLE {database}.logical_merge_tree "
            f"TO {database}.renamed_merge_tree"
        )
        assert udt_uuid(database, "PrincipalId") == user_id_uuid
        assert udt_uuid(database, "EventLabel") == label_uuid
        assert q(
            "SELECT toString(uuid) FROM system.tables "
            f"WHERE database = '{database}' AND name = 'renamed_merge_tree'"
        ).strip() == table_uuid_before

        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(f"CREATE TYPE {database}.Label AS String")
        recreated_user_id_uuid = udt_uuid(database, "UserId")
        recreated_label_uuid = udt_uuid(database, "Label")
        assert recreated_user_id_uuid != user_id_uuid
        assert recreated_label_uuid != label_uuid

        restart_server()
        assert q(f"EXISTS TABLE {database}.logical_merge_tree").strip() == "0"
        assert q(f"EXISTS TABLE {database}.renamed_merge_tree").strip() == "1"
        assert native_sha256(
            select_all.format(database, "renamed_merge_tree")
        ) == physical_hash
        assert q(f"SELECT count() FROM {database}.logical_memory").strip() == "0"

        renamed_show = q(f"SHOW CREATE TABLE {database}.renamed_merge_tree")
        assert f"{database}.PrincipalId" in renamed_show
        assert f"{database}.EventLabel" in renamed_show
        assert f"{database}.Text" not in renamed_show
        assert f"{database}.UserId" not in renamed_show
        assert f"{database}.Label" not in renamed_show
        renamed_columns = rows_json(
            "SELECT udt_declared_type, udt_references FROM system.columns "
            f"WHERE database = '{database}' AND table = 'renamed_merge_tree' "
            "ORDER BY position"
        )
        renamed_projection = json.dumps(renamed_columns, sort_keys=True)
        assert f"{database}.PrincipalId" in renamed_projection
        assert f"{database}.EventLabel" in renamed_projection
        assert f"{database}.Text" not in renamed_projection
        assert f"{database}.UserId" not in renamed_projection
        assert f"{database}.Label" not in renamed_projection

        q(f"DROP TYPE {database}.UserId RESTRICT")
        q(f"DROP TYPE {database}.Label RESTRICT")
        restrict = error(f"DROP TYPE {database}.PrincipalId RESTRICT")
        assert "dependent" in restrict.lower() or "refer" in restrict.lower()

        q(f"DROP TABLE {database}.renamed_merge_tree SYNC")
        q(f"DROP TABLE {database}.logical_memory SYNC")
        q(f"DROP TYPE {database}.PrincipalId RESTRICT")
        q(f"DROP TYPE {database}.EventLabel RESTRICT")
        q(f"DROP TYPE {database}.Code RESTRICT")
        assert q(
            "SELECT count() FROM system.user_defined_types "
            f"WHERE database = '{database}'"
        ).strip() == "0"
    finally:
        q(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_rejected_admission_and_metadata_mutations_write_nothing(started_cluster):
    suffix = uuid.uuid4().hex[:8]
    source = f"udt_fail_closed_source_{suffix}"
    target = f"udt_fail_closed_target_{suffix}"

    try:
        q(f"CREATE DATABASE {source} ENGINE = Atomic")
        q(f"CREATE DATABASE {target} ENGINE = Atomic")
        q(f"CREATE TYPE {source}.UserId AS UInt64")
        q(f"CREATE TYPE {target}.LocalId AS UInt8")
        q(f"CREATE TABLE {source}.physical (id UInt64) ENGINE = Memory")
        q(f"INSERT INTO {source}.physical VALUES (11)")

        source_before = database_metadata_snapshot(source)
        target_before = database_metadata_snapshot(target)
        rejected = error(
            f"ALTER TABLE {source}.physical ADD COLUMN mapped {source}.UserId",
            settings=DISABLED,
        )
        assert "disabled" in rejected.lower()
        rejected = error(
            f"ALTER TABLE {source}.physical MODIFY COLUMN id {source}.UserId",
            settings=DISABLED,
        )
        assert "disabled" in rejected.lower()
        assert "support only memory" in error(
            f"CREATE TABLE {source}.tiny_log (id {source}.UserId) ENGINE = TinyLog"
        ).lower()
        assert "cannot span database authorities" in error(
            f"CREATE TABLE {target}.cross_database "
            f"(id {source}.UserId) ENGINE = Memory"
        ).lower()
        assert "stored create context" in error(
            f"CREATE TABLE IF NOT EXISTS {source}.if_not_exists "
            f"(id {source}.UserId) ENGINE = Memory"
        ).lower()
        assert "stored create context" in error(
            f"CREATE TEMPORARY TABLE temporary_probe_{suffix} "
            f"(id {source}.UserId) ENGINE = Memory"
        ).lower()
        invalid_map = error(
            f"CREATE TABLE {source}.invalid_map "
            f"(value Map(Nullable({source}.UserId), String)) ENGINE = Memory"
        )
        assert "map" in invalid_map.lower() and "nullable" in invalid_map.lower()
        invalid_low_cardinality = error(
            f"CREATE TABLE {source}.invalid_low_cardinality "
            f"(value LowCardinality(Array({source}.UserId))) ENGINE = Memory"
        )
        assert "lowcardinality" in invalid_low_cardinality.lower()

        for table in (
            "tiny_log",
            "if_not_exists",
            "invalid_map",
            "invalid_low_cardinality",
        ):
            assert q(f"EXISTS TABLE {source}.{table}").strip() == "0"
        assert q(f"EXISTS TABLE {target}.cross_database").strip() == "0"
        assert q(
            "SELECT name, type, udt_declared_type FROM system.columns "
            f"WHERE database = '{source}' AND table = 'physical' FORMAT TSV"
        ) == "id\tUInt64\t\n"
        assert database_metadata_snapshot(source) == source_before
        assert database_metadata_snapshot(target) == target_before

        q(
            f"CREATE TABLE {source}.mapped (id {source}.UserId) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {source}.mapped VALUES (1), (2)")
        mapped_uuid = q(
            "SELECT toString(uuid) FROM system.tables "
            f"WHERE database = '{source}' AND name = 'mapped'"
        ).strip()
        source_before = database_metadata_snapshot(source)
        target_before = database_metadata_snapshot(target)
        mapped_data_hash = native_sha256(
            f"SELECT * FROM {source}.mapped ORDER BY id"
        )
        mapped_provenance = q(
            "SELECT name, udt_declared_type, toString(udt_uuid) "
            "FROM system.columns "
            f"WHERE database = '{source}' AND table = 'mapped' "
            "ORDER BY position FORMAT TSV"
        )

        q(f"DETACH TABLE {source}.mapped")
        assert q(f"EXISTS TABLE {source}.mapped").strip() == "0"
        q(f"ATTACH TABLE {source}.mapped")
        assert "cross-database udt authority transfer is not implemented" in error(
            f"RENAME TABLE {source}.mapped TO {target}.moved"
        ).lower()
        assert "rename exchange is not supported" in error(
            f"EXCHANGE TABLES {source}.mapped AND {source}.physical"
        ).lower()
        assert "rename database is not supported" in error(
            f"RENAME DATABASE {source} TO {source}_renamed"
        ).lower()

        assert q(f"EXISTS TABLE {source}.mapped").strip() == "1"
        assert q(f"EXISTS TABLE {target}.moved").strip() == "0"
        assert q(f"EXISTS DATABASE {source}_renamed").strip() == "0"
        assert native_sha256(
            f"SELECT * FROM {source}.mapped ORDER BY id"
        ) == mapped_data_hash
        assert q(
            "SELECT toString(uuid) FROM system.tables "
            f"WHERE database = '{source}' AND name = 'mapped'"
        ).strip() == mapped_uuid
        assert q(
            "SELECT name, udt_declared_type, toString(udt_uuid) "
            "FROM system.columns "
            f"WHERE database = '{source}' AND table = 'mapped' "
            "ORDER BY position FORMAT TSV"
        ) == mapped_provenance
        assert database_metadata_snapshot(source) == source_before
        assert database_metadata_snapshot(target) == target_before
    finally:
        q(f"DROP DATABASE IF EXISTS {source} SYNC")
        q(f"DROP DATABASE IF EXISTS {source}_renamed SYNC")
        q(f"DROP DATABASE IF EXISTS {target} SYNC")


def test_usage_type_multi_reference_authorization_is_atomic(started_cluster):
    suffix = uuid.uuid4().hex[:8]
    database = f"udt_usage_atomic_{suffix}"
    writer = f"udt_usage_writer_{suffix}"

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(f"CREATE TYPE {database}.SecretId AS UInt64")
        q(f"CREATE USER {writer} IDENTIFIED WITH no_password")
        q(
            f"GRANT CREATE TABLE, ALTER TABLE, SELECT, INSERT "
            f"ON {database}.* TO {writer}"
        )
        q(f"GRANT TABLE ENGINE ON Memory TO {writer}")

        database_uuid = q(
            f"SELECT toString(uuid) FROM system.databases WHERE name = '{database}'"
        ).strip()
        user_id_uuid = udt_uuid(database, "UserId")
        secret_id_uuid = udt_uuid(database, "SecretId")
        q(
            "GRANT USAGE TYPE ON TYPE UUID "
            f"'{database_uuid}' '{user_id_uuid}' TO {writer}"
        )

        before = database_metadata_snapshot(database)
        denied = error(
            f"CREATE TABLE {database}.events "
            f"(id {database}.UserId, ids Array({database}.UserId), "
            f"secret {database}.SecretId) ENGINE = MergeTree ORDER BY id",
            user=writer,
        )
        assert "usage" in denied.lower()
        assert q(f"EXISTS TABLE {database}.events").strip() == "0"
        assert database_metadata_snapshot(database) == before

        q(
            "GRANT USAGE TYPE ON TYPE UUID "
            f"'{database_uuid}' '{secret_id_uuid}' TO {writer}"
        )
        q(
            f"CREATE TABLE {database}.events "
            f"(id {database}.UserId, ids Array({database}.UserId), "
            f"secret {database}.SecretId) ENGINE = MergeTree ORDER BY id",
            user=writer,
        )
        q(f"INSERT INTO {database}.events VALUES (1, [1, 10], 100)", user=writer)

        q(
            "REVOKE USAGE TYPE ON TYPE UUID "
            f"'{database_uuid}' '{secret_id_uuid}' FROM {writer}"
        )
        assert q(f"SELECT sum(secret) FROM {database}.events", user=writer).strip() == "100"
        q(f"INSERT INTO {database}.events VALUES (2, [2, 20], 200)", user=writer)
        before = database_metadata_snapshot(database)
        denied = error(
            f"ALTER TABLE {database}.events ADD COLUMN "
            f"secret_copy {database}.SecretId DEFAULT secret",
            user=writer,
        )
        assert "usage" in denied.lower()
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table = 'events' "
            "AND name = 'secret_copy'"
        ).strip() == "0"
        assert database_metadata_snapshot(database) == before

        q(
            "GRANT USAGE TYPE ON TYPE UUID "
            f"'{database_uuid}' '{secret_id_uuid}' TO {writer}"
        )
        q(
            f"ALTER TABLE {database}.events ADD COLUMN "
            f"secret_copy {database}.SecretId DEFAULT secret",
            user=writer,
        )
        assert q(f"SELECT sum(secret_copy) FROM {database}.events").strip() == "300"
        assert q(
            "SELECT default_kind, default_expression FROM system.columns "
            f"WHERE database = '{database}' AND table = 'events' "
            "AND name = 'secret_copy' FORMAT TSV"
        ) == "DEFAULT\tsecret\n"
        q(f"ALTER TYPE {database}.SecretId RENAME TO PrivateId")
        assert udt_uuid(database, "PrivateId") == secret_id_uuid
        q(
            f"ALTER TABLE {database}.events ADD COLUMN "
            f"private_copy {database}.PrivateId DEFAULT secret_copy",
            user=writer,
        )
        assert q(
            "SELECT default_kind, default_expression FROM system.columns "
            f"WHERE database = '{database}' AND table = 'events' "
            "AND name = 'private_copy' FORMAT TSV"
        ) == "DEFAULT\tsecret_copy\n"
        assert q(f"SELECT sum(private_copy) FROM {database}.events").strip() == "300"
        q(f"CREATE TYPE {database}.SecretId AS UInt64")
        assert udt_uuid(database, "SecretId") != secret_id_uuid
        denied = error(
            f"ALTER TABLE {database}.events ADD COLUMN "
            f"recreated_copy {database}.SecretId DEFAULT secret",
            user=writer,
        )
        assert "usage" in denied.lower()
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table = 'events' "
            "AND name = 'recreated_copy'"
        ).strip() == "0"
        assert q(
            f"SELECT sum(secret), sum(secret_copy), sum(private_copy) "
            f"FROM {database}.events",
            user=writer,
        ).strip() == "300\t300\t300"
    finally:
        q(f"DROP DATABASE IF EXISTS {database} SYNC")
        q(f"DROP USER IF EXISTS {writer}")






def test_same_name_recreation_with_different_body_keeps_both_identities_after_restart(
    started_cluster,
    restart_server,
):
    suffix = uuid.uuid4().hex[:8]
    database = f"udt_recreate_body_{suffix}"

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        legacy_uuid = udt_uuid(database, "UserId")
        q(
            f"CREATE TABLE {database}.legacy_rows "
            f"(id {database}.UserId, payload String) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.legacy_rows VALUES (7, 'legacy')")

        q(f"ALTER TYPE {database}.UserId RENAME TO LegacyUserId")
        q(f"CREATE TYPE {database}.UserId AS String")
        current_uuid = udt_uuid(database, "UserId")
        assert current_uuid != legacy_uuid
        q(
            f"CREATE TABLE {database}.mixed_rows "
            f"(legacy_id {database}.LegacyUserId, current_id {database}.UserId) "
            "ENGINE = MergeTree ORDER BY legacy_id"
        )
        q(f"INSERT INTO {database}.mixed_rows VALUES (8, 'current')")

        restart_server()
        assert udt_uuid(database, "LegacyUserId") == legacy_uuid
        assert udt_uuid(database, "UserId") == current_uuid
        assert q(
            f"SELECT id, payload FROM {database}.legacy_rows FORMAT TSV"
        ) == "7\tlegacy\n"
        assert q(
            f"SELECT legacy_id, current_id FROM {database}.mixed_rows FORMAT TSV"
        ) == "8\tcurrent\n"

        columns = {
            row["name"]: row
            for row in rows_json(
                "SELECT name, type, udt_declared_type, toString(udt_uuid) AS udt_uuid "
                "FROM system.columns "
                f"WHERE database = '{database}' AND table = 'mixed_rows' "
                "ORDER BY position"
            )
        }
        assert columns["legacy_id"] == {
            "name": "legacy_id",
            "type": "UInt64",
            "udt_declared_type": f"{database}.LegacyUserId",
            "udt_uuid": legacy_uuid,
        }
        assert columns["current_id"] == {
            "name": "current_id",
            "type": "String",
            "udt_declared_type": f"{database}.UserId",
            "udt_uuid": current_uuid,
        }
        show_create = q(f"SHOW CREATE TABLE {database}.mixed_rows")
        assert f"{database}.LegacyUserId" in show_create
        assert f"{database}.UserId" in show_create
        for type_name in ("LegacyUserId", "UserId"):
            restrict = error(f"DROP TYPE {database}.{type_name} RESTRICT")
            assert "dependent" in restrict.lower() or "refer" in restrict.lower()
    finally:
        q(f"DROP DATABASE IF EXISTS {database} SYNC")






def test_multi_udt_alter_batch_is_atomic_and_updates_every_reference(
    started_cluster,
    restart_server,
):
    suffix = uuid.uuid4().hex[:8]
    database = f"udt_multi_alter_{suffix}"

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.Number AS UInt64")
        q(f"CREATE TYPE {database}.TextNumber AS String")
        q(f"CREATE TYPE {database}.SmallNumber AS UInt32")
        q(
            f"CREATE TABLE {database}.events "
            f"(row_id UInt64, old_a {database}.Number, "
            f"old_b {database}.TextNumber, doomed {database}.SmallNumber) "
            "ENGINE = MergeTree ORDER BY row_id"
        )
        q(f"INSERT INTO {database}.events VALUES (1, 10, '20', 30)")

        before = database_metadata_snapshot(database)
        rejected = error(
            f"ALTER TABLE {database}.events "
            f"ADD COLUMN partial {database}.Number AFTER row_id, "
            f"MODIFY COLUMN definitely_missing {database}.TextNumber"
        )
        assert "column" in rejected.lower()
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table = 'events' "
            "AND name = 'partial'"
        ).strip() == "0"
        assert database_metadata_snapshot(database) == before

        q(
            f"ALTER TABLE {database}.events "
            f"ADD COLUMN added {database}.SmallNumber DEFAULT 7 FIRST, "
            f"MODIFY COLUMN old_b {database}.Number AFTER row_id, "
            "DROP COLUMN doomed, "
            "RENAME COLUMN old_a TO renamed_a",
            settings={**ENABLED, "mutations_sync": 2},
        )
        columns = rows_json(
            "SELECT name, type, udt_declared_type FROM system.columns "
            f"WHERE database = '{database}' AND table = 'events' "
            "ORDER BY position"
        )
        assert columns == [
            {
                "name": "added",
                "type": "UInt32",
                "udt_declared_type": f"{database}.SmallNumber",
            },
            {"name": "row_id", "type": "UInt64", "udt_declared_type": ""},
            {
                "name": "old_b",
                "type": "UInt64",
                "udt_declared_type": f"{database}.Number",
            },
            {
                "name": "renamed_a",
                "type": "UInt64",
                "udt_declared_type": f"{database}.Number",
            },
        ]
        assert q(
            f"SELECT added, row_id, old_b, renamed_a "
            f"FROM {database}.events FORMAT TSV"
        ) == "7\t1\t20\t10\n"

        q(f"DROP TYPE {database}.TextNumber RESTRICT")
        for type_name in ("Number", "SmallNumber"):
            restrict = error(f"DROP TYPE {database}.{type_name} RESTRICT")
            assert "dependent" in restrict.lower() or "refer" in restrict.lower()
        restart_server()
        assert q(
            f"SELECT added, row_id, old_b, renamed_a "
            f"FROM {database}.events FORMAT TSV"
        ) == "7\t1\t20\t10\n"
    finally:
        q(f"DROP DATABASE IF EXISTS {database} SYNC")




def test_table_ddl_and_type_lifecycle_races_are_serialized(started_cluster):
    suffix = uuid.uuid4().hex[:8]
    database = f"udt_ddl_races_{suffix}"
    mutation_thread = None
    failpoint_enabled = False

    def interleave_type_mutation(mutation_sql, table_ddl):
        nonlocal mutation_thread, failpoint_enabled
        outcome = {"returned": False, "error": None}

        def mutate_in_background():
            try:
                node.query(mutation_sql, settings=ENABLED, timeout=60)
            except BaseException as ex:  # noqa: BLE001 - asserted in the main thread.
                outcome["error"] = ex
            finally:
                outcome["returned"] = True

        node.query(f"SYSTEM ENABLE FAILPOINT {TYPE_MUTATION_LOOKUP_FAILPOINT}")
        failpoint_enabled = True
        mutation_thread = threading.Thread(
            target=mutate_in_background,
            daemon=True,
        )
        mutation_thread.start()
        node.query(
            f"SYSTEM WAIT FAILPOINT {TYPE_MUTATION_LOOKUP_FAILPOINT} PAUSE",
            timeout=30,
        )
        assert mutation_thread.is_alive()
        try:
            table_ddl()
        finally:
            node.query(f"SYSTEM DISABLE FAILPOINT {TYPE_MUTATION_LOOKUP_FAILPOINT}")
            failpoint_enabled = False
            mutation_thread.join(timeout=30)
        assert not mutation_thread.is_alive()
        assert outcome["returned"]
        mutation_thread = None
        return outcome["error"]

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")

        q(f"CREATE TYPE {database}.CreateVsDrop AS UInt8")
        mutation_error = interleave_type_mutation(
            f"DROP TYPE {database}.CreateVsDrop RESTRICT",
            lambda: q(
                f"CREATE TABLE {database}.created_before_drop "
                f"(id {database}.CreateVsDrop) ENGINE = Memory"
            ),
        )
        assert mutation_error is not None
        assert "dependent" in str(mutation_error).lower() or "refer" in str(
            mutation_error
        ).lower()

        q(f"CREATE TYPE {database}.CreateVsRename AS UInt16")
        assert interleave_type_mutation(
            f"ALTER TYPE {database}.CreateVsRename RENAME TO CreatedRenamed",
            lambda: q(
                f"CREATE TABLE {database}.created_before_rename "
                f"(id {database}.CreateVsRename) ENGINE = Memory"
            ),
        ) is None
        assert f"{database}.CreatedRenamed" in q(
            f"SHOW CREATE TABLE {database}.created_before_rename"
        )

        q(f"CREATE TYPE {database}.AlterVsDrop AS UInt32")
        q(f"CREATE TABLE {database}.alter_before_drop (key UInt8) ENGINE = Memory")
        mutation_error = interleave_type_mutation(
            f"DROP TYPE {database}.AlterVsDrop RESTRICT",
            lambda: q(
                f"ALTER TABLE {database}.alter_before_drop "
                f"ADD COLUMN value {database}.AlterVsDrop"
            ),
        )
        assert mutation_error is not None
        assert "dependent" in str(mutation_error).lower() or "refer" in str(
            mutation_error
        ).lower()

        q(f"CREATE TYPE {database}.AlterVsRename AS UInt64")
        q(f"CREATE TABLE {database}.alter_before_rename (key UInt8) ENGINE = Memory")
        assert interleave_type_mutation(
            f"ALTER TYPE {database}.AlterVsRename RENAME TO AlteredRenamed",
            lambda: q(
                f"ALTER TABLE {database}.alter_before_rename "
                f"ADD COLUMN value {database}.AlterVsRename"
            ),
        ) is None
        assert f"{database}.AlteredRenamed" in q(
            f"SHOW CREATE TABLE {database}.alter_before_rename"
        )

        q(f"CREATE TYPE {database}.DropVsDrop AS UInt128")
        q(
            f"CREATE TABLE {database}.dropped_before_type "
            f"(id {database}.DropVsDrop) ENGINE = Memory"
        )
        assert interleave_type_mutation(
            f"DROP TYPE {database}.DropVsDrop RESTRICT",
            lambda: q(f"DROP TABLE {database}.dropped_before_type SYNC"),
        ) is None
        assert "DropVsDrop" not in type_identities(database)

        q(f"CREATE TYPE {database}.DropVsRename AS UInt256")
        q(
            f"CREATE TABLE {database}.dropped_before_rename "
            f"(id {database}.DropVsRename) ENGINE = Memory"
        )
        assert interleave_type_mutation(
            f"ALTER TYPE {database}.DropVsRename RENAME TO DroppedRenamed",
            lambda: q(f"DROP TABLE {database}.dropped_before_rename SYNC"),
        ) is None
        identities = type_identities(database)
        assert "DropVsRename" not in identities
        assert "DroppedRenamed" in identities
    finally:
        if failpoint_enabled:
            try:
                node.query(
                    f"SYSTEM DISABLE FAILPOINT {TYPE_MUTATION_LOOKUP_FAILPOINT}"
                )
            except Exception:
                pass
        if mutation_thread is not None:
            mutation_thread.join(timeout=30)
        q(f"DROP DATABASE IF EXISTS {database} SYNC")
