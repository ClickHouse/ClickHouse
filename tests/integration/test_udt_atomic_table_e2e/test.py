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


def physicalization_dry_run(selector, *, user="default"):
    result = rows_json(
        f"PHYSICALIZE TYPE REFERENCES {selector} DRY RUN",
        user=user,
    )
    assert len(result) == 1
    assert result[0]["manifest_count"] > 0
    assert result[0]["apply_token"]
    encoded_manifest = result[0]["canonical_loss_manifest_base64"]
    manifest = base64.b64decode(encoded_manifest, validate=True)
    assert manifest
    assert base64.b64encode(manifest).decode("ascii") == encoded_manifest
    assert "canonical_loss_manifest" not in result[0]
    digest = hashlib.sha256()
    digest.update(MANIFEST_HASH_DOMAIN)
    digest.update(b"\0")
    digest.update(encode_var_uint(len(manifest)))
    digest.update(manifest)
    assert digest.hexdigest() == result[0]["manifest_digest"]
    return result[0]


def dry_run(database, *, user="default"):
    return physicalization_dry_run(f"DATABASE {database}", user=user)


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


def test_schema_inferred_table_function_create_is_fail_closed(started_cluster):
    suffix = uuid.uuid4().hex[:8]
    database = f"udt_table_function_guard_{suffix}"

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(f"CREATE TABLE {database}.events (id {database}.UserId) ENGINE = Memory")

        # Direct AS-table-function schema inference has no generic resolved
        # source closure. In particular, merge() could otherwise copy the
        # physical schema of a mapped table into an untracked permanent wrapper.
        inferred_error = error(
            f"CREATE TABLE {database}.inferred_escape "
            f"AS merge('{database}', '^events$')"
        )
        assert "invalid logical provenance source" in inferred_error.lower()
        assert "stored create context" in inferred_error.lower()
        assert q(f"EXISTS TABLE {database}.inferred_escape").strip() == "0"

        # The explicit-schema screening boundary must recognize every quoted
        # identifier and whitespace form accepted by the SQL lexer.  In
        # particular, curly quotes and Unicode whitespace must not hide a
        # qualified UDT inside a table-function string.
        unicode_schema_error = error(
            f"CREATE TABLE {database}.unicode_schema_escape "
            f"AS values('id “{database}”\u2009.\u2009“UserId”', 42)"
        )
        assert "incomplete type-string classification" in unicode_schema_error.lower()
        assert q(f"EXISTS TABLE {database}.unicode_schema_escape").strip() == "0"
        assert q(f"SHOW CREATE TYPE {database}.UserId").strip().endswith("AS UInt64")

        # An explicit physical schema closes that inference gap. The wrapper
        # remains usable while carrying no copied UDT provenance of its own.
        q(f"INSERT INTO {database}.events VALUES (42)")
        q(
            f"CREATE TABLE {database}.explicit_physical (id UInt64) "
            f"AS merge('{database}', '^events$')"
        )
        assert q(f"SELECT sum(id) FROM {database}.explicit_physical").strip() == "42"
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table = 'explicit_physical' "
            "AND (udt_declared_type != '' OR notEmpty(udt_references))"
        ).strip() == "0"

        # A table function whose schema is intrinsic remains ordinary built-in
        # behavior even in the UDT-enabled session.
        q(f"CREATE TABLE {database}.static_table_function AS numbers(1)")
        assert q(f"SELECT count() FROM {database}.static_table_function").strip() == "1"
    finally:
        q(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_physicalization_apply_waits_for_prepared_alter(started_cluster):
    suffix = uuid.uuid4().hex[:8]
    race_timeout = 300 if node.is_built_with_sanitizer() else 60
    apply_query_id = f"udt_physicalize_apply_wait_{suffix}"
    database = f"udt_physicalize_alter_race_{suffix}"
    table = f"{database}.events"
    alter_outcome = {"returned": False, "error": None}
    alter_thread = None
    failpoint_enabled = False
    apply_query_may_be_running = False

    def alter_in_background():
        try:
            node.query(
                f"ALTER TABLE {table} MODIFY COLUMN id {database}.AccountId",
                settings=ENABLED,
                timeout=race_timeout,
            )
            alter_outcome["returned"] = True
        except BaseException as ex:  # noqa: BLE001 - surfaced in the main thread.
            alter_outcome["error"] = ex

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(f"CREATE TYPE {database}.AccountId AS UInt64")
        q(f"CREATE TABLE {table} (id {database}.UserId) ENGINE = Memory")
        plan = dry_run(database)

        node.query(f"SYSTEM ENABLE FAILPOINT {ALTER_PREPARED_FAILPOINT}")
        failpoint_enabled = True
        alter_thread = threading.Thread(target=alter_in_background, daemon=True)
        alter_thread.start()
        node.query(
            f"SYSTEM WAIT FAILPOINT {ALTER_PREPARED_FAILPOINT} PAUSE",
            timeout=race_timeout,
        )
        assert alter_thread.is_alive(), "ALTER returned before publishing its prepared package"

        # ALTER owns the table ALTER lock but has not entered DatabaseAtomic yet.
        # APPLY must wait without holding the schema mutex, and a timed-out wait
        # must leave the token available for the subsequent stale-root decision.
        apply_sql = (
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(plan["apply_token"])
        )
        apply_query_may_be_running = True
        apply_error = node.query_and_get_error(
            apply_sql,
            settings={**ENABLED, "lock_acquire_timeout": 1},
            timeout=race_timeout,
            query_id=apply_query_id,
        )
        apply_query_may_be_running = False
        assert "DEADLOCK_AVOIDED" in apply_error, apply_error
        assert "user-defined type physicalization" in apply_error.lower(), apply_error
        assert "timed out" in apply_error.lower(), apply_error
        assert "(1000 ms)" in apply_error, apply_error
        assert alter_thread.is_alive(), "ALTER resumed while its failpoint was enabled"

        node.query(f"SYSTEM DISABLE FAILPOINT {ALTER_PREPARED_FAILPOINT}")
        failpoint_enabled = False
        alter_thread.join(timeout=race_timeout)
        assert not alter_thread.is_alive(), "ALTER did not resume after disabling its failpoint"
        if alter_outcome["error"] is not None:
            raise alter_outcome["error"]
        assert alter_outcome["returned"]

        stale = error(apply_sql)
        assert "anchored to an obsolete authority root" in stale.lower(), stale
        assert f"{database}.AccountId" in q(f"SHOW CREATE TABLE {table}")
    finally:
        if apply_query_may_be_running:
            try:
                node.query(
                    f"KILL QUERY WHERE query_id = {sql_string(apply_query_id)} SYNC",
                    timeout=race_timeout,
                )
            except Exception:
                pass
        if failpoint_enabled:
            try:
                node.query(f"SYSTEM DISABLE FAILPOINT {ALTER_PREPARED_FAILPOINT}")
            except Exception:
                pass
        if alter_thread is not None:
            alter_thread.join(timeout=race_timeout)
        q(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_atomic_table_product_journey(started_cluster, restart_server):
    suffix = uuid.uuid4().hex[:8]
    database = f"udt_e2e_{suffix}"
    writer = f"udt_writer_{suffix}"
    config_disabled = False

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(f"CREATE TYPE {database}.Box(T TYPE) AS Tuple(value T)")

        # A disabled reference-bearing CREATE is rejected before either the table
        # metadata or the per-database UDT authority can be touched.
        files_before = database_metadata_snapshot(database)
        disabled_create_error = error(
            f"CREATE TABLE {database}.disabled_probe "
            f"(id {database}.UserId) ENGINE = Memory",
            settings=DISABLED,
        )
        assert "disabled" in disabled_create_error.lower()
        assert database_metadata_snapshot(database) == files_before
        assert q(f"EXISTS TABLE {database}.disabled_probe").strip() == "0"

        logical_columns = f"""
            id {database}.UserId,
            ids Array({database}.UserId),
            owner Tuple(primary {database}.UserId, optional Nullable({database}.UserId)),
            boxed {database}.Box(Nullable({database}.UserId))
        """
        physical_columns = """
            id UInt64,
            ids Array(UInt64),
            owner Tuple(primary UInt64, optional Nullable(UInt64)),
            boxed Tuple(value Nullable(UInt64))
        """
        q(f"CREATE TABLE {database}.memory ({logical_columns}) ENGINE = Memory")
        q(
            f"CREATE TABLE {database}.events ({logical_columns}) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(
            f"CREATE TABLE {database}.physical_twin ({physical_columns}) "
            "ENGINE = MergeTree ORDER BY id"
        )
        values = """
            (3, [3, 30], tuple(3, NULL), tuple(300)),
            (1, [1, 10], tuple(1, 11), tuple(NULL)),
            (2, [2, 20], tuple(2, 22), tuple(200))
        """
        for table in ("memory", "events", "physical_twin"):
            q(f"INSERT INTO {database}.{table} VALUES {values}")

        # The execution pipeline sees only physical ClickHouse types. Exercise
        # expressions, nullable/tuple/array access, grouping, sorting and a join.
        [type_names] = rows_json(
            f"SELECT [toTypeName(id), toTypeName(ids), toTypeName(owner), "
            f"toTypeName(boxed)] AS names FROM {database}.events LIMIT 1"
        )
        assert [normalize_type_whitespace(name) for name in type_names["names"]] == [
            "UInt64",
            "Array(UInt64)",
            "Tuple(primary UInt64, optional Nullable(UInt64))",
            "Tuple(value Nullable(UInt64))",
        ]
        assert q(
            f"SELECT id, arraySum(ids), owner.optional, boxed.value "
            f"FROM {database}.events ORDER BY id FORMAT TSV"
        ) == "1\t11\t11\t\\N\n2\t22\t22\t200\n3\t33\t\\N\t300\n"
        assert q(
            f"SELECT e.id, count(), sum(arraySum(e.ids)) "
            f"FROM {database}.events e INNER JOIN {database}.physical_twin p "
            "ON e.id = p.id GROUP BY e.id ORDER BY e.id FORMAT TSV"
        ) == "1\t1\t11\n2\t1\t22\n3\t1\t33\n"
        select_all = "SELECT * FROM {}.{} ORDER BY id"
        assert native_sha256(select_all.format(database, "events")) == native_sha256(
            select_all.format(database, "physical_twin")
        )

        show_create = q(f"SHOW CREATE TABLE {database}.events").strip()
        assert f"{database}.UserId" in show_create
        assert f"{database}.Box(Nullable({database}.UserId))" in show_create
        described = q(f"DESCRIBE TABLE {database}.events FORMAT TSV")
        assert f"{database}.UserId" in described
        columns = rows_json(
            "SELECT name, type, udt_declared_type, udt_arguments, udt_references "
            f"FROM system.columns WHERE database = '{database}' "
            "AND table = 'events' ORDER BY position"
        )
        assert [normalize_type_whitespace(column["type"]) for column in columns] == [
            "UInt64",
            "Array(UInt64)",
            "Tuple(primary UInt64, optional Nullable(UInt64))",
            "Tuple(value Nullable(UInt64))",
        ]
        assert columns[0]["udt_declared_type"] == f"{database}.UserId"
        assert columns[3]["udt_declared_type"] == (
            f"{database}.Box(Nullable({database}.UserId))"
        )
        assert columns[3]["udt_arguments"] == ["Nullable(UInt64)"]
        assert all(column["udt_references"] for column in columns)

        # USAGE TYPE is UUID-bound: rename preserves the grant, recreating the
        # old spelling does not, and revocation affects new DDL but not reads.
        q(f"CREATE USER {writer} IDENTIFIED WITH no_password")
        q(
            f"GRANT CREATE TABLE, ALTER TABLE, SELECT, INSERT "
            f"ON {database}.* TO {writer}"
        )
        q(f"GRANT TABLE ENGINE ON MergeTree TO {writer}")
        denied = error(
            f"CREATE TABLE {database}.writer_denied "
            f"(id {database}.UserId) ENGINE = Memory",
            user=writer,
        )
        assert "usage" in denied.lower()
        assert q(f"EXISTS TABLE {database}.writer_denied").strip() == "0"
        database_uuid = q(
            f"SELECT toString(uuid) FROM system.databases WHERE name = '{database}'"
        ).strip()
        old_type_uuid = udt_uuid(database, "UserId")
        q(
            "GRANT USAGE TYPE ON TYPE UUID "
            f"'{database_uuid}' '{old_type_uuid}' TO {writer}"
        )
        q(
            f"CREATE TABLE {database}.writer_ok "
            f"(id {database}.UserId) ENGINE = Memory",
            user=writer,
        )
        q(f"INSERT INTO {database}.writer_ok VALUES (7)", user=writer)
        q(f"ALTER TYPE {database}.UserId RENAME TO PrincipalId")
        assert udt_uuid(database, "PrincipalId") == old_type_uuid
        q(
            f"CREATE TABLE {database}.writer_renamed "
            f"(id {database}.PrincipalId) ENGINE = Memory",
            user=writer,
        )
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        assert udt_uuid(database, "UserId") != old_type_uuid
        recreated_denied = error(
            f"CREATE TABLE {database}.writer_recreated "
            f"(id {database}.UserId) ENGINE = Memory",
            user=writer,
        )
        assert "usage" in recreated_denied.lower()
        assert q(f"EXISTS TABLE {database}.writer_recreated").strip() == "0"
        q(
            "REVOKE USAGE TYPE ON TYPE UUID "
            f"'{database_uuid}' '{old_type_uuid}' FROM {writer}"
        )
        assert q(f"SELECT sum(id) FROM {database}.writer_ok", user=writer).strip() == "7"
        revoked_alter = error(
            f"ALTER TABLE {database}.writer_ok ADD COLUMN "
            f"denied {database}.PrincipalId",
            user=writer,
        )
        assert "usage" in revoked_alter.lower()
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table = 'writer_ok' "
            "AND name = 'denied'"
        ).strip() == "0"
        revoked = error(
            f"CREATE TABLE {database}.writer_revoked "
            f"(id {database}.PrincipalId) ENGINE = Memory",
            user=writer,
        )
        assert "usage" in revoked.lower()
        assert q(f"EXISTS TABLE {database}.writer_revoked").strip() == "0"
        hidden_physicalization = error(
            f"PHYSICALIZE TYPE REFERENCES DATABASE {database} DRY RUN",
            user=writer,
        )
        assert (
            "not enough privileges to inspect the complete physicalize type references scope"
            in hidden_physicalization.lower()
        )

        q(
            f"ALTER TABLE {database}.events ADD COLUMN "
            f"extra {database}.PrincipalId DEFAULT id"
        )
        assert q(f"SELECT sum(extra) FROM {database}.events").strip() == "6"
        q(
            f"ALTER TABLE {database}.events MODIFY COLUMN "
            f"extra Nullable({database}.PrincipalId)"
        )
        q(f"ALTER TABLE {database}.events DROP COLUMN extra")
        restrict = error(f"DROP TYPE {database}.PrincipalId RESTRICT")
        assert "dependent" in restrict.lower() or "refer" in restrict.lower()

        restart_server()
        assert q(f"SELECT sum(id) FROM {database}.events").strip() == "6"
        assert f"{database}.PrincipalId" in q(
            f"SHOW CREATE TABLE {database}.events"
        )
        assert rows_json(
            "SELECT udt_declared_type FROM system.columns "
            f"WHERE database = '{database}' AND table = 'events' AND name = 'boxed'"
        )[0]["udt_declared_type"] == f"{database}.Box(Nullable({database}.PrincipalId))"
        restart_restrict = error(f"DROP TYPE {database}.PrincipalId RESTRICT")
        assert (
            "dependent" in restart_restrict.lower()
            or "refer" in restart_restrict.lower()
        )

        # Memory is intentionally empty after restart; refill it so the
        # physicalization step also proves that this engine's live data is untouched.
        q(
            f"INSERT INTO {database}.memory VALUES "
            "(4, [4, 40], tuple(4, 44), tuple(400))"
        )

        data_before = native_sha256(select_all.format(database, "events"))
        memory_before = native_sha256(select_all.format(database, "memory"))
        parts_before = q(
            "SELECT name, hash_of_all_files, hash_of_uncompressed_files "
            f"FROM system.parts WHERE database = '{database}' "
            "AND table = 'events' AND active ORDER BY name FORMAT TSV"
        )
        wrong = error(
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN 'not-a-real-udt-token'"
        )
        assert "token" in wrong.lower()

        stale_plan = dry_run(database)
        wrong_principal = error(
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(stale_plan["apply_token"]),
            user=writer,
        )
        assert "token" in wrong_principal.lower()
        q(f"ALTER TYPE {database}.PrincipalId COMMENT 'bumps the authority epoch'")
        stale = error(
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(stale_plan["apply_token"])
        )
        assert "anchored to an obsolete authority root" in stale.lower(), stale

        plan = dry_run(database)
        for mapped_table in ("memory", "events", "writer_ok", "writer_renamed"):
            assert mapped_table in plan["loss_summary"]
        definitions_before = type_identities(database)
        assert set(definitions_before) == {"Box", "PrincipalId", "UserId"}
        q(
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(plan["apply_token"])
        )
        replay = error(
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(plan["apply_token"])
        )
        assert "token" in replay.lower() or "replay" in replay.lower()
        assert native_sha256(select_all.format(database, "events")) == data_before
        assert native_sha256(select_all.format(database, "memory")) == memory_before
        assert q(
            "SELECT name, hash_of_all_files, hash_of_uncompressed_files "
            f"FROM system.parts WHERE database = '{database}' "
            "AND table = 'events' AND active ORDER BY name FORMAT TSV"
        ) == parts_before
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' "
            "AND (udt_declared_type != '' OR notEmpty(udt_references))"
        ).strip() == "0"
        physical_show_create = q(f"SHOW CREATE TABLE {database}.events")
        assert f"{database}.PrincipalId" not in physical_show_create
        assert f"{database}.Box" not in physical_show_create
        assert "Array(UInt64)" in physical_show_create
        assert type_identities(database) == definitions_before

        node.replace_in_config(CONFIG, SETTING_ON, SETTING_OFF)
        config_disabled = True
        restart_server()
        assert q(
            f"SELECT sum(id), sum(arraySum(ids)) FROM {database}.events",
            settings={},
        ).strip() == "6\t66"
        assert q(
            f"SELECT count() FROM {database}.memory", settings={}
        ).strip() == "0"
        assert f"{database}.PrincipalId" not in q(
            f"SHOW CREATE TABLE {database}.events", settings={}
        )
        assert type_identities(database, settings={}) == definitions_before
        disabled_again = error(
            f"CREATE TABLE {database}.disabled_after_restart "
            f"(id {database}.PrincipalId) ENGINE = Memory",
            settings={},
        )
        assert "disabled" in disabled_again.lower()
    finally:
        if config_disabled:
            node.replace_in_config(CONFIG, SETTING_OFF, SETTING_ON)
            restart_server()
        q(f"DROP DATABASE IF EXISTS {database} SYNC")
        q(f"DROP USER IF EXISTS {writer}")


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


def test_physicalization_scope_restart_and_drop_unused_types(
    started_cluster, restart_server
):
    suffix = uuid.uuid4().hex[:8]
    database = f"udt_token_scope_{suffix}"

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(f"CREATE TYPE {database}.Spare AS String")
        for table in ("one", "two"):
            q(
                f"CREATE TABLE {database}.{table} "
                f"(id {database}.UserId DEFAULT 0 CODEC(Delta, ZSTD(1)), "
                "value String DEFAULT 'missing' CODEC(ZSTD(1)), "
                "event_time DateTime DEFAULT toDateTime(0)) "
                "ENGINE = MergeTree ORDER BY id "
                "TTL event_time + INTERVAL 100 YEAR"
            )
            q(
                f"INSERT INTO {database}.{table} (id, value) VALUES "
                f"(1, '{table}-first'), (2, '{table}-second')"
            )

        table_uuids = {
            table: q(
                "SELECT toString(uuid) FROM system.tables "
                f"WHERE database = '{database}' AND name = '{table}'"
            ).strip()
            for table in ("one", "two")
        }
        select_all = "SELECT id, value FROM {}.{} ORDER BY id"
        data_before = {
            table: native_sha256(select_all.format(database, table))
            for table in ("one", "two")
        }
        parts_sql = (
            "SELECT table, name, hash_of_all_files, hash_of_uncompressed_files "
            f"FROM system.parts WHERE database = '{database}' AND active "
            "ORDER BY table, name FORMAT TSV"
        )
        parts_before = q(parts_sql)
        column_metadata_sql = (
            "SELECT name, default_kind, default_expression, compression_codec "
            f"FROM system.columns WHERE database = '{database}' AND table = 'one' "
            "AND name IN ('id', 'value', 'event_time') ORDER BY position FORMAT TSV"
        )
        column_metadata_before = q(column_metadata_sql)

        def assert_surrounding_metadata_survives():
            assert q(column_metadata_sql) == column_metadata_before
            show_create = q(f"SHOW CREATE TABLE {database}.one")
            for fragment in (
                "DEFAULT 0",
                "CODEC(Delta",
                "ZSTD(1)",
                "toIntervalYear(100)",
            ):
                assert fragment in show_create

        assert_surrounding_metadata_survives()

        old_plan = physicalization_dry_run(f"OBJECT TABLE {database}.one")
        assert old_plan["scope_count"] == 1
        assert "TABLE `one`" in old_plan["loss_summary"]
        assert "TABLE `two`" not in old_plan["loss_summary"]
        old_apply = (
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(old_plan["apply_token"])
        )

        restart_server()
        expired = error(old_apply)
        assert "token" in expired.lower()
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table IN ('one', 'two') "
            "AND udt_declared_type != ''"
        ).strip() == "2"

        object_plan = physicalization_dry_run(f"OBJECT TABLE {database}.one")
        object_apply = (
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(object_plan["apply_token"])
        )
        q(object_apply)
        assert_surrounding_metadata_survives()
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table = 'one' "
            "AND (udt_declared_type != '' OR notEmpty(udt_references))"
        ).strip() == "0"
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table = 'two' "
            "AND udt_declared_type != ''"
        ).strip() == "1"
        replay = error(object_apply)
        assert "token" in replay.lower() or "replay" in replay.lower()

        q(
            f"ALTER TABLE {database}.one ADD COLUMN "
            f"rebound {database}.UserId DEFAULT id"
        )
        assert q(
            "SELECT toString(uuid) FROM system.tables "
            f"WHERE database = '{database}' AND name = 'one'"
        ).strip() == table_uuids["one"]
        assert rows_json(
            "SELECT type, udt_declared_type FROM system.columns "
            f"WHERE database = '{database}' AND table = 'one' "
            "AND name = 'rebound'"
        ) == [{"type": "UInt64", "udt_declared_type": f"{database}.UserId"}]
        restrict = error(f"DROP TYPE {database}.UserId RESTRICT")
        assert "dependent" in restrict.lower() or "refer" in restrict.lower()

        restart_server()
        assert_surrounding_metadata_survives()
        assert q(
            "SELECT toString(uuid) FROM system.tables "
            f"WHERE database = '{database}' AND name = 'one'"
        ).strip() == table_uuids["one"]
        assert q(f"SELECT sum(rebound) FROM {database}.one").strip() == "3"
        assert rows_json(
            "SELECT type, udt_declared_type FROM system.columns "
            f"WHERE database = '{database}' AND table = 'one' "
            "AND name = 'rebound'"
        ) == [{"type": "UInt64", "udt_declared_type": f"{database}.UserId"}]

        database_plan = physicalization_dry_run(
            f"DATABASE {database} DROP UNUSED TYPES"
        )
        assert "TABLE `one`" in database_plan["loss_summary"]
        assert "TABLE `two`" in database_plan["loss_summary"]
        assert "UserId" in database_plan["loss_summary"]
        assert "Spare" in database_plan["loss_summary"]
        database_apply = (
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(database_plan["apply_token"])
        )
        q(database_apply)
        assert_surrounding_metadata_survives()
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' "
            "AND (udt_declared_type != '' OR notEmpty(udt_references))"
        ).strip() == "0"
        assert q(
            "SELECT count() FROM system.user_defined_types "
            f"WHERE database = '{database}'"
        ).strip() == "0"
        assert q(parts_sql) == parts_before
        for table in ("one", "two"):
            assert native_sha256(
                select_all.format(database, table)
            ) == data_before[table]
        replay = error(database_apply)
        assert "token" in replay.lower() or "replay" in replay.lower()
    finally:
        q(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_drop_unused_types_is_selective_for_shared_exclusive_and_standalone_types(
    started_cluster,
):
    suffix = uuid.uuid4().hex[:8]
    database = f"udt_selective_drop_{suffix}"
    manifest_uuid = str(uuid.UUID(bytes=b"\xc2\x85" + uuid.uuid4().bytes[2:]))

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.Shared AS UInt64")
        q(f"CREATE TYPE {database}.Exclusive AS String")
        q(f"CREATE TYPE {database}.Standalone AS UInt8")
        q(
            f"CREATE TABLE {database}.selected UUID '{manifest_uuid}' "
            f"(id {database}.Shared, payload {database}.Exclusive) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(
            f"CREATE TABLE {database}.retained "
            f"(id {database}.Shared) ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.selected VALUES (1, 'selected')")
        q(f"INSERT INTO {database}.retained VALUES (2)")

        object_plan = physicalization_dry_run(
            f"OBJECT TABLE {database}.selected DROP UNUSED TYPES"
        )
        assert b"\xc2\x85" in base64.b64decode(
            object_plan["canonical_loss_manifest_base64"], validate=True
        )
        assert "action=DROP" in loss_summary_line(object_plan, ".Exclusive")
        assert "action=RETAIN" in loss_summary_line(object_plan, ".Shared")
        assert "Standalone" not in object_plan["loss_summary"]
        q(
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(object_plan["apply_token"])
        )

        assert set(type_identities(database)) == {"Shared", "Standalone"}
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table = 'selected' "
            "AND (udt_declared_type != '' OR notEmpty(udt_references))"
        ).strip() == "0"
        assert rows_json(
            "SELECT type, udt_declared_type FROM system.columns "
            f"WHERE database = '{database}' AND table = 'retained' AND name = 'id'"
        ) == [{"type": "UInt64", "udt_declared_type": f"{database}.Shared"}]
        assert q(f"SELECT sum(id) FROM {database}.selected").strip() == "1"
        assert q(f"SELECT sum(id) FROM {database}.retained").strip() == "2"

        # Database scope must also inventory definitions which no selected
        # descriptor reaches. This makes Standalone visible to DROP UNUSED.
        database_plan = physicalization_dry_run(
            f"DATABASE {database} DROP UNUSED TYPES"
        )
        assert "action=DROP" in loss_summary_line(database_plan, ".Shared")
        assert "action=DROP" in loss_summary_line(database_plan, ".Standalone")
        q(
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(database_plan["apply_token"])
        )
        assert type_identities(database) == {}
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' "
            "AND (udt_declared_type != '' OR notEmpty(udt_references))"
        ).strip() == "0"
        assert q(f"SELECT sum(id) FROM {database}.selected").strip() == "1"
        assert q(f"SELECT sum(id) FROM {database}.retained").strip() == "2"

        # A definition-only loss transaction is still a useful database-scope
        # operation: there are no mapped objects left to rewrite, but every
        # independently inventoried unused definition must be reviewed and
        # removed atomically.
        q(f"CREATE TYPE {database}.OrphanA AS UInt16")
        q(f"CREATE TYPE {database}.OrphanB AS String")
        definition_only_plan = physicalization_dry_run(
            f"DATABASE {database} DROP UNUSED TYPES"
        )
        assert definition_only_plan["scope_count"] == 0
        for type_name in ("OrphanA", "OrphanB"):
            assert "action=DROP" in loss_summary_line(
                definition_only_plan, f".{type_name}"
            )
        q(
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(definition_only_plan["apply_token"])
        )
        assert type_identities(database) == {}
        assert q(f"SELECT sum(id) FROM {database}.selected").strip() == "1"
        assert q(f"SELECT sum(id) FROM {database}.retained").strip() == "2"
    finally:
        q(f"DROP DATABASE IF EXISTS {database} SYNC")


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


def test_definition_chain_and_diamond_are_physicalized_through_real_closure_scope(
    started_cluster,
    restart_server,
):
    suffix = uuid.uuid4().hex[:8]
    database = f"udt_definition_diamond_{suffix}"

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.Base AS UInt64")
        q(f"CREATE TYPE {database}.Left AS Array({database}.Base)")
        q(f"CREATE TYPE {database}.Right AS Tuple(value {database}.Base)")
        q(
            f"CREATE TYPE {database}.Diamond AS "
            f"Tuple(left {database}.Left, right {database}.Right)"
        )
        definition_uuids = type_identities(database)
        q(
            f"CREATE TABLE {database}.root "
            f"(key UInt8, payload {database}.Diamond) "
            "ENGINE = MergeTree ORDER BY key"
        )
        q(f"INSERT INTO {database}.root VALUES (1, tuple([10, 20], tuple(30)))")

        plan = physicalization_dry_run(
            f"CLOSURE OF TABLE {database}.root"
        )
        assert plan["scope_count"] == 1
        for type_name in ("Base", "Left", "Right", "Diamond"):
            assert "action=RETAIN" in loss_summary_line(plan, f".{type_name}")
        q(
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(plan["apply_token"])
        )

        assert type_identities(database) == definition_uuids
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table = 'root' "
            "AND (udt_declared_type != '' OR notEmpty(udt_references))"
        ).strip() == "0"
        [physical_column] = rows_json(
            "SELECT type FROM system.columns "
            f"WHERE database = '{database}' AND table = 'root' AND name = 'payload'"
        )
        assert normalize_type_whitespace(physical_column["type"]) == (
            "Tuple(left Array(UInt64), right Tuple(value UInt64))"
        )
        restart_server()
        assert q(
            f"SELECT arraySum(payload.left), payload.right.value "
            f"FROM {database}.root FORMAT TSV"
        ) == "30\t30\n"
        assert type_identities(database) == definition_uuids
    finally:
        q(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_mergetree_numbers_workload_preserves_results_parts_and_prewhere_after_physicalization(
    started_cluster,
    restart_server,
):
    suffix = uuid.uuid4().hex[:8]
    database = f"udt_numbers_workload_{suffix}"
    merges_stopped = False
    insert_batches = 8
    rows_per_batch = 1024
    total_rows = insert_batches * rows_per_batch

    workload_query = f"""
        SELECT
            filtered.bucket,
            count() AS rows,
            sum(filtered.score * dimensions.weight) AS weighted_score,
            sum(arraySum(filtered.tags)) AS tag_sum,
            uniqExact(filtered.owner.id) AS owners
        FROM
        (
            SELECT id, bucket, score, tags, owner
            FROM {database}.events
            PREWHERE bucket IN (1, 2, 3)
            WHERE id % 5 != 0
        ) AS filtered
        INNER JOIN {database}.dimensions AS dimensions USING (id)
        WHERE dimensions.enabled
        GROUP BY filtered.bucket
        ORDER BY filtered.bucket
    """
    ordered_rows_query = f"SELECT * FROM {database}.events ORDER BY id"

    def assert_prewhere_plan():
        explain = q(
            f"EXPLAIN actions = 1 SELECT count() FROM {database}.events "
            "PREWHERE bucket IN (1, 2, 3) WHERE id % 5 != 0"
        )
        assert "Prewhere" in explain

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(f"CREATE TYPE {database}.Score AS Int64")
        q(
            f"CREATE TABLE {database}.events "
            f"(id {database}.UserId, bucket UInt8, score {database}.Score, "
            f"tags Array(UInt16), owner Tuple(id {database}.UserId, label String)) "
            "ENGINE = MergeTree PARTITION BY bucket ORDER BY (bucket, id) "
            "SETTINGS index_granularity = 128"
        )
        q(
            f"CREATE TABLE {database}.dimensions "
            "(id UInt64, weight Int16, enabled UInt8) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(
            f"INSERT INTO {database}.dimensions "
            f"SELECT number, toInt16(toInt64(number % 17) - 8), number % 3 != 0 "
            f"FROM numbers({total_rows})"
        )

        q(f"SYSTEM STOP MERGES {database}.events")
        merges_stopped = True
        for batch in range(insert_batches):
            offset = batch * rows_per_batch
            q(
                f"INSERT INTO {database}.events SELECT "
                f"number + {offset} AS id, "
                "toUInt8(id % 4) AS bucket, "
                "toInt64(id % 97) - 48 AS score, "
                "[toUInt16(id % 11), toUInt16((id * 7) % 13)] AS tags, "
                "tuple(id, concat('owner-', toString(id % 29))) AS owner "
                f"FROM numbers({rows_per_batch})"
            )

        active_parts_before_merge = int(
            q(
                "SELECT count() FROM system.parts "
                f"WHERE database = '{database}' AND table = 'events' AND active"
            ).strip()
        )
        assert active_parts_before_merge >= insert_batches
        assert q(f"SELECT count() FROM {database}.events").strip() == str(total_rows)

        q(f"SYSTEM START MERGES {database}.events")
        merges_stopped = False
        q(f"OPTIMIZE TABLE {database}.events FINAL")
        active_parts_after_merge = int(
            q(
                "SELECT count() FROM system.parts "
                f"WHERE database = '{database}' AND table = 'events' AND active"
            ).strip()
        )
        assert active_parts_after_merge < active_parts_before_merge
        assert active_parts_after_merge <= 4

        assert_prewhere_plan()

        expected_workload = q(workload_query + " FORMAT TSV")
        assert len(expected_workload.splitlines()) == 3
        rows_digest = native_sha256(ordered_rows_query)
        parts_snapshot = q(
            "SELECT partition, name, rows, hash_of_all_files, "
            "hash_of_uncompressed_files FROM system.parts "
            f"WHERE database = '{database}' AND table = 'events' AND active "
            "ORDER BY partition, name FORMAT TSV"
        )

        plan = physicalization_dry_run(f"OBJECT TABLE {database}.events")
        assert plan["scope_count"] == 1
        assert "events" in plan["loss_summary"]
        q(
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(plan["apply_token"])
        )

        assert q(workload_query + " FORMAT TSV") == expected_workload
        assert_prewhere_plan()
        assert native_sha256(ordered_rows_query) == rows_digest
        assert q(
            "SELECT partition, name, rows, hash_of_all_files, "
            "hash_of_uncompressed_files FROM system.parts "
            f"WHERE database = '{database}' AND table = 'events' AND active "
            "ORDER BY partition, name FORMAT TSV"
        ) == parts_snapshot
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table = 'events' "
            "AND (udt_declared_type != '' OR notEmpty(udt_references))"
        ).strip() == "0"
        physical_create = q(f"SHOW CREATE TABLE {database}.events")
        assert f"{database}.UserId" not in physical_create
        assert f"{database}.Score" not in physical_create

        restart_server()
        assert q(workload_query + " FORMAT TSV") == expected_workload
        assert_prewhere_plan()
        assert native_sha256(ordered_rows_query) == rows_digest
        assert q(
            "SELECT partition, name, rows, hash_of_all_files, "
            "hash_of_uncompressed_files FROM system.parts "
            f"WHERE database = '{database}' AND table = 'events' AND active "
            "ORDER BY partition, name FORMAT TSV"
        ) == parts_snapshot
        assert q(f"SELECT count() FROM {database}.events").strip() == str(total_rows)
    finally:
        if merges_stopped:
            try:
                q(f"SYSTEM START MERGES {database}.events")
            except Exception:
                pass
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


def test_physicalization_apply_rechecks_alter_drop_and_not_usage_type(
    started_cluster,
):
    suffix = uuid.uuid4().hex[:8]
    database = f"udt_apply_auth_{suffix}"
    executor = f"udt_physicalizer_{suffix}"

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.VisibleId AS UInt64")
        q(
            f"CREATE TABLE {database}.visible_rows "
            f"(id {database}.VisibleId) ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.visible_rows VALUES (1), (2)")
        q(f"CREATE USER {executor} IDENTIFIED WITH no_password")
        q(f"GRANT SHOW TYPES ON {database}.* TO {executor}")
        q(
            f"GRANT SHOW TABLES, SHOW COLUMNS, ALTER TABLE "
            f"ON {database}.* TO {executor}"
        )
        assert "USAGE TYPE" not in q(f"SHOW GRANTS FOR {executor}")

        object_plan = physicalization_dry_run(
            f"OBJECT TABLE {database}.visible_rows",
            user=executor,
        )
        object_apply = (
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(object_plan["apply_token"])
        )
        q(f"REVOKE ALTER TABLE ON {database}.* FROM {executor}")
        denied = error(object_apply, user=executor)
        assert "privilege" in denied.lower() or "access" in denied.lower()
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table = 'visible_rows' "
            "AND udt_declared_type != ''"
        ).strip() == "1"

        # Authorization failed before token consumption. Restoring ALTER lets
        # the same principal apply the same plan without any USAGE TYPE grant.
        q(f"GRANT ALTER TABLE ON {database}.* TO {executor}")
        q(object_apply, user=executor)
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table = 'visible_rows' "
            "AND (udt_declared_type != '' OR notEmpty(udt_references))"
        ).strip() == "0"
        assert q(f"SELECT sum(id) FROM {database}.visible_rows").strip() == "3"

        q(f"CREATE TYPE {database}.DropCandidate AS UInt32")
        q(f"CREATE TYPE {database}.Standalone AS String")
        q(
            f"CREATE TABLE {database}.drop_rows "
            f"(id {database}.DropCandidate) ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.drop_rows VALUES (11)")
        definitions_before = type_identities(database)
        drop_plan = physicalization_dry_run(
            f"DATABASE {database} DROP UNUSED TYPES",
            user=executor,
        )
        for type_name in ("VisibleId", "DropCandidate", "Standalone"):
            assert "action=DROP" in loss_summary_line(drop_plan, f".{type_name}")
        drop_apply = (
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(drop_plan["apply_token"])
        )
        denied = error(drop_apply, user=executor)
        assert "privilege" in denied.lower() or "access" in denied.lower()
        assert type_identities(database) == definitions_before
        assert rows_json(
            "SELECT type, udt_declared_type FROM system.columns "
            f"WHERE database = '{database}' AND table = 'drop_rows' AND name = 'id'"
        ) == [
            {
                "type": "UInt32",
                "udt_declared_type": f"{database}.DropCandidate",
            }
        ]

        q(f"GRANT DROP TYPE ON {database}.* TO {executor}")
        q(drop_apply, user=executor)
        assert type_identities(database) == {}
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' "
            "AND (udt_declared_type != '' OR notEmpty(udt_references))"
        ).strip() == "0"
        assert q(f"SELECT sum(id) FROM {database}.drop_rows").strip() == "11"

        # Definition-only DROP UNUSED has no table identity on which to
        # preflight ALTER. It must recheck database-wide DROP TYPE before
        # reconciling hidden durable records, and an authorization failure must
        # leave the principal-bound token reusable after the grant is restored.
        q(f"CREATE TYPE {database}.OrphanDefinition AS UInt16")
        definition_only_plan = physicalization_dry_run(
            f"DATABASE {database} DROP UNUSED TYPES",
            user=executor,
        )
        assert definition_only_plan["scope_count"] == 0
        assert "action=DROP" in loss_summary_line(
            definition_only_plan, ".OrphanDefinition"
        )
        definition_only_apply = (
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(definition_only_plan["apply_token"])
        )
        q(f"REVOKE DROP TYPE ON {database}.* FROM {executor}")
        denied = error(definition_only_apply, user=executor)
        assert "privilege" in denied.lower() or "access" in denied.lower()
        assert set(type_identities(database)) == {"OrphanDefinition"}

        q(f"GRANT DROP TYPE ON {database}.* TO {executor}")
        q(definition_only_apply, user=executor)
        assert type_identities(database) == {}
    finally:
        q(f"DROP DATABASE IF EXISTS {database} SYNC")
        q(f"DROP USER IF EXISTS {executor}")


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
