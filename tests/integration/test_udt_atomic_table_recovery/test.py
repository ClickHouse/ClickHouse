"""Real-server crash recovery and fail-closed durable-state coverage for UDT tables."""

import base64
import json
import os
import sys
import threading
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
TABLE = "events"
MAX_TEST_ARTIFACT_BYTES = 1 << 20
SIDECAR_PHYSICAL_FINGERPRINT_OFFSET = 2 + 1 + 16 + 16 + 8
SIDECAR_HEADER_BYTES = SIDECAR_PHYSICAL_FINGERPRINT_OFFSET + 32 + 2


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown(ignore_fatal=True)


def query(sql, *, timeout=None):
    kwargs = {"settings": ENABLED}
    if timeout is not None:
        kwargs["timeout"] = timeout
    return node.query(sql, **kwargs)


def query_error(sql):
    result = node.query_and_get_error(sql, settings=ENABLED)
    assert result, sql
    return result


def sql_string(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def unique_database(label):
    return f"udt_atomic_recovery_{label}_{uuid.uuid4().hex[:8]}"


def create_mapped_table(database):
    query(f"CREATE DATABASE {database} ENGINE = Atomic")
    query(f"CREATE TYPE {database}.UserId AS UInt64")
    query(
        f"CREATE TABLE {database}.{TABLE} "
        f"(id {database}.UserId, ids Array({database}.UserId), note String) "
        "ENGINE = MergeTree ORDER BY id"
    )
    query(
        f"INSERT INTO {database}.{TABLE} VALUES "
        "(1, [2, 3], 'first'), (2, [4, 5], 'second')"
    )
    assert_mapped_table(database)


def activate_mapped_table_capabilities(database):
    warmup_table = "capability_warmup"
    query(
        f"CREATE TABLE {database}.{warmup_table} (id {database}.UserId) "
        "ENGINE = Memory"
    )
    query(f"DROP TABLE {database}.{warmup_table} SYNC")


def assert_mapped_table(database, expected_mapped_columns=2):
    assert query(
        f"SELECT sum(id), sum(arraySum(ids)), arraySort(groupArray(note)) "
        f"FROM {database}.{TABLE}"
    ).strip() == "3\t14\t['first','second']"
    assert query(
        "SELECT count() FROM system.columns "
        f"WHERE database = '{database}' AND table = '{TABLE}' "
        "AND notEmpty(udt_references)"
    ).strip() == str(expected_mapped_columns)
    show_create = query(f"SHOW CREATE TABLE {database}.{TABLE}")
    assert f"{database}.UserId" in show_create
    assert f"Array({database}.UserId)" in show_create


def assert_physical_table(database):
    assert query(
        f"SELECT sum(id), sum(arraySum(ids)), arraySort(groupArray(note)) "
        f"FROM {database}.{TABLE}"
    ).strip() == "3\t14\t['first','second']"
    assert query(
        "SELECT count() FROM system.columns "
        f"WHERE database = '{database}' AND table = '{TABLE}' "
        "AND (udt_declared_type != '' OR notEmpty(udt_references))"
    ).strip() == "0"
    assert query(
        "SELECT name, type FROM system.columns "
        f"WHERE database = '{database}' AND table = '{TABLE}' "
        "ORDER BY position FORMAT TSV"
    ) == "id\tUInt64\nids\tArray(UInt64)\nnote\tString\n"
    show_create = query(f"SHOW CREATE TABLE {database}.{TABLE}")
    assert f"{database}.UserId" not in show_create
    assert "Array(UInt64)" in show_create


def active_parts_snapshot(database):
    return query(
        "SELECT name, hash_of_all_files, hash_of_uncompressed_files "
        "FROM system.parts "
        f"WHERE database = '{database}' AND table = '{TABLE}' AND active "
        "ORDER BY name FORMAT TSV"
    )


def durable_artifact_paths(database):
    default_disk_root = query(
        "SELECT path FROM system.disks WHERE name = 'default'"
    ).strip()
    assert os.path.isabs(default_disk_root), default_disk_root

    def container_path(disk_path):
        result = (
            os.path.normpath(disk_path)
            if os.path.isabs(disk_path)
            else os.path.normpath(os.path.join(default_disk_root, disk_path))
        )
        assert os.path.commonpath([default_disk_root, result]) == os.path.normpath(
            default_disk_root
        )
        return result

    database_row = query(
        "SELECT metadata_path, toString(uuid) FROM system.databases "
        f"WHERE name = '{database}' FORMAT TSV"
    ).strip()
    metadata_root, database_uuid = database_row.split("\t")
    table_row = query(
        "SELECT metadata_path, toString(uuid) FROM system.tables "
        f"WHERE database = '{database}' AND name = '{TABLE}' FORMAT TSV"
    ).strip()
    table_metadata, table_uuid = table_row.split("\t")
    expectations = os.path.join(
        metadata_root.rstrip("/"),
        "types",
        ".authority",
        "databases",
        database_uuid,
        "expectations",
    )
    paths = {
        "references": container_path(
            os.path.join(expectations, f"{table_uuid}.references")
        ),
        "expectation": container_path(
            os.path.join(expectations, f"{table_uuid}.bin")
        ),
        "installation": container_path(
            os.path.join(expectations, f"{table_uuid}.installation")
        ),
        "metadata": container_path(table_metadata),
    }
    assert all(node.file_exists_in_container(path) for path in paths.values()), paths
    return paths


def definition_artifact_path(database, type_name):
    default_disk_root = query(
        "SELECT path FROM system.disks WHERE name = 'default'"
    ).strip()
    metadata_root = query(
        "SELECT metadata_path FROM system.databases "
        f"WHERE name = '{database}'"
    ).strip()
    if not os.path.isabs(metadata_root):
        metadata_root = os.path.join(default_disk_root, metadata_root)
    metadata_root = os.path.normpath(metadata_root)
    assert os.path.commonpath([os.path.normpath(default_disk_root), metadata_root]) == (
        os.path.normpath(default_disk_root)
    )
    definition_uuid = query(
        "SELECT toString(uuid) FROM system.user_defined_types "
        f"WHERE database = '{database}' AND name = '{type_name}'"
    ).strip()
    assert definition_uuid
    result = os.path.join(metadata_root, "types", f"{definition_uuid}.sql")
    assert node.file_exists_in_container(result), result
    return result


def read_container_file(path):
    encoded = node.exec_in_container(["base64", path], user="root")
    result = base64.b64decode(encoded, validate=False)
    assert 0 < len(result) <= MAX_TEST_ARTIFACT_BYTES, (path, len(result))
    return result


def write_container_file(path, contents):
    assert len(contents) <= MAX_TEST_ARTIFACT_BYTES
    encoded = base64.b64encode(contents).decode("ascii")
    node.exec_in_container(
        [
            "bash",
            "-c",
            "printf '%s' \"$1\" | base64 --decode > \"$2\"",
            "udt-recovery-write",
            encoded,
            path,
        ],
        user="root",
    )


def read_var_uint(payload, position, end):
    start = position
    value = 0
    shift = 0
    for _ in range(10):
        assert position < end, "truncated sidecar VarUInt"
        byte = payload[position]
        position += 1
        if shift == 63:
            assert byte & 0xFE == 0, "overflowing sidecar VarUInt"
        value |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            assert position - start == 1 or byte != 0, "non-minimal sidecar VarUInt"
            return value, position, start
        shift += 7
        assert shift <= 63, "overflowing sidecar VarUInt"
    raise AssertionError("overlong sidecar VarUInt")


def first_nested_occurrence_child_offset(payload):
    """Locate a child ordinal through the bounded canonical V1 frame layout."""
    assert len(payload) <= MAX_TEST_ARTIFACT_BYTES
    assert len(payload) >= SIDECAR_HEADER_BYTES
    assert payload[0:2] == b"\x01\x00"
    assert payload[SIDECAR_HEADER_BYTES - 2 : SIDECAR_HEADER_BYTES] == b"\x01\x00"

    position = SIDECAR_HEADER_BYTES
    descriptor_count, position, _ = read_var_uint(payload, position, len(payload))
    assert 0 < descriptor_count <= 65_536
    for _ in range(descriptor_count):
        frame_size, position, _ = read_var_uint(payload, position, len(payload))
        assert frame_size <= MAX_TEST_ARTIFACT_BYTES
        position += frame_size
        assert position <= len(payload), "truncated sidecar descriptor frame"

    path_count, position, _ = read_var_uint(payload, position, len(payload))
    assert 0 < path_count <= 65_536
    for _ in range(path_count):
        frame_size, position, _ = read_var_uint(payload, position, len(payload))
        assert 4 <= frame_size <= MAX_TEST_ARTIFACT_BYTES
        frame_end = position + frame_size
        assert frame_end <= len(payload), "truncated sidecar occurrence-path frame"
        position += 1  # PersistedTypePathSection.
        _, position, _ = read_var_uint(payload, position, frame_end)
        _, position, _ = read_var_uint(payload, position, frame_end)
        depth, position, _ = read_var_uint(payload, position, frame_end)
        assert depth <= 64
        for _ in range(depth):
            child, next_position, child_offset = read_var_uint(
                payload, position, frame_end
            )
            if child < 0x7F and next_position == child_offset + 1:
                return child_offset
            position = next_position
        position = frame_end
    raise AssertionError("the test table sidecar has no nested occurrence path")


def physicalization_plan(database):
    output = query(
        f"PHYSICALIZE TYPE REFERENCES OBJECT TABLE {database}.{TABLE} "
        "DRY RUN FORMAT JSONEachRow"
    )
    rows = [json.loads(line) for line in output.split("\n") if line]
    assert len(rows) == 1
    assert rows[0]["scope_count"] == 1
    assert rows[0]["manifest_count"] > 0
    assert rows[0]["apply_token"]
    assert base64.b64decode(rows[0]["canonical_loss_manifest_base64"], validate=True)
    return rows[0]


def crash_during_physicalization(database, failpoint):
    plan = physicalization_plan(database)
    apply_sql = (
        "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
        + sql_string(plan["apply_token"])
    )
    outcome = {"returned": False, "error": None}

    def apply_in_background():
        try:
            query(apply_sql, timeout=180)
            outcome["returned"] = True
        except BaseException as ex:  # noqa: BLE001 - surfaced in the main thread.
            outcome["error"] = ex

    query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    worker = threading.Thread(target=apply_in_background, daemon=True)
    worker.start()
    crashed = False
    try:
        query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)
        assert worker.is_alive(), "APPLY returned instead of pausing at the failpoint"
        node.stop_clickhouse(kill=True)
        assert node.get_process_pid("clickhouse") is None
        crashed = True
    finally:
        if not crashed and node.get_process_pid("clickhouse") is not None:
            try:
                query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")
            except Exception:
                pass
            try:
                query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
            except Exception:
                pass
        worker.join(timeout=60)
        if crashed:
            node.start_clickhouse()

    assert crashed
    assert not worker.is_alive(), "APPLY client did not observe the killed server"
    assert not outcome["returned"], "APPLY unexpectedly returned across SIGKILL"
    assert outcome["error"] is not None
    return plan


def crash_query_at_schema_wal_failpoint(sql, failpoint):
    outcome = {"returned": False, "error": None}

    def run_query():
        try:
            query(sql, timeout=120)
            outcome["returned"] = True
        except BaseException as ex:  # noqa: BLE001 - asserted by the parent.
            outcome["error"] = ex

    query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    worker = threading.Thread(target=run_query, daemon=True)
    worker.start()
    crashed = False
    try:
        query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)
        assert worker.is_alive(), f"query returned before pausing at {failpoint}"
        node.stop_clickhouse(kill=True)
        assert node.get_process_pid("clickhouse") is None
        crashed = True
    finally:
        if not crashed and node.get_process_pid("clickhouse") is not None:
            try:
                query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")
            except Exception:
                pass
            try:
                query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
            except Exception:
                pass
        worker.join(timeout=60)
        if crashed:
            node.start_clickhouse()

    assert crashed
    assert not worker.is_alive(), "DDL client did not observe the killed server"
    assert not outcome["returned"], "DDL unexpectedly returned across SIGKILL"
    assert outcome["error"] is not None


@pytest.mark.parametrize(
    ("failpoint", "expect_mapped"),
    [
        ("database_schema_mutation_pause_after_prepare", True),
        ("database_schema_mutation_pause_after_first_artifact_action", True),
        ("database_schema_mutation_pause_after_installation_barrier", True),
        ("database_schema_mutation_pause_after_commit", False),
    ],
    ids=[
        "rollback-after-prepare",
        "rollback-partial-after-image",
        "rollback-installed-before-commit",
        "complete-after-commit",
    ],
)
def test_physicalization_recovers_across_sigkill(started_cluster, failpoint, expect_mapped):
    database = unique_database("wal")
    test_failed = False
    try:
        create_mapped_table(database)
        paths = durable_artifact_paths(database)
        before = {name: read_container_file(path) for name, path in paths.items()}
        parts_before = active_parts_snapshot(database)

        crashed_plan = crash_during_physicalization(database, failpoint)

        def assert_recovered_outcome():
            if expect_mapped:
                assert_mapped_table(database)
                assert {
                    name: read_container_file(path) for name, path in paths.items()
                } == before
            else:
                assert_physical_table(database)
                assert node.file_exists_in_container(paths["metadata"])
                # Ordinary Atomic metadata is already the canonical physical-only
                # image; Commit removes provenance without rewriting those bytes.
                assert read_container_file(paths["metadata"]) == before["metadata"]
                for name in ("references", "expectation", "installation"):
                    assert not node.file_exists_in_container(paths[name]), name
            assert active_parts_snapshot(database) == parts_before
            # Physicalization without DROP UNUSED TYPES must retain the definition.
            assert query(
                "SELECT count() FROM system.user_defined_types "
                f"WHERE database = '{database}' AND name = 'UserId'"
            ).strip() == "1"

        assert_recovered_outcome()

        # APPLY consumes the token before entering the durable transition. A
        # process crash therefore makes that token unusable regardless of which
        # side of Commit recovery selected.
        replay_error = query_error(
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(crashed_plan["apply_token"])
        )
        assert "physicalization token was rejected" in replay_error.lower()

        # Recovery is durable, not merely a one-start in-memory publication.
        node.restart_clickhouse()
        assert_recovered_outcome()

        if expect_mapped:
            # A rolled-back prepared transaction must not wedge future schema
            # mutations. A fresh token completes the same operation normally.
            retry_plan = physicalization_plan(database)
            query(
                "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
                + sql_string(retry_plan["apply_token"])
            )
            assert_physical_table(database)
            assert active_parts_snapshot(database) == parts_before
            for name in ("references", "expectation", "installation"):
                assert not node.file_exists_in_container(paths[name]), name
            node.restart_clickhouse()
            assert_physical_table(database)
            assert active_parts_snapshot(database) == parts_before
    except BaseException:
        test_failed = True
        raise
    finally:
        try:
            if node.get_process_pid("clickhouse") is None:
                node.start_clickhouse()
            query(f"DROP DATABASE IF EXISTS {database} SYNC")
        except Exception:
            if not test_failed:
                raise


@pytest.mark.parametrize(
    ("failpoint", "expect_created"),
    [
        ("database_schema_mutation_pause_after_prepare", False),
        ("database_schema_mutation_pause_after_first_artifact_action", False),
        ("database_schema_mutation_pause_after_installation_barrier", False),
        ("database_schema_mutation_pause_after_commit", True),
    ],
    ids=[
        "create-rolls-back-after-prepare",
        "create-rolls-back-after-first-artifact",
        "create-rolls-back-after-installation",
        "create-completes-after-commit",
    ],
)
def test_mapped_table_create_recovers_across_sigkill(
    started_cluster, failpoint, expect_created
):
    database = unique_database("create_wal")
    test_failed = False
    try:
        query(f"CREATE DATABASE {database} ENGINE = Atomic")
        query(f"CREATE TYPE {database}.UserId AS UInt64")
        # The first mapped-table mutation activates dependent-object authority
        # capabilities through its own schema transaction. Warm it up before
        # enabling global WAL failpoints so they target CREATE TABLE admission.
        activate_mapped_table_capabilities(database)
        create_sql = (
            f"CREATE TABLE {database}.{TABLE} "
            f"(id {database}.UserId, ids Array({database}.UserId), note String) "
            "ENGINE = MergeTree ORDER BY id"
        )
        crash_query_at_schema_wal_failpoint(create_sql, failpoint)

        assert query(f"EXISTS TABLE {database}.{TABLE}").strip() == (
            "1" if expect_created else "0"
        )
        if not expect_created:
            # A rolled-back admission leaves no table artifact, expectation or
            # dependency edge and must not wedge the next transaction.
            query(create_sql)
        query(
            f"INSERT INTO {database}.{TABLE} VALUES "
            "(1, [2, 3], 'first'), (2, [4, 5], 'second')"
        )
        assert_mapped_table(database)
        node.restart_clickhouse()
        assert_mapped_table(database)
    except BaseException:
        test_failed = True
        raise
    finally:
        try:
            if node.get_process_pid("clickhouse") is None:
                node.start_clickhouse()
            query(f"DROP DATABASE IF EXISTS {database} SYNC")
        except Exception:
            if not test_failed:
                raise


@pytest.mark.parametrize(
    ("failpoint", "expect_altered"),
    [
        ("database_schema_mutation_pause_after_prepare", False),
        ("database_schema_mutation_pause_after_first_artifact_action", False),
        ("database_schema_mutation_pause_after_installation_barrier", False),
        ("database_schema_mutation_pause_after_commit", True),
    ],
    ids=[
        "alter-rolls-back-after-prepare",
        "alter-rolls-back-after-first-artifact",
        "alter-rolls-back-after-installation",
        "alter-completes-after-commit",
    ],
)
def test_multi_udt_alter_recovers_across_sigkill(
    started_cluster, failpoint, expect_altered
):
    database = unique_database("alter_wal")
    test_failed = False
    try:
        create_mapped_table(database)
        query(f"CREATE TYPE {database}.Code AS String")
        alter_sql = (
            f"ALTER TABLE {database}.{TABLE} "
            f"ADD COLUMN code {database}.Code DEFAULT 'restored'"
        )
        crash_query_at_schema_wal_failpoint(alter_sql, failpoint)

        assert query(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table = '{TABLE}' "
            "AND name = 'code' AND udt_declared_type != ''"
        ).strip() == ("1" if expect_altered else "0")
        if not expect_altered:
            query(alter_sql)
        assert query(
            f"SELECT arraySort(groupArray(code)) FROM {database}.{TABLE}"
        ).strip() == "['restored','restored']"
        assert_mapped_table(database, expected_mapped_columns=3)
        node.restart_clickhouse()
        assert_mapped_table(database, expected_mapped_columns=3)
        assert f"{database}.Code" in query(
            f"SHOW CREATE TABLE {database}.{TABLE}"
        )
    except BaseException:
        test_failed = True
        raise
    finally:
        try:
            if node.get_process_pid("clickhouse") is None:
                node.start_clickhouse()
            query(f"DROP DATABASE IF EXISTS {database} SYNC")
        except Exception:
            if not test_failed:
                raise


def mutated_artifact(original, mutation):
    if mutation == "truncate":
        return original[:1]
    result = bytearray(original)
    if mutation == "corrupt":
        result[-1] ^= 0x01
    elif mutation == "physical_fingerprint":
        assert len(result) > SIDECAR_PHYSICAL_FINGERPRINT_OFFSET
        result[SIDECAR_PHYSICAL_FINGERPRINT_OFFSET] ^= 0x01
    elif mutation == "occurrence_path":
        result[first_nested_occurrence_child_offset(result)] ^= 0x01
    else:
        raise AssertionError(f"unknown artifact mutation {mutation}")
    assert bytes(result) != original
    return bytes(result)


def assert_database_startup_fail_closed(database):
    row = query(
        "SELECT udt_verification_state, udt_verification_last_error, "
        "udt_verification_runtime_status_available, "
        "udt_verification_runtime_fail_closed "
        "FROM system.databases "
        f"WHERE name = '{database}' FORMAT TSV"
    ).strip()
    assert row == "FailClosed\tStartupIncomplete\t1\t1", row
    assert query(
        "SELECT count() FROM system.tables "
        f"WHERE database = '{database}' AND name = '{TABLE}'"
    ).strip() == "0"
    assert query_error(f"SELECT count() FROM {database}.{TABLE}")


def assert_startup_rejects_artifact_change(
    database, path, mutation, *, replacement=None
):
    case_id = f"{os.path.basename(path)}-{mutation}-{uuid.uuid4().hex[:8]}"
    backup = f"/tmp/udt-recovery-{case_id}.backup"

    node.stop_clickhouse()
    assert node.get_process_pid("clickhouse") is None
    node.exec_in_container(
        ["cp", "--preserve=all", "--", path, backup], user="root"
    )
    node.exec_in_container(["cmp", "--silent", "--", backup, path], user="root")
    original = read_container_file(backup)

    restored = False
    try:
        if mutation == "remove":
            assert replacement is None
            node.exec_in_container(["rm", "--", path], user="root")
        elif mutation == "replace":
            assert replacement is not None
            assert 0 < len(replacement) <= MAX_TEST_ARTIFACT_BYTES
            assert replacement != original
            write_container_file(path, replacement)
        else:
            assert replacement is None
            write_container_file(path, mutated_artifact(original, mutation))

        node.start_clickhouse()
        assert node.get_process_pid("clickhouse") is not None, case_id
        assert_database_startup_fail_closed(database)
    finally:
        if node.get_process_pid("clickhouse") is not None:
            node.stop_clickhouse()
        node.exec_in_container(
            ["cp", "--preserve=all", "--", backup, path], user="root"
        )
        node.exec_in_container(
            ["cmp", "--silent", "--", backup, path], user="root"
        )
        restored = True
        try:
            node.start_clickhouse()
            node.exec_in_container(
                ["cmp", "--silent", "--", backup, path], user="root"
            )
        finally:
            node.exec_in_container(["rm", "-f", "--", backup], user="root")

    assert restored
    assert_mapped_table(database)


def repair_provenance(database):
    row = query(
        "SELECT "
        "udt_verification_last_repair_transaction_id, "
        "udt_verification_last_repair_local_wal_sources, "
        "udt_verification_last_repair_replicated_authority_sources, "
        "udt_verification_last_repair_verified_backup_sources, "
        "udt_verification_last_repair_provenance_available, "
        "udt_verification_last_repair_damaged_artifacts, "
        "udt_verification_last_repair_manifest_digest, "
        "udt_verification_last_repair_previous_catalog_epoch, "
        "udt_verification_last_repair_previous_authority_anchor, "
        "udt_verification_last_repair_repaired_catalog_epoch, "
        "udt_verification_last_repair_repaired_authority_anchor "
        "FROM system.databases "
        f"WHERE name = '{database}' FORMAT TSV"
    ).rstrip("\n").split("\t")
    assert len(row) == 11, row
    return {
        "transaction_id": int(row[0]),
        "local_wal_sources": int(row[1]),
        "replicated_sources": int(row[2]),
        "backup_sources": int(row[3]),
        "available": int(row[4]),
        "damaged_artifacts": int(row[5]),
        "manifest_digest": row[6],
        "previous_epoch": int(row[7]),
        "previous_anchor": row[8],
        "repaired_epoch": int(row[9]),
        "repaired_anchor": row[10],
    }


def assert_local_wal_repair_provenance(database, previous_transaction_id):
    provenance = repair_provenance(database)
    assert provenance["available"] == 1, provenance
    assert provenance["transaction_id"] > previous_transaction_id, provenance
    assert provenance["local_wal_sources"] == 1, provenance
    assert provenance["replicated_sources"] == 0, provenance
    assert provenance["backup_sources"] == 0, provenance
    assert provenance["damaged_artifacts"] == 1, provenance
    assert len(provenance["manifest_digest"]) == 64, provenance
    assert provenance["previous_epoch"] > 0, provenance
    assert provenance["repaired_epoch"] == provenance["previous_epoch"] + 1, provenance
    assert len(provenance["previous_anchor"]) == 64, provenance
    assert len(provenance["repaired_anchor"]) == 64, provenance
    return provenance["transaction_id"]


def assert_startup_repairs_artifact_from_local_wal(
    database, path, mutation, *, replacement=None
):
    case_id = f"{os.path.basename(path)}-{mutation}-{uuid.uuid4().hex[:8]}"
    backup = f"/tmp/udt-recovery-{case_id}.backup"
    previous_transaction_id = repair_provenance(database)["transaction_id"]

    node.stop_clickhouse()
    assert node.get_process_pid("clickhouse") is None
    node.exec_in_container(
        ["cp", "--preserve=all", "--", path, backup], user="root"
    )
    node.exec_in_container(["cmp", "--silent", "--", backup, path], user="root")
    original = read_container_file(backup)
    repaired = False

    try:
        if mutation == "remove":
            assert replacement is None
            node.exec_in_container(["rm", "--", path], user="root")
        elif mutation == "replace":
            assert replacement is not None
            assert 0 < len(replacement) <= MAX_TEST_ARTIFACT_BYTES
            assert replacement != original
            write_container_file(path, replacement)
        else:
            assert replacement is None
            write_container_file(path, mutated_artifact(original, mutation))

        node.start_clickhouse()
        assert node.get_process_pid("clickhouse") is not None, case_id
        node.exec_in_container(["cmp", "--silent", "--", backup, path], user="root")
        assert read_container_file(path) == original
        assert_local_wal_repair_provenance(database, previous_transaction_id)
        assert_mapped_table(database)
        repaired = True
    finally:
        if not repaired:
            if node.get_process_pid("clickhouse") is not None:
                node.stop_clickhouse()
            node.exec_in_container(
                ["cp", "--preserve=all", "--", backup, path], user="root"
            )
            node.start_clickhouse()
        node.exec_in_container(["rm", "-f", "--", backup], user="root")


def assert_startup_rejects_unexpected_artifact(database, directory):
    case_id = f"unexpected-{uuid.uuid4().hex[:8]}"
    path = os.path.join(directory, f"{case_id}.references")

    assert not node.file_exists_in_container(path), path
    node.stop_clickhouse()
    assert node.get_process_pid("clickhouse") is None
    removed = False
    try:
        write_container_file(path, b"bounded unexpected authority artifact")
        node.start_clickhouse()
        assert node.get_process_pid("clickhouse") is not None, case_id
        assert_database_startup_fail_closed(database)
    finally:
        if node.get_process_pid("clickhouse") is not None:
            node.stop_clickhouse()
        node.exec_in_container(["rm", "-f", "--", path], user="root")
        assert not node.file_exists_in_container(path), path
        removed = True
        node.start_clickhouse()

    assert removed
    assert_mapped_table(database)


def cleanup_databases(*databases):
    primary_exception_active = sys.exc_info()[0] is not None
    errors = []
    try:
        # This also waits for readiness when a server PID already exists.
        node.start_clickhouse()
    except Exception as ex:  # noqa: BLE001 - report every cleanup failure.
        errors.append(ex)
    else:
        for database in databases:
            try:
                query(f"DROP DATABASE IF EXISTS {database} SYNC")
            except Exception as ex:  # noqa: BLE001 - continue cleaning peers.
                errors.append(ex)

    if errors and not primary_exception_active:
        details = "; ".join(f"{type(error).__name__}: {error}" for error in errors)
        raise RuntimeError(f"UDT recovery test cleanup failed: {details}") from errors[0]


@pytest.mark.timeout(1200)
def test_mapped_table_artifacts_repair_or_fail_closed_exactly(started_cluster):
    database = unique_database("artifacts")
    try:
        create_mapped_table(database)
        paths = durable_artifact_paths(database)
        committed_image = {
            name: read_container_file(path) for name, path in paths.items()
        }
        repairable_artifacts = {
            "references": ("truncate", "corrupt"),
            "expectation": ("truncate", "corrupt"),
        }
        for artifact, mutations in repairable_artifacts.items():
            assert_startup_repairs_artifact_from_local_wal(
                database, paths[artifact], "remove"
            )
            for mutation in mutations:
                assert_startup_repairs_artifact_from_local_wal(
                    database, paths[artifact], mutation
                )

        fail_closed_artifacts = {
            "installation": ("truncate", "corrupt"),
            "metadata": ("truncate", "corrupt"),
        }
        for artifact, mutations in fail_closed_artifacts.items():
            assert_startup_rejects_artifact_change(
                database,
                paths[artifact],
                "remove",
            )
            for mutation in mutations:
                assert_startup_rejects_artifact_change(
                    database, paths[artifact], mutation
                )

        assert_startup_repairs_artifact_from_local_wal(
            database,
            paths["references"],
            "physical_fingerprint",
        )
        assert_startup_repairs_artifact_from_local_wal(
            database,
            paths["references"],
            "occurrence_path",
        )
        assert_startup_rejects_unexpected_artifact(
            database,
            os.path.dirname(paths["expectation"]),
        )
        assert {
            name: read_container_file(path) for name, path in paths.items()
        } == committed_image
    finally:
        cleanup_databases(database)


def test_canonical_foreign_artifacts_repair_or_fail_closed_exactly(
    started_cluster,
):
    database = unique_database("foreign_target")
    donor_database = unique_database("foreign_donor")
    try:
        create_mapped_table(database)
        create_mapped_table(donor_database)
        paths = durable_artifact_paths(database)
        donor_paths = durable_artifact_paths(donor_database)
        committed_image = {
            name: read_container_file(path) for name, path in paths.items()
        }
        donor_image = {
            name: read_container_file(path) for name, path in donor_paths.items()
        }

        # These are individually valid canonical files, not random damaged
        # bytes. WAL-backed definition/expectation/reference images are replaced
        # with the target's exact committed bytes; installation remains an
        # unrepairable identity/content-address barrier and degrades fail-closed.
        for artifact in ("references", "expectation"):
            assert_startup_repairs_artifact_from_local_wal(
                database,
                paths[artifact],
                "replace",
                replacement=donor_image[artifact],
            )
            assert_mapped_table(donor_database)

        assert_startup_rejects_artifact_change(
            database,
            paths["installation"],
            "replace",
            replacement=donor_image["installation"],
        )
        assert_mapped_table(donor_database)

        assert {
            name: read_container_file(path) for name, path in paths.items()
        } == committed_image
        assert {
            name: read_container_file(path) for name, path in donor_paths.items()
        } == donor_image
    finally:
        cleanup_databases(database, donor_database)


def test_unused_definition_artifact_is_still_required_and_inventory_anchored(
    started_cluster,
):
    database = unique_database("unused_definition")
    donor_database = unique_database("unused_definition_donor")
    try:
        create_mapped_table(database)
        query(f"CREATE TYPE {database}.Spare AS String")
        query(f"CREATE DATABASE {donor_database} ENGINE = Atomic")
        query(f"CREATE TYPE {donor_database}.Spare AS String")
        spare_path = definition_artifact_path(database, "Spare")
        donor_spare_path = definition_artifact_path(donor_database, "Spare")
        committed_spare = read_container_file(spare_path)
        donor_spare = read_container_file(donor_spare_path)
        assert donor_spare != committed_spare

        # Spare has never appeared in table metadata, but its independently
        # inventoried exact bytes remain recoverable from the authenticated
        # local schema WAL rather than being inferred from directory contents.
        assert_startup_repairs_artifact_from_local_wal(
            database, spare_path, "remove"
        )
        assert_startup_repairs_artifact_from_local_wal(
            database, spare_path, "truncate"
        )
        assert_startup_repairs_artifact_from_local_wal(
            database, spare_path, "replace", replacement=donor_spare
        )
        assert read_container_file(spare_path) == committed_spare
        assert query(
            "SELECT count() FROM system.user_defined_types "
            f"WHERE database = '{database}' AND name = 'Spare'"
        ).strip() == "1"
    finally:
        cleanup_databases(database, donor_database)
