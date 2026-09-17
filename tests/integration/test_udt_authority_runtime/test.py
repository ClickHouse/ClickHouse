"""Real-server authority verification, quarantine, repair, and quota coverage."""

import base64
import json
import os
import threading
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["configs/udt.xml"],
    main_configs=["configs/resource_limits.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)

ENABLED = {"allow_experimental_user_defined_types": 1}
DATABASE = "udt_authority_runtime"
DATABASE_UUID = "a4000000-0000-4000-8000-000000000001"
MANIFEST_EVENTS = (
    "UDTManifestEntries",
    "UDTManifestBytes",
    "UDTManifestDispatchCopies",
    "UDTManifestDispatchBytes",
    "UDTManifestReceiverAdmissions",
)
PUBLICATION_WAITER_FAILPOINT = (
    "udt_authority_runtime_pause_after_publication_waiter_registration"
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown(ignore_fatal=True)


def query(sql):
    return node.query(sql, settings=ENABLED)


def query_error(sql):
    error = node.query_and_get_error(sql, settings=ENABLED)
    assert error, sql
    return error


def json_rows(sql):
    output = query(f"{sql} FORMAT JSONEachRow")
    return [json.loads(line) for line in output.splitlines() if line]


def event_value(name):
    return int(
        query(
            "SELECT sum(value) FROM system.events "
            f"WHERE event = '{name}' SETTINGS system_events_show_zero_values = 1"
        ).strip()
    )


def database_status(database=DATABASE):
    rows = json_rows(
        "SELECT uuid, udt_verification_state, udt_verification_runtime_status_available, "
        "udt_verification_runtime_fail_closed, udt_verification_runtime_revision, "
        "udt_verification_quarantine_failing_seeds, udt_verification_quarantined_objects, "
        "udt_verification_last_error, udt_verification_runs, "
        "udt_verification_cached_targets, udt_verification_planned_batches, "
        "udt_verification_terminal_targets, udt_verification_verified_targets, "
        "udt_verification_damaged_targets, udt_verification_cursor_advances, "
        "udt_verification_completed_rotations, "
        "udt_verification_last_root_catalog_epoch, "
        "udt_verification_last_root_authority_anchor, "
        "udt_verification_last_successful_root_catalog_epoch, "
        "udt_verification_last_successful_root_authority_anchor, "
        "udt_verification_repair_attempts, udt_verification_repair_successes, "
        "udt_verification_repair_unavailable, "
        "udt_verification_last_repair_transaction_id, "
        "udt_verification_last_repair_local_wal_sources, "
        "udt_verification_last_repair_provenance_available, "
        "udt_verification_scheduler_override_state, "
        "udt_verification_successful_batch_interval_ms, "
        "udt_resource_quota_override_state, "
        "udt_quota_state, udt_quota_revision, udt_quota_definitions, "
        "udt_quota_deterministic_catalog_bytes, udt_quota_limit_definitions "
        f"FROM system.databases WHERE name = '{database}'"
    )
    assert len(rows) == 1
    return rows[0]


def wait_for_runtime(database=DATABASE):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        status = database_status(database)
        if status["udt_verification_runtime_status_available"] == 1:
            return status
        time.sleep(0.1)
    raise AssertionError("UDT verification runtime did not become observable")


def wait_for_status(database, predicate, description, timeout=60):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = database_status(database)
        if predicate(last):
            return last
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {description}: {last}")


def unique_database(prefix):
    return f"udt_authority_{prefix}_{uuid.uuid4().hex[:8]}"


def mapped_table_artifact_paths(database, table):
    default_disk_root = query(
        "SELECT path FROM system.disks WHERE name = 'default'"
    ).strip()
    database_row = query(
        "SELECT metadata_path, toString(uuid) FROM system.databases "
        f"WHERE name = '{database}' FORMAT TSV"
    ).strip()
    metadata_root, database_uuid = database_row.split("\t")
    table_row = query(
        "SELECT metadata_path, toString(uuid) FROM system.tables "
        f"WHERE database = '{database}' AND name = '{table}' FORMAT TSV"
    ).strip()
    _, table_uuid = table_row.split("\t")

    if not os.path.isabs(metadata_root):
        metadata_root = os.path.join(default_disk_root, metadata_root)
    metadata_root = os.path.normpath(metadata_root)
    default_disk_root = os.path.normpath(default_disk_root)
    assert os.path.commonpath([default_disk_root, metadata_root]) == default_disk_root
    expectations = os.path.join(
        metadata_root,
        "types",
        ".authority",
        "databases",
        database_uuid,
        "expectations",
    )
    result = {
        "references": os.path.join(expectations, f"{table_uuid}.references"),
        "installation": os.path.join(expectations, f"{table_uuid}.installation"),
    }
    assert all(node.file_exists_in_container(path) for path in result.values()), result
    return result


def read_container_file(path):
    return base64.b64decode(
        node.exec_in_container(["base64", path], user="root"), validate=False
    )


def write_container_file(path, contents):
    encoded = base64.b64encode(contents).decode("ascii")
    node.exec_in_container(
        [
            "bash",
            "-c",
            "printf '%s' \"$1\" | base64 --decode > \"$2\"",
            "udt-authority-write",
            encoded,
            path,
        ],
        user="root",
    )


def corrupt_last_byte(contents):
    assert contents
    result = bytearray(contents)
    result[-1] ^= 0x01
    return bytes(result)


def assert_policy_free_manifest_counters_are_zero():
    assert {name: event_value(name) for name in MANIFEST_EVENTS} == {
        name: 0 for name in MANIFEST_EVENTS
    }


def test_authority_observability_restart_and_growth_neutral_quota_failure():
    query(f"DROP DATABASE IF EXISTS {DATABASE} SYNC")
    publications_before = event_value("UDTAuthorityRootPublications")
    bytes_before = event_value("UDTAuthorityPublishedDeterministicCatalogBytes")

    query(
        f"CREATE DATABASE {DATABASE} UUID '{DATABASE_UUID}' ENGINE = Atomic"
    )
    query(f"CREATE TYPE {DATABASE}.OnlyType AS UInt64")
    assert query(f"SELECT toTypeName(CAST(1 AS {DATABASE}.OnlyType))").strip() == "UInt64"

    status = wait_for_runtime()
    assert status["uuid"] == DATABASE_UUID
    assert status["udt_verification_runtime_fail_closed"] == 0
    assert status["udt_verification_runtime_revision"] >= 1
    assert status["udt_verification_quarantine_failing_seeds"] == 0
    assert status["udt_verification_quarantined_objects"] == 0
    assert status["udt_verification_scheduler_override_state"] == "Persisted"
    assert status["udt_verification_successful_batch_interval_ms"] == 80
    assert status["udt_resource_quota_override_state"] == "Persisted"
    assert status["udt_quota_state"] == "ACTIVE"
    assert status["udt_quota_definitions"] == 1
    assert status["udt_quota_limit_definitions"] == 1
    assert status["udt_quota_deterministic_catalog_bytes"] > 0
    assert event_value("UDTAuthorityRootPublications") > publications_before
    assert event_value("UDTAuthorityPublishedDeterministicCatalogBytes") > bytes_before
    assert_policy_free_manifest_counters_are_zero()

    publications_before_rejection = event_value("UDTAuthorityRootPublications")
    bytes_before_rejection = event_value(
        "UDTAuthorityPublishedDeterministicCatalogBytes"
    )
    error = query_error(f"CREATE TYPE {DATABASE}.RejectedGrowth AS UInt8")
    assert "LIMIT_EXCEEDED" in error
    assert "definitions including the definition being lowered" in error
    rejected_status = database_status()
    assert rejected_status["udt_quota_definitions"] == 1
    assert event_value("UDTAuthorityRootPublications") == publications_before_rejection
    assert (
        event_value("UDTAuthorityPublishedDeterministicCatalogBytes")
        == bytes_before_rejection
    )

    node.restart_clickhouse()
    restarted = wait_for_runtime()
    assert restarted["uuid"] == DATABASE_UUID
    assert restarted["udt_verification_runtime_fail_closed"] == 0
    assert restarted["udt_verification_scheduler_override_state"] == "Persisted"
    assert restarted["udt_verification_successful_batch_interval_ms"] == 80
    assert restarted["udt_resource_quota_override_state"] == "Persisted"
    assert restarted["udt_quota_state"] == "ACTIVE"
    assert restarted["udt_quota_definitions"] == 1
    assert restarted["udt_quota_limit_definitions"] == 1
    assert query(f"SHOW CREATE TYPE {DATABASE}.OnlyType").strip().endswith("AS UInt64")
    assert "LIMIT_EXCEEDED" in query_error(
        f"CREATE TYPE {DATABASE}.StillRejected AS UInt16"
    )
    assert event_value("UDTAuthorityRootPublications") >= 1
    assert event_value("UDTAuthorityPublishedDeterministicCatalogBytes") > 0
    assert_policy_free_manifest_counters_are_zero()

    query(f"DROP TYPE {DATABASE}.OnlyType")
    shrunk = database_status()
    assert shrunk["udt_quota_state"] == "ACTIVE"
    assert shrunk["udt_quota_definitions"] == 0
    query(f"CREATE TYPE {DATABASE}.Replacement AS String")
    assert database_status()["udt_quota_definitions"] == 1
    assert query(f"SELECT toTypeName(CAST('x' AS {DATABASE}.Replacement))").strip() == "String"

    node.restart_clickhouse()
    final = wait_for_runtime()
    assert final["udt_quota_definitions"] == 1
    assert final["udt_quota_limit_definitions"] == 1
    assert final["udt_verification_runtime_fail_closed"] == 0
    assert query(f"SHOW CREATE TYPE {DATABASE}.Replacement").strip().endswith("AS String")
    assert_policy_free_manifest_counters_are_zero()

    query(f"DROP DATABASE {DATABASE} SYNC")


def test_periodic_verification_quarantine_exact_repair_and_release_cycle():
    database = unique_database("live_cycle")
    table = "events"
    paths = {}
    committed = {}
    try:
        query(f"CREATE DATABASE {database} ENGINE = Atomic")
        query(f"CREATE TYPE {database}.UserId AS UInt64")
        query(
            f"CREATE TABLE {database}.{table} "
            f"(id {database}.UserId, payload String) "
            "ENGINE = MergeTree ORDER BY id"
        )
        query(
            f"CREATE TABLE {database}.unmapped "
            "(id UInt64) ENGINE = MergeTree ORDER BY id"
        )
        query(f"INSERT INTO {database}.{table} VALUES (1, 'one'), (2, 'two')")
        query(f"INSERT INTO {database}.unmapped VALUES (10)")

        clean = wait_for_status(
            database,
            lambda status: status["udt_verification_planned_batches"] > 0
            and status["udt_verification_verified_targets"] >= 2
            and status["udt_verification_cursor_advances"] > 0
            and status["udt_verification_last_successful_root_catalog_epoch"] > 0,
            "one complete clean periodic verification batch",
        )
        assert clean["udt_verification_runs"] > 0
        assert clean["udt_verification_cached_targets"] >= 2
        assert clean["udt_verification_terminal_targets"] >= 2
        assert clean["udt_verification_damaged_targets"] == 0
        assert clean["udt_verification_last_error"] == ""
        assert len(clean["udt_verification_last_root_authority_anchor"]) == 64
        assert (
            clean["udt_verification_last_root_authority_anchor"]
            == clean["udt_verification_last_successful_root_authority_anchor"]
        )

        paths = mapped_table_artifact_paths(database, table)
        committed = {name: read_container_file(path) for name, path in paths.items()}
        assert all(committed.values())

        # An installation record is intentionally not reconstructible from a
        # different source.  Damage must remain visible long enough to prove
        # the full operation gate and the exact affected-object closure.
        before_damage = database_status(database)
        write_container_file(
            paths["installation"], corrupt_last_byte(committed["installation"])
        )
        quarantined = wait_for_status(
            database,
            lambda status: status["udt_verification_damaged_targets"]
            > before_damage["udt_verification_damaged_targets"]
            and status["udt_verification_quarantined_objects"] > 0
            and status["udt_verification_repair_unavailable"]
            > before_damage["udt_verification_repair_unavailable"],
            "live damage to enter quarantine and remain unrepaired",
        )
        assert quarantined["udt_verification_quarantine_failing_seeds"] == 1
        assert quarantined["udt_verification_runtime_fail_closed"] == 0
        assert quarantined["udt_verification_last_error"] in (
            "IntegrityDamageQuarantined",
            "ExactRepairUnavailable",
        )

        for blocked_sql in (
            f"SELECT count() FROM {database}.{table}",
            f"INSERT INTO {database}.{table} VALUES (3, 'three')",
            f"UPDATE {database}.{table} SET payload = 'blocked' WHERE id = 1",
            f"DELETE FROM {database}.{table} WHERE id = 2",
            f"OPTIMIZE TABLE {database}.{table} FINAL",
            f"ALTER TABLE {database}.{table} ADD COLUMN blocked UInt8 DEFAULT 0",
            f"TRUNCATE TABLE {database}.{table}",
        ):
            error = query_error(blocked_sql)
            assert "quarantin" in error.lower() or "verification" in error.lower(), error

        # The closure is object-specific: unrelated physical storage remains
        # readable and writable while the mapped object is isolated.
        assert query(f"SELECT sum(id) FROM {database}.unmapped").strip() == "10"
        query(f"INSERT INTO {database}.unmapped VALUES (20)")
        assert query(f"SELECT sum(id) FROM {database}.unmapped").strip() == "30"

        # Restoring the exact committed record requires a complete new-root
        # re-verification before operations are admitted again.
        repair_successes_before_restore = quarantined[
            "udt_verification_repair_successes"
        ]
        write_container_file(paths["installation"], committed["installation"])
        released = wait_for_status(
            database,
            lambda status: status["udt_verification_quarantined_objects"] == 0
            and status["udt_verification_quarantine_failing_seeds"] == 0
            and status["udt_verification_repair_successes"]
            > repair_successes_before_restore,
            "exact re-verification to release quarantine",
        )
        assert released["udt_verification_last_error"] == ""
        assert query(f"SELECT sum(id) FROM {database}.{table}").strip() == "3"
        query(f"INSERT INTO {database}.{table} VALUES (3, 'three')")

        # A sidecar is recoverable from the exact retained local schema WAL.
        # This second transition proves an actual durable repair rather than
        # the re-verification-only release above.
        before_repair = database_status(database)
        write_container_file(
            paths["references"], corrupt_last_byte(committed["references"])
        )
        repaired = wait_for_status(
            database,
            lambda status: status["udt_verification_repair_successes"]
            > before_repair["udt_verification_repair_successes"]
            and status["udt_verification_last_repair_transaction_id"] > 0
            and status["udt_verification_last_repair_local_wal_sources"] == 1
            and status["udt_verification_last_repair_provenance_available"] == 1
            and status["udt_verification_quarantined_objects"] == 0,
            "automatic exact sidecar repair from local schema WAL",
        )
        assert repaired["udt_verification_damaged_targets"] > before_repair[
            "udt_verification_damaged_targets"
        ]
        assert read_container_file(paths["references"]) == committed["references"]
        assert query(f"SELECT sum(id) FROM {database}.{table}").strip() == "6"

        # Both externally modified files are exact again.  Subsequent ALTERs
        # legitimately replace their durable images, so cleanup must no longer
        # restore this earlier generation.
        committed = {}

        query(
            f"ALTER TABLE {database}.{table} "
            "ADD COLUMN note String DEFAULT 'after-release'"
        )
        assert query(
            f"SELECT count(), uniqExact(note) FROM {database}.{table}"
        ).strip() == "3\t1"

        node.restart_clickhouse()
        restarted = wait_for_status(
            database,
            lambda status: status["udt_verification_runtime_status_available"] == 1
            and status["udt_verification_runtime_fail_closed"] == 0
            and status["udt_verification_quarantined_objects"] == 0
            and status["udt_verification_verified_targets"] >= 2,
            "clean verification after restart",
        )
        assert restarted["udt_verification_last_error"] == ""
        assert query(
            f"SELECT sum(id), groupUniqArray(note) FROM {database}.{table}"
        ).strip() == "6\t['after-release']"
    finally:
        # Restore externally changed bytes before cleanup even if an assertion
        # interrupts the state transition.  DROP then remains a normal catalog
        # operation rather than inheriting a damaged test fixture.
        for name, path in paths.items():
            if name in committed and node.file_exists_in_container(path):
                try:
                    if read_container_file(path) != committed[name]:
                        write_container_file(path, committed[name])
                except Exception:
                    pass
        try:
            if query(
                "SELECT count() FROM system.databases "
                f"WHERE name = '{database}'"
            ).strip() == "1":
                try:
                    wait_for_status(
                        database,
                        lambda status: status[
                            "udt_verification_quarantined_objects"
                        ]
                        == 0,
                        "cleanup quarantine release",
                        timeout=20,
                    )
                finally:
                    query(f"DROP DATABASE IF EXISTS {database} SYNC")
        except Exception:
            query(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_inflight_mapped_write_finishes_before_new_quarantine_publication():
    database = unique_database("commit_fence")
    suffix = uuid.uuid4().hex[:8]
    fifo_name = f"udt_commit_fence_{suffix}.fifo"
    fifo_path = f"/var/lib/clickhouse/user_files/{fifo_name}"
    reader_ready_path = f"/tmp/udt_commit_fence_{suffix}.ready"
    reader_release_path = f"/tmp/udt_commit_fence_{suffix}.release"
    reader_output_path = f"/tmp/udt_commit_fence_{suffix}.out"
    reader_done_path = f"/tmp/udt_commit_fence_{suffix}.done"
    reader_pid_path = f"/tmp/udt_commit_fence_{suffix}.pid"
    write_query_id = f"udt_commit_fence_write_{suffix}"
    write_outcome = {"returned": False, "error": None}
    write_thread = None
    artifact_paths = {}
    committed_artifacts = {}
    publication_failpoint_enabled = False

    def remove_reader_state():
        node.exec_in_container(
            [
                "rm",
                "-f",
                reader_ready_path,
                reader_release_path,
                reader_output_path,
                reader_done_path,
                reader_pid_path,
            ],
            user="root",
        )

    def start_reader(*, release_immediately):
        remove_reader_state()
        if release_immediately:
            node.exec_in_container(
                ["touch", reader_release_path],
                user="root",
            )
        script = """
            echo $$ > "$4"
            exec 3<"$1"
            : > "$2"
            while [ ! -e "$3" ]; do sleep 0.02; done
            cat <&3 > "$5"
            rm -f "$4"
            : > "$6"
        """
        node.exec_in_container(
            [
                "bash",
                "-c",
                script,
                "udt-commit-fence-reader",
                fifo_path,
                reader_ready_path,
                reader_release_path,
                reader_pid_path,
                reader_output_path,
                reader_done_path,
            ],
            user="root",
            detach=True,
        )

    def release_reader():
        node.exec_in_container(["touch", reader_release_path], user="root")

    def wait_for_container_file(path, description, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if node.file_exists_in_container(path):
                return
            time.sleep(0.05)
        raise AssertionError(f"timed out waiting for {description}: {path}")

    def stack_traces(query_id=None):
        where = f"WHERE query_id = '{query_id}'" if query_id else ""
        return node.query(
            "SELECT concat(query_id, '\\n', arrayStringConcat(arrayMap(address -> "
            "demangle(addressToSymbol(address)), trace), '\\n')) "
            f"FROM system.stack_trace {where} FORMAT TSVRaw",
            settings={**ENABLED, "allow_introspection_functions": 1},
            timeout=30,
        )

    def wait_for_stack(predicate, description, timeout=60, query_id=None):
        deadline = time.monotonic() + timeout
        last = ""
        while time.monotonic() < deadline:
            last = stack_traces(query_id)
            if predicate(last):
                return last
            time.sleep(0.2)
        raise AssertionError(f"timed out waiting for {description}: {last[-4000:]}")

    def write_blocking_row():
        try:
            # Keep the row below DBMS_DEFAULT_BUFFER_SIZE so consume only
            # fills the userspace buffer. The payload still exceeds the FIFO
            # capacity, making the guarded onFinish flush block on the reader.
            node.query(
                f"INSERT INTO {database}.mapped_mv "
                "SELECT toUInt64(42), repeat('x', 512 * 1024)",
                settings=ENABLED,
                query_id=write_query_id,
                timeout=120,
            )
            write_outcome["returned"] = True
        except BaseException as ex:  # noqa: BLE001 - surfaced in the main thread.
            write_outcome["error"] = ex

    def write_one_row_after_release(row_id):
        start_reader(release_immediately=True)
        query(
            f"INSERT INTO {database}.mapped_mv VALUES "
            f"({row_id}, 'after-release-{row_id}')"
        )
        wait_for_container_file(
            reader_done_path,
            f"drained mapped write {row_id}",
        )

    try:
        node.exec_in_container(
            [
                "bash",
                "-c",
                "rm -f \"$1\"; mkfifo \"$1\"; chmod 0666 \"$1\"",
                "udt-commit-fence-fifo",
                fifo_path,
            ],
            user="root",
        )
        query(f"CREATE DATABASE {database} ENGINE = Atomic")
        query(f"CREATE TYPE {database}.UserId AS UInt64")
        query(
            f"CREATE TABLE {database}.target (id UInt64, payload String) "
            f"ENGINE = File(TSV, '{fifo_name}')"
        )
        query(
            f"CREATE TABLE {database}.source (id UInt64, payload String) "
            "ENGINE = Memory"
        )
        query(
            f"CREATE MATERIALIZED VIEW {database}.mapped_mv "
            f"TO {database}.target "
            f"(id {database}.UserId, payload String) AS "
            f"SELECT id, payload FROM {database}.source"
        )
        query(
            f"CREATE TABLE {database}.unmapped (id UInt64) "
            "ENGINE = MergeTree ORDER BY id"
        )

        clean = wait_for_status(
            database,
            lambda status: status["udt_verification_planned_batches"] > 0
            and status["udt_verification_verified_targets"] >= 2
            and status["udt_verification_quarantined_objects"] == 0,
            "clean mapped MaterializedView verification",
        )
        artifact_paths = mapped_table_artifact_paths(database, "mapped_mv")
        committed_artifacts = {
            name: read_container_file(path) for name, path in artifact_paths.items()
        }

        start_reader(release_immediately=False)
        write_thread = threading.Thread(target=write_blocking_row, daemon=True)
        write_thread.start()
        wait_for_container_file(
            reader_ready_path,
            "the File sink to open its FIFO",
        )
        wait_for_stack(
            lambda traces: "StorageFileSink::onFinish" in traces
            or "StorageFileSink::finalizeBuffers" in traces,
            "the mapped write to block inside guarded File sink finalization",
            query_id=write_query_id,
        )
        assert write_thread.is_alive()

        query(f"SYSTEM ENABLE FAILPOINT {PUBLICATION_WAITER_FAILPOINT}")
        publication_failpoint_enabled = True
        write_container_file(
            artifact_paths["installation"],
            corrupt_last_byte(committed_artifacts["installation"]),
        )
        node.query(
            f"SYSTEM WAIT FAILPOINT {PUBLICATION_WAITER_FAILPOINT} PAUSE",
            settings=ENABLED,
            timeout=60,
        )
        publication_waiting = database_status(database)
        assert publication_waiting["udt_verification_state"] == "Executing"
        assert publication_waiting["udt_verification_quarantined_objects"] == 0
        assert write_thread.is_alive()

        # A physical object does not acquire the mapped commit fence. It remains
        # usable while the writer-preferred quarantine publication is waiting.
        query(f"INSERT INTO {database}.unmapped VALUES (10), (20)")
        assert query(f"SELECT sum(id) FROM {database}.unmapped").strip() == "30"

        # Let the registered publication proceed to its commit-fence wait
        # before allowing the mapped writer to finish and release its guard.
        query(f"SYSTEM DISABLE FAILPOINT {PUBLICATION_WAITER_FAILPOINT}")
        publication_failpoint_enabled = False
        release_reader()
        write_thread.join(timeout=30)
        assert not write_thread.is_alive(), "mapped write did not release its final fence"
        wait_for_container_file(
            reader_done_path,
            "the blocked mapped write FIFO to drain",
        )
        if write_outcome["error"] is not None:
            raise write_outcome["error"]
        assert write_outcome["returned"]
        assert int(
            node.exec_in_container(
                ["stat", "-c", "%s", reader_output_path],
                user="root",
            ).strip()
        ) > 512 * 1024

        quarantined = wait_for_status(
            database,
            lambda status: status["udt_verification_damaged_targets"]
            > clean["udt_verification_damaged_targets"]
            and status["udt_verification_quarantined_objects"] > 0,
            "quarantine publication after the in-flight mapped write committed",
        )
        rejected = node.query_and_get_error(
            f"INSERT INTO {database}.mapped_mv VALUES (43, 'blocked')",
            settings=ENABLED,
            timeout=10,
        )
        assert "quarantin" in rejected.lower() or "verification" in rejected.lower()
        query(f"INSERT INTO {database}.unmapped VALUES (30)")
        assert query(f"SELECT sum(id) FROM {database}.unmapped").strip() == "60"

        repair_successes_before_restore = quarantined[
            "udt_verification_repair_successes"
        ]
        write_container_file(
            artifact_paths["installation"],
            committed_artifacts["installation"],
        )
        released = wait_for_status(
            database,
            lambda status: status["udt_verification_quarantined_objects"] == 0
            and status["udt_verification_quarantine_failing_seeds"] == 0
            and status["udt_verification_repair_successes"]
            > repair_successes_before_restore,
            "exact restored image to release quarantine",
        )
        assert released["udt_verification_last_error"] == ""
        write_one_row_after_release(44)

        restart_wait_seconds = 300 if node.is_built_with_sanitizer() else 60
        node.restart_clickhouse(stop_start_wait_sec=restart_wait_seconds)
        wait_for_status(
            database,
            lambda status: status["udt_verification_runtime_status_available"] == 1
            and status["udt_verification_runtime_fail_closed"] == 0
            and status["udt_verification_quarantined_objects"] == 0
            and status["udt_verification_verified_targets"] >= 2,
            "clean commit-fence authority after restart",
        )
        write_one_row_after_release(45)
        assert query(f"SELECT sum(id) FROM {database}.unmapped").strip() == "60"
    finally:
        if (
            publication_failpoint_enabled
            and node.get_process_pid("clickhouse") is not None
        ):
            try:
                query(f"SYSTEM DISABLE FAILPOINT {PUBLICATION_WAITER_FAILPOINT}")
            except Exception:
                pass
            publication_failpoint_enabled = False
        try:
            release_reader()
        except Exception:
            pass
        if write_thread is not None:
            write_thread.join(timeout=30)
            if write_thread.is_alive():
                try:
                    node.query(
                        f"KILL QUERY WHERE query_id = '{write_query_id}' SYNC",
                        settings=ENABLED,
                        timeout=10,
                    )
                except Exception:
                    pass
                node.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "if [ -s \"$1\" ]; then kill \"$(cat \"$1\")\" 2>/dev/null || true; fi",
                        "udt-commit-fence-stop-reader",
                        reader_pid_path,
                    ],
                    user="root",
                    timeout=30,
                )
                write_thread.join(timeout=30)
            if write_thread.is_alive():
                restart_wait_seconds = 300 if node.is_built_with_sanitizer() else 60
                node.restart_clickhouse(
                    stop_start_wait_sec=restart_wait_seconds,
                    kill=True,
                )
                write_thread.join(timeout=30)
            assert not write_thread.is_alive(), "mapped write survived forced cleanup"
        for name, path in artifact_paths.items():
            if name in committed_artifacts and node.file_exists_in_container(path):
                try:
                    if read_container_file(path) != committed_artifacts[name]:
                        write_container_file(path, committed_artifacts[name])
                except Exception:
                    pass
        try:
            if query(
                "SELECT count() FROM system.databases "
                f"WHERE name = '{database}'"
            ).strip() == "1":
                wait_for_status(
                    database,
                    lambda status: status[
                        "udt_verification_quarantined_objects"
                    ]
                    == 0,
                    "commit-fence cleanup quarantine release",
                    timeout=20,
                )
                query(f"DROP DATABASE IF EXISTS {database} SYNC")
        except Exception:
            query(f"DROP DATABASE IF EXISTS {database} SYNC")
        node.exec_in_container(
            [
                "bash",
                "-c",
                "if [ -s \"$1\" ]; then kill \"$(cat \"$1\")\" 2>/dev/null || true; fi; "
                "rm -f \"$2\" \"$3\" \"$4\" \"$5\" \"$6\" \"$1\"",
                "udt-commit-fence-cleanup",
                reader_pid_path,
                fifo_path,
                reader_ready_path,
                reader_release_path,
                reader_output_path,
                reader_done_path,
            ],
            user="root",
        )


def test_mapped_mergetree_data_and_partition_operation_matrix():
    database = unique_database("operations")
    table = "mapped"
    try:
        query(f"CREATE DATABASE {database} ENGINE = Atomic")
        query(f"CREATE TYPE {database}.UserId AS UInt64")
        query(
            f"CREATE TABLE {database}.{table} "
            f"(id {database}.UserId, p UInt8, value Int64) "
            "ENGINE = MergeTree PARTITION BY p ORDER BY id "
            "SETTINGS enable_block_number_column = 1, "
            "enable_block_offset_column = 1"
        )
        query(
            f"CREATE TABLE {database}.donor "
            "(id UInt64, p UInt8, value Int64) "
            "ENGINE = MergeTree PARTITION BY p ORDER BY id"
        )
        query(
            f"CREATE TABLE {database}.sink "
            "(id UInt64, p UInt8, value Int64) "
            "ENGINE = MergeTree PARTITION BY p ORDER BY id"
        )
        query(
            f"INSERT INTO {database}.{table} VALUES "
            "(1, 0, 10), (2, 0, 20), (3, 1, 30), (4, 1, 40)"
        )
        paths = mapped_table_artifact_paths(database, table)
        authority_image = {
            name: read_container_file(path) for name, path in paths.items()
        }

        def assert_authority_image_unchanged():
            assert {
                name: read_container_file(path) for name, path in paths.items()
            } == authority_image
            assert query(
                "SELECT count() FROM system.columns "
                f"WHERE database = '{database}' AND table = '{table}' "
                "AND notEmpty(udt_references)"
            ).strip() == "1"

        # Exercise every interpreter-level mutation boundary as well as the
        # MergeTree commit fence.  None changes the logical schema image.
        query(
            f"UPDATE {database}.{table} SET value = value + 5 WHERE id = 1 "
            "SETTINGS mutations_sync = 2"
        )
        assert query(
            f"SELECT value FROM {database}.{table} WHERE id = 1"
        ).strip() == "15"
        assert_authority_image_unchanged()

        query(
            f"DELETE FROM {database}.{table} WHERE id = 2 "
            "SETTINGS mutations_sync = 2"
        )
        assert query(
            f"SELECT groupArray(id) FROM {database}.{table} WHERE p = 0"
        ).strip() == "[1]"
        assert_authority_image_unchanged()

        query(
            f"ALTER TABLE {database}.{table} "
            "UPDATE value = value + 1 IN PARTITION 1 WHERE id = 3 "
            "SETTINGS mutations_sync = 2"
        )
        assert query(
            f"SELECT value FROM {database}.{table} WHERE id = 3"
        ).strip() == "31"
        assert_authority_image_unchanged()

        query(f"OPTIMIZE TABLE {database}.{table} FINAL")
        assert_authority_image_unchanged()
        freeze_error = query_error(
            f"ALTER TABLE {database}.{table} FREEZE WITH NAME 'udt_authority'"
        )
        assert "FREEZE/UNFREEZE are not supported" in freeze_error
        assert_authority_image_unchanged()

        query(f"ALTER TABLE {database}.{table} DROP PARTITION 1")
        assert query(
            f"SELECT arraySort(groupArray(id)) FROM {database}.{table}"
        ).strip() == "[1]"
        assert_authority_image_unchanged()

        query(f"INSERT INTO {database}.{table} VALUES (5, 2, 50)")
        detach_error = query_error(
            f"ALTER TABLE {database}.{table} DETACH PARTITION 2"
        )
        assert "DETACH PARTITION is not supported" in detach_error
        assert query(
            f"SELECT sum(value) FROM {database}.{table} WHERE p = 2"
        ).strip() == "50"
        assert_authority_image_unchanged()
        attach_error = query_error(
            f"ALTER TABLE {database}.{table} ATTACH PARTITION 2"
        )
        assert "ATTACH PART/PARTITION is not supported" in attach_error
        assert query(
            f"SELECT sum(value) FROM {database}.{table} WHERE p = 2"
        ).strip() == "50"
        assert_authority_image_unchanged()

        query(f"INSERT INTO {database}.donor VALUES (6, 3, 60)")
        replace_error = query_error(
            f"ALTER TABLE {database}.{table} "
            f"REPLACE PARTITION 3 FROM {database}.donor"
        )
        assert query(
            f"SELECT count() FROM {database}.{table} WHERE p = 3"
        ).strip() == "0"
        assert "REPLACE PARTITION FROM is not supported" in replace_error
        assert_authority_image_unchanged()

        query(f"INSERT INTO {database}.{table} VALUES (7, 4, 70)")
        move_error = query_error(
            f"ALTER TABLE {database}.{table} "
            f"MOVE PARTITION 4 TO TABLE {database}.sink"
        )
        assert query(
            f"SELECT sum(value) FROM {database}.{table} WHERE p = 4"
        ).strip() == "70"
        assert query(
            f"SELECT count() FROM {database}.sink WHERE p = 4"
        ).strip() == "0"
        assert "MOVE PART/PARTITION is not supported" in move_error
        assert_authority_image_unchanged()

        query(f"TRUNCATE TABLE {database}.{table}")
        assert query(f"SELECT count() FROM {database}.{table}").strip() == "0"
        assert_authority_image_unchanged()
        query(f"INSERT INTO {database}.{table} VALUES (8, 5, 80)")

        node.restart_clickhouse()
        wait_for_status(
            database,
            lambda status: status["udt_verification_runtime_status_available"] == 1
            and status["udt_verification_quarantined_objects"] == 0
            and status["udt_verification_verified_targets"] >= 2,
            "clean operation matrix after restart",
        )
        assert query(
            f"SELECT id, p, value FROM {database}.{table} FORMAT TSV"
        ) == "8\t5\t80\n"
        assert_authority_image_unchanged()
    finally:
        query(f"DROP DATABASE IF EXISTS {database} SYNC")
