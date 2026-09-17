"""Focused real-server correctness coverage for stored-object UDT bindings."""

import base64
import json
import os
import re
import sys
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

ENABLED = {
    "allow_experimental_analyzer": 1,
    "allow_experimental_eval_table_function": 1,
    "allow_experimental_user_defined_types": 1,
}
CONFIG = "/etc/clickhouse-server/users.d/udt.xml"
SETTING_ON = "<allow_experimental_user_defined_types>1</allow_experimental_user_defined_types>"
SETTING_OFF = "<allow_experimental_user_defined_types>0</allow_experimental_user_defined_types>"
PUBLICATION_FAILPOINTS = (
    "udt_authority_prepared_publication_failure",
    "udt_schema_storage_temp_write_failure",
)
DICTIONARY_REPOSITORY_FAILPOINT = (
    "udt_dictionary_repository_pause_after_live_admission"
)
DICTIONARY_REPOSITORY_QUERY_ID_PREFIX = "udt_dictionary_repository_load_"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def q(sql, *, user="default", settings=ENABLED, timeout=None):
    return node.query(sql, user=user, settings=settings, timeout=timeout)


def query_error(sql, *, user="default", settings=ENABLED):
    result = node.query_and_get_error(sql, user=user, settings=settings)
    assert result, sql
    return result


def rows_json(sql, *, user="default", settings=ENABLED):
    output = q(f"{sql} FORMAT JSONEachRow", user=user, settings=settings)
    return [json.loads(line) for line in output.splitlines() if line]


def sql_string(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def unique_database(prefix):
    return f"udt_stored_object_{prefix}_{uuid.uuid4().hex[:8]}"


def run_cleanup_steps(*steps):
    """Run every cleanup step without replacing an active test failure."""
    primary_exception_active = sys.exc_info()[0] is not None
    first_cleanup_error = None
    for step in steps:
        try:
            step()
        except Exception as error:
            if first_cleanup_error is None:
                first_cleanup_error = error

    if first_cleanup_error is not None and not primary_exception_active:
        raise first_cleanup_error


def assert_absent(database, object_name):
    assert q(f"EXISTS TABLE {database}.{object_name}").strip() == "0"


def wait_for_clean_verification(database, timeout=60):
    deadline = time.monotonic() + timeout
    last_status = None
    while time.monotonic() < deadline:
        statuses = rows_json(
            "SELECT udt_verification_state, udt_verification_cursor_advances, "
            "udt_verification_last_root_authority_anchor, "
            "udt_verification_last_successful_root_authority_anchor "
            f"FROM system.databases WHERE name = {sql_string(database)}"
        )
        assert len(statuses) == 1
        last_status = statuses[0]
        if (
            last_status["udt_verification_state"] == "Scheduled"
            and last_status["udt_verification_cursor_advances"] > 0
            and last_status["udt_verification_last_root_authority_anchor"]
            == last_status["udt_verification_last_successful_root_authority_anchor"]
        ):
            return
        time.sleep(0.1)
    raise AssertionError(
        f"UDT authority verification did not reach a clean scheduled state: {last_status}"
    )


def assert_stored_object_rejected(sql):
    result = query_error(sql)
    assert "NOT_IMPLEMENTED" in result, result
    assert "stored CREATE context" in result, result
    return result


def physical_column(database, table, name="id"):
    return q(
        "SELECT type, udt_declared_type FROM system.columns "
        f"WHERE database = '{database}' AND table = '{table}' AND name = '{name}' "
        "FORMAT TSV"
    ).strip()


def type_uuid(database, name, *, settings=ENABLED):
    described = dict(
        row.split("\t", 1)
        for row in q(
            f"DESCRIBE TYPE {database}.{name} FORMAT TSV", settings=settings
        ).splitlines()
    )
    return described["uuid"]


def object_uuid(database, name, *, settings=ENABLED):
    return q(
        "SELECT toString(uuid) FROM system.tables "
        f"WHERE database = '{database}' AND name = '{name}'",
        settings=settings,
    ).strip()


def show_create(database, name, kind, *, settings=ENABLED):
    statement_kind = "DICTIONARY" if kind == "DICTIONARY" else "TABLE"
    return q(
        f"SHOW CREATE {statement_kind} {database}.{name} FORMAT TSVRaw",
        settings=settings,
    )


def dictionary_value(
    database, dictionary, key, *, settings=ENABLED, timeout=None
):
    return q(
        f"SELECT dictGetString('{database}.{dictionary}', 'value', toUInt64({key}))",
        settings=settings,
        timeout=timeout,
    ).strip()


def physicalization_dry_run(selector):
    result = rows_json(f"PHYSICALIZE TYPE REFERENCES {selector} DRY RUN")
    assert len(result) == 1
    assert result[0]["scope_count"] == 1
    assert result[0]["manifest_count"] > 0
    assert result[0]["apply_token"]
    assert result[0]["canonical_loss_manifest_base64"]
    return result[0]


def container_path(path):
    disk_root = q("SELECT path FROM system.disks WHERE name = 'default'").strip()
    result = (
        os.path.normpath(path)
        if os.path.isabs(path)
        else os.path.normpath(os.path.join(disk_root, path))
    )
    assert os.path.commonpath([os.path.normpath(disk_root), result]) == os.path.normpath(
        disk_root
    )
    return result


def stored_object_reference_path(database, object_name):
    metadata_root, database_uuid = q(
        "SELECT metadata_path, toString(uuid) FROM system.databases "
        f"WHERE name = '{database}' FORMAT TSV"
    ).strip().split("\t")
    stored_object_uuid = q(
        "SELECT toString(uuid) FROM system.tables "
        f"WHERE database = '{database}' AND name = '{object_name}'"
    ).strip()
    result = os.path.join(
        metadata_root.rstrip("/"),
        "types",
        ".authority",
        "databases",
        database_uuid,
        "expectations",
        f"{stored_object_uuid}.references",
    )
    result = container_path(result)
    assert node.file_exists_in_container(result), result
    return result


def read_file(path):
    return base64.b64decode(node.exec_in_container(["base64", path]), validate=False)


def replace_file_atomically(path, contents):
    encoded = base64.b64encode(contents).decode("ascii")
    temporary_path = f"{path}.udt-test-{uuid.uuid4().hex}.tmp"
    node.exec_in_container(
        [
            "bash",
            "-c",
            "set -e; "
            "trap 'rm -f -- \"$3\"' EXIT; "
            "printf '%s' \"$1\" | base64 --decode > \"$3\"; "
            "chmod --reference=\"$2\" \"$3\"; "
            "chown --reference=\"$2\" \"$3\"; "
            "mv -f -- \"$3\" \"$2\"; "
            "trap - EXIT",
            "udt-stored-object-replace",
            encoded,
            path,
            temporary_path,
        ],
        user="root",
        timeout=30,
    )


def test_exact_schema_strings_nested_owners_and_restart(started_cluster):
    database = unique_database("nested")
    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")

        q(
            f"CREATE VIEW {database}.direct AS "
            f"SELECT id FROM null('id {database}.UserId')"
        )
        q(
            f"CREATE VIEW {database}.nested_loop AS "
            f"SELECT id FROM loop(`null`('id {database}.UserId')) LIMIT 0"
        )
        q(
            f"CREATE VIEW {database}.nested_view_if AS "
            "SELECT id FROM viewIfPermitted("
            f"SELECT CAST(1 AS {database}.UserId) AS id "
            f"ELSE null('id {database}.UserId'))"
        )
        q(
            f"CREATE VIEW {database}.eval_with_exact_auxiliary AS "
            f"SELECT structureToProtobufSchema('id {database}.UserId') AS schema "
            "FROM eval('SELECT 1 AS n')"
        )

        for view in (
            "direct",
            "nested_loop",
            "nested_view_if",
            "eval_with_exact_auxiliary",
        ):
            shown = q(f"SHOW CREATE VIEW {database}.{view}")
            assert f"{database}.UserId" in shown, shown
        assert q(f"SELECT id FROM {database}.nested_view_if").strip() == "1"

        node.restart_clickhouse()
        for view in (
            "direct",
            "nested_loop",
            "nested_view_if",
            "eval_with_exact_auxiliary",
        ):
            shown = q(f"SHOW CREATE VIEW {database}.{view}")
            assert f"{database}.UserId" in shown, shown
        assert q(f"SELECT id FROM {database}.nested_view_if").strip() == "1"

        # A public ATTACH with logical metadata but no matching sidecar must not
        # manufacture identity from its physical header.
        attach_error = query_error(
            f"ATTACH VIEW {database}.orphan "
            f"(id {database}.UserId) AS SELECT toUInt64(1) AS id"
        )
        assert "sidecar" in attach_error.lower() or "attach" in attach_error.lower()
        assert_absent(database, "orphan")
    finally:
        run_cleanup_steps(lambda: q(f"DROP DATABASE IF EXISTS {database} SYNC"))


def test_multiple_schema_endpoints_hints_and_stored_cast_are_indivisible(
    started_cluster,
):
    database = unique_database("mixed")
    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(
            f"CREATE VIEW {database}.mixed AS "
            f"SELECT CAST(id AS {database}.UserId) AS id, "
            f"structureToProtobufSchema('copy {database}.UserId') AS schema "
            f"FROM null('id {database}.UserId') "
            f"SETTINGS schema_inference_hints = 'hint {database}.UserId'"
        )
        shown = q(f"SHOW CREATE VIEW {database}.mixed")
        assert shown.count(f"{database}.UserId") >= 4, shown

        rejected = query_error(
            f"CREATE VIEW {database}.partial AS "
            f"SELECT CAST(id AS {database}.UserId) AS id, "
            f"structureToProtobufSchema('copy {database}.Missing') AS schema "
            f"FROM null('id {database}.UserId')"
        )
        assert "Missing" in rejected or "UNKNOWN_TYPE" in rejected, rejected
        assert_absent(database, "partial")
        assert f"{database}.UserId" in q(f"SHOW CREATE VIEW {database}.mixed")

        node.restart_clickhouse()
        shown = q(f"SHOW CREATE VIEW {database}.mixed")
        assert shown.count(f"{database}.UserId") >= 4, shown
    finally:
        run_cleanup_steps(lambda: q(f"DROP DATABASE IF EXISTS {database} SYNC"))


def test_declared_output_with_physical_auxiliary_endpoints_survives_restart(
    started_cluster,
):
    database = unique_database("declared_physical_auxiliary")
    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(
            f"CREATE VIEW {database}.declared (id {database}.UserId) AS "
            "SELECT CAST(id AS UInt64) AS id FROM null('id UInt64')"
        )

        shown = q(f"SHOW CREATE VIEW {database}.declared FORMAT TSVRaw")
        assert f"{database}.UserId" in shown, shown
        assert "CAST(id, 'UInt64')" in shown or "CAST(id AS UInt64)" in shown, shown
        assert q(f"SELECT count() FROM {database}.declared").strip() == "0"

        node.restart_clickhouse()
        shown = q(f"SHOW CREATE VIEW {database}.declared FORMAT TSVRaw")
        assert f"{database}.UserId" in shown, shown
        assert "CAST(id, 'UInt64')" in shown or "CAST(id AS UInt64)" in shown, shown
        assert q(f"SELECT count() FROM {database}.declared").strip() == "0"
    finally:
        run_cleanup_steps(lambda: q(f"DROP DATABASE IF EXISTS {database} SYNC"))


def test_context_owned_eval_unknown_and_malformed_sources_reject_before_mutation(
    started_cluster,
):
    database = unique_database("reject")
    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        statements = {
            "values_context": (
                f"CREATE VIEW {database}.values_context AS "
                f"SELECT * FROM values('id {database}.UserId', (1))"
            ),
            "url_adapter": (
                f"CREATE VIEW {database}.url_adapter AS "
                "SELECT * FROM url('http://127.0.0.1:1/no-request', 'CSV', "
                f"'id {database}.UserId')"
            ),
            "s3_adapter": (
                f"CREATE VIEW {database}.s3_adapter AS "
                "SELECT * FROM s3('http://127.0.0.1:1/no-request', 'CSV', "
                f"'id {database}.UserId')"
            ),
            "named_collection": (
                f"CREATE VIEW {database}.named_collection AS "
                "SELECT * FROM s3(missing_named_collection)"
            ),
            "mongodb_adapter": (
                f"CREATE VIEW {database}.mongodb_adapter AS "
                "SELECT * FROM mongodb('127.0.0.1:1', 'db', 'collection', "
                f"'user', 'password', 'id {database}.UserId')"
            ),
            "eval_source": (
                f"CREATE VIEW {database}.eval_source AS "
                "SELECT * FROM eval('SELECT 1 AS id')"
            ),
            "unknown_source": (
                f"CREATE VIEW {database}.unknown_source AS "
                "SELECT * FROM unclassified_source()"
            ),
            "unknown_nested": (
                f"CREATE VIEW {database}.unknown_nested AS "
                f"SELECT * FROM loop(unclassified_nested_source('id {database}.UserId'))"
            ),
            "malformed_loop": (
                f"CREATE VIEW {database}.malformed_loop AS "
                f"SELECT * FROM loop(`null`('id {database}.UserId'), "
                f"`null`('id {database}.UserId'), `null`('id {database}.UserId'))"
            ),
            "scalar_loop": (
                f"CREATE VIEW {database}.scalar_loop AS "
                f"SELECT * FROM loop('{database}', 'physical_source') LIMIT 0"
            ),
        }
        q(
            f"CREATE TABLE {database}.physical_source (id UInt64) "
            "ENGINE = MergeTree ORDER BY id"
        )

        for object_name, statement in statements.items():
            assert_stored_object_rejected(statement)
            assert_absent(database, object_name)
        assert q(f"SELECT count() FROM {database}.physical_source").strip() == "0"
    finally:
        run_cleanup_steps(lambda: q(f"DROP DATABASE IF EXISTS {database} SYNC"))


def test_exact_source_authority_view_mv_dictionary_rename_access_and_restart(
    started_cluster,
):
    database = unique_database("objects")
    reader = f"udt_stored_object_reader_{uuid.uuid4().hex[:8]}"
    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(
            f"CREATE TABLE {database}.source "
            f"(id {database}.UserId, value String) ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.source VALUES (1, 'one'), (2, 'two')")

        q(
            f"CREATE VIEW {database}.mapped_view AS "
            f"SELECT id, value FROM {database}.source"
        )
        q(
            f"CREATE MATERIALIZED VIEW {database}.mapped_mv "
            "ENGINE = MergeTree ORDER BY id AS "
            f"SELECT id, value FROM {database}.source"
        )
        q(
            f"CREATE DICTIONARY {database}.mapped_dictionary "
            f"(id {database}.UserId, value String) PRIMARY KEY id "
            "SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' "
            f"DB '{database}' TABLE 'source')) LIFETIME(0) LAYOUT(FLAT())"
        )

        assert q(f"SELECT sum(id) FROM {database}.mapped_view").strip() == "3"
        q(f"INSERT INTO {database}.source VALUES (3, 'three')")
        assert q(f"SELECT sum(id) FROM {database}.mapped_mv").strip() == "3"
        for object_name in ("mapped_view", "mapped_mv", "mapped_dictionary"):
            assert f"{database}.UserId" in q(
                f"SHOW CREATE {('DICTIONARY' if object_name == 'mapped_dictionary' else 'TABLE')} "
                f"{database}.{object_name}"
            )

        mapped_mv_uuid = object_uuid(database, "mapped_mv")
        mapped_dictionary_uuid = object_uuid(database, "mapped_dictionary")
        node.restart_clickhouse()
        assert q(f"SELECT sum(id) FROM {database}.mapped_view").strip() == "6"
        assert q(f"SELECT sum(id) FROM {database}.mapped_mv").strip() == "3"

        q(f"RENAME TABLE {database}.mapped_view TO {database}.renamed_view")
        q(f"RENAME TABLE {database}.mapped_mv TO {database}.renamed_mv")
        q(
            f"RENAME DICTIONARY {database}.mapped_dictionary "
            f"TO {database}.renamed_dictionary"
        )
        assert f"{database}.UserId" in q(
            f"SHOW CREATE VIEW {database}.renamed_view"
        )
        assert f"{database}.UserId" in show_create(
            database, "renamed_mv", "MATERIALIZED VIEW"
        )
        assert f"{database}.UserId" in show_create(
            database, "renamed_dictionary", "DICTIONARY"
        )
        assert object_uuid(database, "renamed_mv") == mapped_mv_uuid
        assert (
            object_uuid(database, "renamed_dictionary") == mapped_dictionary_uuid
        )
        assert_absent(database, "mapped_mv")
        assert_absent(database, "mapped_dictionary")

        q(f"INSERT INTO {database}.source VALUES (4, 'four')")
        q(f"SYSTEM RELOAD DICTIONARY {database}.renamed_dictionary")
        assert q(f"SELECT sum(id) FROM {database}.renamed_mv").strip() == "7"
        assert dictionary_value(database, "renamed_dictionary", 4) == "four"

        node.restart_clickhouse()
        assert object_uuid(database, "renamed_mv") == mapped_mv_uuid
        assert (
            object_uuid(database, "renamed_dictionary") == mapped_dictionary_uuid
        )
        assert f"{database}.UserId" in show_create(
            database, "renamed_mv", "MATERIALIZED VIEW"
        )
        assert f"{database}.UserId" in show_create(
            database, "renamed_dictionary", "DICTIONARY"
        )
        q(f"INSERT INTO {database}.source VALUES (5, 'five')")
        q(f"SYSTEM RELOAD DICTIONARY {database}.renamed_dictionary")
        assert q(f"SELECT sum(id) FROM {database}.renamed_mv").strip() == "12"
        assert dictionary_value(database, "renamed_dictionary", 5) == "five"
        restrict = query_error(f"DROP TYPE {database}.UserId RESTRICT")
        assert "dependent" in restrict.lower(), restrict

        q(f"CREATE USER {reader} IDENTIFIED WITH no_password")
        q(f"GRANT SELECT ON {database}.source TO {reader}")
        q(f"GRANT SELECT ON {database}.renamed_view TO {reader}")
        q(f"GRANT SELECT ON system.user_defined_types TO {reader}")
        assert q(
            f"SELECT sum(id) FROM {database}.renamed_view", user=reader
        ).strip() == "15"
        assert q(
            "SELECT count() FROM system.user_defined_types "
            f"WHERE database = '{database}'",
            user=reader,
        ).strip() == "0"
    finally:
        run_cleanup_steps(
            lambda: q(f"DROP USER IF EXISTS {reader}"),
            lambda: q(f"DROP DATABASE IF EXISTS {database} SYNC"),
        )


def test_dictionary_read_reload_restart_and_drop_releases_dependency(started_cluster):
    database = unique_database("dictionary_runtime")
    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(
            f"CREATE TABLE {database}.source (id UInt64, value String) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.source VALUES (1, 'one'), (2, 'two')")
        q(
            f"CREATE DICTIONARY {database}.mapped_dictionary "
            f"(id {database}.UserId, value String DEFAULT '') PRIMARY KEY id "
            "SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' "
            f"DB '{database}' TABLE 'source')) LIFETIME(0) LAYOUT(FLAT())"
        )

        assert dictionary_value(database, "mapped_dictionary", 1) == "one"
        q(f"INSERT INTO {database}.source VALUES (3, 'three')")
        q(f"SYSTEM RELOAD DICTIONARY {database}.mapped_dictionary")
        assert dictionary_value(database, "mapped_dictionary", 3) == "three"

        node.restart_clickhouse()
        assert dictionary_value(database, "mapped_dictionary", 2) == "two"
        assert dictionary_value(database, "mapped_dictionary", 3) == "three"
        assert f"{database}.UserId" in show_create(
            database, "mapped_dictionary", "DICTIONARY"
        )

        restricted = query_error(f"DROP TYPE {database}.UserId RESTRICT")
        assert "dependent" in restricted.lower() or "refer" in restricted.lower()
        q(f"DROP DICTIONARY {database}.mapped_dictionary")
        q(f"DROP TYPE {database}.UserId RESTRICT")
        assert (
            q(
                "SELECT count() FROM system.user_defined_types "
                f"WHERE database = '{database}' AND name = 'UserId'"
            ).strip()
            == "0"
        )
    finally:
        run_cleanup_steps(lambda: q(f"DROP DATABASE IF EXISTS {database} SYNC"))


def test_dictionary_config_reload_detach_attach_and_drop_release_dependency(
    started_cluster,
):
    database = unique_database("dictionary_config_reload")
    failpoint_enabled = False
    pending_requests = {}

    def start_request(name, sql, query_id):
        request = node.get_query_request(
            sql,
            settings=ENABLED,
            query_id=query_id,
            timeout=120,
        )
        pending_requests[name] = request
        return request

    def finish_request(name):
        request = pending_requests.pop(name)
        return request.get_answer_and_error()

    def disable_failpoint():
        nonlocal failpoint_enabled
        if failpoint_enabled and node.get_process_pid("clickhouse") is not None:
            q(f"SYSTEM DISABLE FAILPOINT {DICTIONARY_REPOSITORY_FAILPOINT}")
        failpoint_enabled = False

    def finish_pending_requests():
        first_error = None
        for name, request in list(pending_requests.items()):
            try:
                request.get_answer_and_error()
            except Exception as error:
                if first_error is None:
                    first_error = RuntimeError(
                        f"failed to finish pending {name} request: {error}"
                    )
            finally:
                pending_requests.pop(name, None)
        if first_error is not None:
            raise first_error

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(
            f"CREATE TABLE {database}.source (id UInt64, value String) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.source VALUES (1, 'one'), (2, 'two')")
        q(
            f"CREATE DICTIONARY {database}.mapped_dictionary "
            f"(id {database}.UserId, value String DEFAULT '') PRIMARY KEY id "
            "SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' "
            f"DB '{database}' TABLE 'source')) LIFETIME(0) LAYOUT(FLAT())"
        )
        dictionary_uuid = object_uuid(database, "mapped_dictionary")
        assert dictionary_value(database, "mapped_dictionary", 1) == "one"

        q(f"INSERT INTO {database}.source VALUES (3, 'three')")
        alter_query_id = (
            f"{DICTIONARY_REPOSITORY_QUERY_ID_PREFIX}{uuid.uuid4().hex}"
        )
        q(f"SYSTEM ENABLE FAILPOINT {DICTIONARY_REPOSITORY_FAILPOINT}")
        failpoint_enabled = True
        start_request(
            "alter",
            f"ALTER TABLE {database}.mapped_dictionary "
            "MODIFY COMMENT 'coordinated repository reload'",
            alter_query_id,
        )
        q(
            f"SYSTEM WAIT FAILPOINT {DICTIONARY_REPOSITORY_FAILPOINT} PAUSE",
            timeout=30,
        )
        assert (
            q(
                "SELECT count() FROM system.processes "
                f"WHERE query_id = '{alter_query_id}'"
            ).strip()
            == "1"
        )

        # The repository is paused after live authority admission but before
        # the new configuration pointer is published to ExternalLoader.
        # Explicit reload must fail closed on that exact split image without
        # waiting for the config-reader lock or dereferencing stale storage.
        reload_query_id = f"udt_dictionary_reload_{uuid.uuid4().hex}"
        start_request(
            "dictionary reload",
            f"SYSTEM RELOAD DICTIONARY {database}.mapped_dictionary",
            reload_query_id,
        )
        _, reload_error = finish_request("dictionary reload")
        assert "ABORTED" in reload_error, reload_error
        assert "exact live mapped StorageDictionary image" in reload_error

        # Both operations below must enter before the pause is released.
        # RELOAD CONFIG waits on the config-reader lock; DETACH waits for the
        # live repository reload/DDL owner and later removes that repository.
        config_query_id = f"udt_dictionary_config_reload_{uuid.uuid4().hex}"
        detach_query_id = f"udt_dictionary_detach_{uuid.uuid4().hex}"
        start_request("config reload", "SYSTEM RELOAD CONFIG", config_query_id)
        start_request(
            "detach",
            f"DETACH DICTIONARY {database}.mapped_dictionary",
            detach_query_id,
        )
        observed_overlap = node.query_with_retry(
            "SELECT count() FROM system.processes WHERE query_id IN "
            f"('{config_query_id}', '{detach_query_id}')",
            check_callback=lambda result: result.strip() == "2",
            retry_count=100,
            sleep_time=0.05,
        )
        assert observed_overlap.strip() == "2"

        disable_failpoint()
        for request_name in ("alter", "config reload", "detach"):
            _, request_error = finish_request(request_name)
            assert not request_error, (request_name, request_error)

        assert_absent(database, "mapped_dictionary")
        detached_show_error = query_error(
            f"SHOW CREATE DICTIONARY {database}.mapped_dictionary"
        )
        assert "NOT_IMPLEMENTED" in detached_show_error, detached_show_error
        assert "detached mapped object" in detached_show_error, detached_show_error
        assert "exact live user-defined type binding" in detached_show_error, detached_show_error
        q("SYSTEM RELOAD CONFIG")
        assert_absent(database, "mapped_dictionary")
        restricted = query_error(f"DROP TYPE {database}.UserId RESTRICT")
        assert "dependent" in restricted.lower() or "refer" in restricted.lower()

        q(f"ATTACH DICTIONARY {database}.mapped_dictionary")
        assert object_uuid(database, "mapped_dictionary") == dictionary_uuid
        q(f"SYSTEM RELOAD DICTIONARY {database}.mapped_dictionary")
        assert dictionary_value(database, "mapped_dictionary", 3) == "three"
        q(f"DROP DICTIONARY {database}.mapped_dictionary")
        assert_absent(database, "mapped_dictionary")
        q("SYSTEM RELOAD CONFIG")
        assert_absent(database, "mapped_dictionary")
        q(f"DROP TYPE {database}.UserId RESTRICT")
    finally:
        run_cleanup_steps(
            disable_failpoint,
            finish_pending_requests,
            lambda: q(f"DROP DATABASE IF EXISTS {database} SYNC"),
        )


def test_materialized_view_to_populate_modify_query_failure_and_restart(
    started_cluster,
):
    database = unique_database("mv_surfaces")
    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(
            f"CREATE TABLE {database}.populate_source (id UInt64) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.populate_source VALUES (1), (2)")
        q(
            f"CREATE MATERIALIZED VIEW {database}.populated "
            f"(id {database}.UserId) ENGINE = MergeTree ORDER BY id POPULATE AS "
            f"SELECT id FROM {database}.populate_source"
        )
        assert (
            q(f"SELECT arraySort(groupArray(id)) FROM {database}.populated").strip()
            == "[1,2]"
        )
        q(f"INSERT INTO {database}.populate_source VALUES (3)")
        assert q(f"SELECT count(), sum(id) FROM {database}.populated").strip() == "3\t6"

        # Exercise the opposite canonical pair order as a regression for the
        # UDT `CREATE` guard handoff: here `view_name < source_name`.
        q(
            f"CREATE TABLE {database}.z_populate_source (id UInt64) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.z_populate_source VALUES (5), (6)")
        q(
            f"CREATE MATERIALIZED VIEW {database}.a_populated "
            f"(id {database}.UserId) ENGINE = MergeTree ORDER BY id POPULATE AS "
            f"SELECT id FROM {database}.z_populate_source"
        )
        assert (
            q(f"SELECT arraySort(groupArray(id)) FROM {database}.a_populated").strip()
            == "[5,6]"
        )
        q(f"INSERT INTO {database}.z_populate_source VALUES (7)")
        assert q(f"SELECT count(), sum(id) FROM {database}.a_populated").strip() == "3\t18"

        populate_failure = query_error(
            f"CREATE MATERIALIZED VIEW {database}.bad_populated "
            f"(id {database}.UserId) ENGINE = MergeTree ORDER BY id POPULATE AS "
            f"SELECT concat('not-a-number-', toString(id)) AS id "
            f"FROM {database}.populate_source"
        )
        assert any(
            word in populate_failure.lower()
            for word in ("output", "physical", "type", "cannot parse")
        )
        assert_absent(database, "bad_populated")

        for table in ("mapped_a", "mapped_b"):
            q(
                f"CREATE TABLE {database}.{table} (id {database}.UserId) "
                "ENGINE = MergeTree ORDER BY id"
            )
        q(
            f"CREATE TABLE {database}.physical_source (id UInt64) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(
            f"CREATE TABLE {database}.target (id UInt64, origin String) "
            "ENGINE = MergeTree ORDER BY (id, origin)"
        )
        q(
            f"CREATE MATERIALIZED VIEW {database}.routed TO {database}.target "
            f"(id {database}.UserId, origin String) AS "
            f"SELECT id, 'a' AS origin FROM {database}.mapped_a"
        )
        q(
            f"CREATE VIEW {database}.mapped_view AS "
            f"SELECT id FROM {database}.mapped_b"
        )

        q(f"INSERT INTO {database}.mapped_a VALUES (10)")
        q(
            f"ALTER TABLE {database}.routed MODIFY QUERY "
            f"SELECT id, 'b' AS origin FROM {database}.mapped_b"
        )
        q(f"INSERT INTO {database}.mapped_a VALUES (11)")
        q(f"INSERT INTO {database}.mapped_b VALUES (20)")
        assert q(
            f"SELECT id, origin FROM {database}.target ORDER BY id FORMAT TSV"
        ) == "10\ta\n20\tb\n"

        mismatch = query_error(
            f"ALTER TABLE {database}.a_populated MODIFY QUERY "
            f"SELECT toString(id) AS id FROM {database}.z_populate_source"
        )
        assert any(word in mismatch.lower() for word in ("output", "physical", "type"))
        assert f"{database}.z_populate_source" in show_create(
            database, "a_populated", "MATERIALIZED VIEW"
        )

        ordinary_view_error = query_error(
            f"ALTER TABLE {database}.mapped_view MODIFY QUERY "
            f"SELECT CAST(id AS {database}.UserId) AS id FROM {database}.mapped_b"
        )
        normalized_error = ordinary_view_error.lower().replace(" ", "")
        assert "materializedview" in normalized_error or "not_implemented" in normalized_error
        assert q(f"SELECT sum(id) FROM {database}.mapped_view").strip() == "20"

        node.restart_clickhouse()
        q(f"INSERT INTO {database}.populate_source VALUES (4)")
        q(f"INSERT INTO {database}.z_populate_source VALUES (8)")
        q(f"INSERT INTO {database}.mapped_b VALUES (21)")
        assert q(f"SELECT sum(id) FROM {database}.mapped_view").strip() == "41"
        assert q(
            f"SELECT id, origin FROM {database}.target ORDER BY id FORMAT TSV"
        ) == "10\ta\n20\tb\n21\tb\n"
        assert q(f"SELECT count(), sum(id) FROM {database}.populated").strip() == "4\t10"
        assert q(f"SELECT count(), sum(id) FROM {database}.a_populated").strip() == "4\t26"

        q(
            f"ALTER TABLE {database}.routed MODIFY QUERY "
            f"SELECT id, 'physical' AS origin FROM {database}.physical_source"
        )
        routed_create = show_create(database, "routed", "MATERIALIZED VIEW")
        assert f"{database}.UserId" not in routed_create
        q(f"INSERT INTO {database}.physical_source VALUES (30)")
        q(f"INSERT INTO {database}.mapped_b VALUES (22)")
        assert q(
            f"SELECT count(), sum(id), countIf(origin = 'physical') "
            f"FROM {database}.target"
        ).strip() == "4\t81\t1"

        node.restart_clickhouse()
        q(f"INSERT INTO {database}.physical_source VALUES (31)")
        assert q(
            f"SELECT count(), sum(id), countIf(origin = 'physical') "
            f"FROM {database}.target"
        ).strip() == "5\t112\t2"
    finally:
        run_cleanup_steps(lambda: q(f"DROP DATABASE IF EXISTS {database} SYNC"))


def test_type_rename_identity_and_physicalize_every_stored_object_kind(
    started_cluster,
):
    database = unique_database("physicalize_objects")
    objects = (
        ("VIEW", "mapped_view"),
        ("MATERIALIZED VIEW", "mapped_mv"),
        ("DICTIONARY", "mapped_dictionary"),
    )
    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        original_type_uuid = type_uuid(database, "UserId")
        q(
            f"CREATE TABLE {database}.source (id UInt64, value String) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(
            f"CREATE TABLE {database}.target (id UInt64, value String) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.source VALUES (1, 'one'), (2, 'two')")
        q(
            f"CREATE VIEW {database}.mapped_view "
            f"(id {database}.UserId, value String) AS "
            f"SELECT id, value FROM {database}.source"
        )
        q(
            f"CREATE MATERIALIZED VIEW {database}.mapped_mv TO {database}.target "
            f"(id {database}.UserId, value String) AS "
            f"SELECT id, value FROM {database}.source"
        )
        q(
            f"CREATE DICTIONARY {database}.mapped_dictionary "
            f"(id {database}.UserId, value String DEFAULT '') PRIMARY KEY id "
            "SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' "
            f"DB '{database}' TABLE 'source')) LIFETIME(0) LAYOUT(FLAT())"
        )
        original_object_uuids = {
            name: object_uuid(database, name) for _, name in objects
        }
        assert dictionary_value(database, "mapped_dictionary", 1) == "one"

        q(f"ALTER TYPE {database}.UserId RENAME TO PrincipalId")
        assert type_uuid(database, "PrincipalId") == original_type_uuid
        q(f"CREATE TYPE {database}.UserId AS String")
        assert type_uuid(database, "UserId") != original_type_uuid
        for kind, name in objects:
            shown = show_create(database, name, kind)
            assert f"{database}.PrincipalId" in shown, shown
            assert f"{database}.UserId" not in shown, shown
            assert object_uuid(database, name) == original_object_uuids[name]

        q(f"INSERT INTO {database}.source VALUES (3, 'three')")
        q(f"SYSTEM RELOAD DICTIONARY {database}.mapped_dictionary")
        assert q(f"SELECT sum(id) FROM {database}.mapped_view").strip() == "6"
        assert q(f"SELECT sum(id) FROM {database}.target").strip() == "3"
        assert dictionary_value(database, "mapped_dictionary", 3) == "three"

        node.restart_clickhouse()
        for kind, name in objects:
            shown = show_create(database, name, kind)
            assert f"{database}.PrincipalId" in shown, shown
            assert object_uuid(database, name) == original_object_uuids[name]
        assert dictionary_value(database, "mapped_dictionary", 2) == "two"
        restricted = query_error(f"DROP TYPE {database}.PrincipalId RESTRICT")
        assert "dependent" in restricted.lower() or "refer" in restricted.lower()

        for kind, name in objects:
            plan = physicalization_dry_run(f"OBJECT {kind} {database}.{name}")
            assert name in plan["loss_summary"]
            q(
                "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
                + sql_string(plan["apply_token"])
            )
            assert f"{database}.PrincipalId" not in show_create(
                database, name, kind
            )

        assert q(f"SELECT sum(id) FROM {database}.mapped_view").strip() == "6"
        assert dictionary_value(database, "mapped_dictionary", 3) == "three"
        q(f"DROP TYPE {database}.PrincipalId RESTRICT")

        node.restart_clickhouse()
        q(f"INSERT INTO {database}.source VALUES (4, 'four')")
        q(f"SYSTEM RELOAD DICTIONARY {database}.mapped_dictionary")
        assert q(f"SELECT sum(id) FROM {database}.mapped_view").strip() == "10"
        assert q(f"SELECT sum(id) FROM {database}.target").strip() == "7"
        assert dictionary_value(database, "mapped_dictionary", 4) == "four"
        for kind, name in objects:
            assert f"{database}.PrincipalId" not in show_create(
                database, name, kind
            )
    finally:
        run_cleanup_steps(lambda: q(f"DROP DATABASE IF EXISTS {database} SYNC"))


def test_stored_objects_execute_and_reload_after_feature_disabled_restart(
    started_cluster,
):
    database = unique_database("disabled_restart")
    config_disabled = False
    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(
            f"CREATE TABLE {database}.source (id UInt64, value String) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(
            f"CREATE TABLE {database}.target (id UInt64, value String) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.source VALUES (1, 'one'), (2, 'two')")
        q(
            f"CREATE VIEW {database}.mapped_view "
            f"(id {database}.UserId, value String) AS "
            f"SELECT id, value FROM {database}.source"
        )
        q(
            f"CREATE MATERIALIZED VIEW {database}.mapped_mv TO {database}.target "
            f"(id {database}.UserId, value String) AS "
            f"SELECT id, value FROM {database}.source"
        )
        q(
            f"CREATE DICTIONARY {database}.mapped_dictionary "
            f"(id {database}.UserId, value String DEFAULT '') PRIMARY KEY id "
            "SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' "
            f"DB '{database}' TABLE 'source')) LIFETIME(0) LAYOUT(FLAT())"
        )
        assert dictionary_value(database, "mapped_dictionary", 1) == "one"

        node.replace_in_config(CONFIG, SETTING_ON, SETTING_OFF)
        config_disabled = True
        node.restart_clickhouse()

        assert (
            q(f"SELECT sum(id) FROM {database}.mapped_view", settings={}).strip()
            == "3"
        )
        assert (
            dictionary_value(
                database, "mapped_dictionary", 2, settings={}
            )
            == "two"
        )
        for kind, name in (
            ("VIEW", "mapped_view"),
            ("MATERIALIZED VIEW", "mapped_mv"),
            ("DICTIONARY", "mapped_dictionary"),
        ):
            assert f"{database}.UserId" in show_create(
                database, name, kind, settings={}
            )

        q(f"INSERT INTO {database}.source VALUES (3, 'three')", settings={})
        q(
            f"SYSTEM RELOAD DICTIONARY {database}.mapped_dictionary",
            settings={},
        )
        assert q(f"SELECT sum(id) FROM {database}.target", settings={}).strip() == "3"
        assert (
            dictionary_value(
                database, "mapped_dictionary", 3, settings={}
            )
            == "three"
        )

        disabled = query_error(
            f"CREATE VIEW {database}.rejected "
            f"(id {database}.UserId) AS SELECT toUInt64(1) AS id",
            settings={},
        )
        assert "disabled" in disabled.lower()
        assert_absent(database, "rejected")
    finally:
        def restore_feature_config():
            if config_disabled:
                node.replace_in_config(CONFIG, SETTING_OFF, SETTING_ON)
                node.restart_clickhouse()

        run_cleanup_steps(
            restore_feature_config,
            lambda: q(f"DROP DATABASE IF EXISTS {database} SYNC"),
        )


def test_create_as_clone_cross_database_and_like_boundaries(started_cluster):
    database = unique_database("copy")
    other = unique_database("other")
    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE DATABASE {other} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(
            f"CREATE TABLE {database}.mapped "
            f"(id {database}.UserId, value String) ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.mapped VALUES (1, 'one'), (2, 'two')")

        q(f"CREATE TABLE {database}.copy AS {database}.mapped ENGINE = Memory")
        q(f"CREATE TABLE {database}.clone CLONE AS {database}.mapped")
        for table in ("copy", "clone"):
            assert f"{database}.UserId" in q(f"SHOW CREATE TABLE {database}.{table}")

        q(f"CREATE TABLE {database}.from_function AS null('id {database}.UserId')")
        assert physical_column(database, "from_function") == "UInt64"
        assert f"{database}.UserId" not in q(
            f"SHOW CREATE TABLE {database}.from_function"
        )

        cross_statements = (
            f"CREATE TABLE {other}.copy AS {database}.mapped ENGINE = Memory",
            f"CREATE TABLE {other}.clone CLONE AS {database}.mapped",
            f"CREATE VIEW {other}.mapped_view AS SELECT id FROM {database}.mapped",
        )
        for statement in cross_statements:
            result = query_error(statement)
            assert "database" in result.lower() or "authority" in result.lower(), result

        q(
            f"CREATE VIEW {other}.physical_view AS "
            f"SELECT toUInt64(id) AS id FROM {database}.mapped"
        )
        assert q(f"SELECT sum(id) FROM {other}.physical_view").strip() == "3"

        like_error = query_error(
            f"CREATE TABLE {database}.like_table LIKE {database}.mapped"
        )
        assert "syntax" in like_error.lower(), like_error
        assert_absent(database, "like_table")

        node.restart_clickhouse()
        assert physical_column(database, "from_function") == "UInt64"
        assert q(f"SELECT sum(id) FROM {database}.clone").strip() == "3"
    finally:
        run_cleanup_steps(
            lambda: q(f"DROP DATABASE IF EXISTS {other} SYNC"),
            lambda: q(f"DROP DATABASE IF EXISTS {database} SYNC"),
        )


@pytest.mark.parametrize("failpoint", PUBLICATION_FAILPOINTS)
def test_publication_failures_leave_no_partial_view(started_cluster, failpoint):
    database = unique_database("publication")
    enabled = False
    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(
            f"CREATE TABLE {database}.source "
            f"(id {database}.UserId) ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.source VALUES (7)")
        if failpoint == "udt_schema_storage_temp_write_failure":
            # This low-level ONCE failpoint is shared with the periodic verifier's
            # cursor persistence.  Wait until that verifier has completed a clean
            # batch so the foreground publication is the next storage writer.
            wait_for_clean_verification(database)
        q(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        enabled = True
        result = query_error(
            f"CREATE VIEW {database}.failed AS SELECT id FROM {database}.source"
        )
        assert "fault" in result.lower() or "fail" in result.lower(), result
        assert_absent(database, "failed")
        q(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        enabled = False

        # Retry the exact failed name: successful `CREATE` proves that publication rollback
        # did not leave catalog metadata that blocks a subsequent lifecycle.
        q(f"CREATE VIEW {database}.failed AS SELECT id FROM {database}.source")
        failed_uuid = object_uuid(database, "failed")
        assert f"{database}.UserId" in q(
            f"SHOW CREATE VIEW {database}.failed"
        )
        assert q(f"SELECT sum(id) FROM {database}.failed").strip() == "7"
        node.restart_clickhouse()
        assert object_uuid(database, "failed") == failed_uuid
        assert f"{database}.UserId" in q(f"SHOW CREATE VIEW {database}.failed")
        assert q(f"SELECT sum(id) FROM {database}.failed").strip() == "7"
        q(f"DROP VIEW {database}.failed")
        assert_absent(database, "failed")

        # The scaffold-cleanup logger has no public failure injection seam. If
        # another exercised rollback emitted it, enforce its bounded payload.
        cleanup_lines = node.grep_in_log(
            "Failed to clean an uncommitted UDT authority scaffold"
        )
        for line in cleanup_lines.splitlines():
            assert database not in line
            assert "CREATE VIEW" not in line
            assert "/metadata/" not in line
            assert re.search(r"database UUID [0-9a-f-]+ \(error code -?[0-9]+\)", line)
    finally:
        cleanup_steps = []
        if enabled:
            cleanup_steps.append(
                lambda: q(f"SYSTEM DISABLE FAILPOINT {failpoint}")
            )
        cleanup_steps.append(
            lambda: q(f"DROP DATABASE IF EXISTS {database} SYNC")
        )
        run_cleanup_steps(*cleanup_steps)


def test_corrupt_dictionary_sidecar_during_reload_is_repaired_from_local_wal(
    started_cluster,
):
    database = unique_database("corrupt_dictionary")
    reference_path = None
    original = None
    committed = None
    artifact_mutated = False
    repair_verified = False
    failpoint_enabled = False
    repository_reload_request = None
    repository_reload_finished = False

    def disable_failpoint():
        nonlocal failpoint_enabled
        if failpoint_enabled and node.get_process_pid("clickhouse") is not None:
            q(f"SYSTEM DISABLE FAILPOINT {DICTIONARY_REPOSITORY_FAILPOINT}")
        failpoint_enabled = False

    def finish_repository_reload():
        nonlocal repository_reload_finished
        if (
            repository_reload_request is not None
            and not repository_reload_finished
        ):
            try:
                repository_reload_request.get_answer_and_error()
            finally:
                repository_reload_finished = True

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(
            f"CREATE TABLE {database}.source (id UInt64, value String) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(
            f"INSERT INTO {database}.source "
            "SELECT number, toString(number) FROM numbers(40)"
        )
        q(
            f"CREATE DICTIONARY {database}.mapped_dictionary "
            f"(id {database}.UserId, value String DEFAULT '') PRIMARY KEY id "
            "SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' "
            f"DB '{database}' TABLE 'source')) "
            "LIFETIME(0) LAYOUT(FLAT())"
        )
        dictionary_uuid = object_uuid(database, "mapped_dictionary")
        assert dictionary_value(database, "mapped_dictionary", 1) == "1"

        reference_path = stored_object_reference_path(
            database, "mapped_dictionary"
        )
        original = read_file(reference_path)
        assert len(original) > 8

        repository_reload_query_id = (
            f"{DICTIONARY_REPOSITORY_QUERY_ID_PREFIX}{uuid.uuid4().hex}"
        )
        q(f"SYSTEM ENABLE FAILPOINT {DICTIONARY_REPOSITORY_FAILPOINT}")
        failpoint_enabled = True
        repository_reload_request = node.get_query_request(
            f"ALTER TABLE {database}.mapped_dictionary "
            "MODIFY COMMENT 'reload interrupted by restart'",
            settings=ENABLED,
            query_id=repository_reload_query_id,
            timeout=120,
        )
        q(
            f"SYSTEM WAIT FAILPOINT {DICTIONARY_REPOSITORY_FAILPOINT} PAUSE",
            timeout=30,
        )
        assert (
            q(
                "SELECT count() FROM system.processes "
                f"WHERE query_id = '{repository_reload_query_id}'"
            ).strip()
            == "1"
        )

        # The ALTER has durably committed before the repository reload starts.
        # The repair source is therefore this new WAL-backed image, not the
        # pre-ALTER sidecar captured above.
        committed = read_file(reference_path)
        assert committed != original
        corrupted = bytearray(committed)
        corrupted[-1] ^= 0x01

        # Replace, rather than rewrite, the artifact so the server can observe
        # either complete image but never test-induced partial bytes.
        artifact_mutated = True
        replace_file_atomically(reference_path, bytes(corrupted))
        assert read_file(reference_path) == bytes(corrupted)

        # Kill the server at the exact live repository-load boundary. Rewrite
        # and verify the damaged image once the process is gone, closing the
        # race with verification/repair work before the next startup.
        node.stop_clickhouse(kill=True)
        assert node.get_process_pid("clickhouse") is None
        failpoint_enabled = False
        replace_file_atomically(reference_path, bytes(corrupted))
        assert read_file(reference_path) == bytes(corrupted)

        try:
            repository_reload_request.get_error()
        finally:
            repository_reload_finished = True
        node.start_clickhouse()

        # Dictionaries are ordinary repairable stored objects in the authority
        # inventory. Since the exact local WAL image is still available, the
        # restart must repair it instead of retaining an inactive object.
        assert read_file(reference_path) == committed
        assert object_uuid(database, "mapped_dictionary") == dictionary_uuid
        assert f"{database}.UserId" in show_create(
            database, "mapped_dictionary", "DICTIONARY"
        )
        assert dictionary_value(database, "mapped_dictionary", 1) == "1"
        assert dictionary_value(database, "mapped_dictionary", 2) == "2"

        # The repair is a durable WAL transition, not process-local tolerance
        # of the damaged image.
        node.restart_clickhouse()
        assert read_file(reference_path) == committed
        assert object_uuid(database, "mapped_dictionary") == dictionary_uuid
        assert dictionary_value(database, "mapped_dictionary", 2) == "2"
        repair_verified = True

        restricted = query_error(f"DROP TYPE {database}.UserId RESTRICT")
        assert "dependent" in restricted.lower() or "refer" in restricted.lower()
        q(f"DROP DICTIONARY {database}.mapped_dictionary")
        q(f"DROP TYPE {database}.UserId RESTRICT")
    finally:
        def restore_artifact_and_server():
            if artifact_mutated and not repair_verified:
                if node.get_process_pid("clickhouse") is not None:
                    node.stop_clickhouse(kill=True)
                replace_file_atomically(
                    reference_path, committed if committed is not None else original
                )
                node.start_clickhouse()
            elif node.get_process_pid("clickhouse") is None:
                node.start_clickhouse()

        run_cleanup_steps(
            disable_failpoint,
            finish_repository_reload,
            restore_artifact_and_server,
            lambda: q(f"DROP DATABASE IF EXISTS {database} SYNC"),
        )


def test_corrupt_view_sidecar_is_repaired_exactly_from_local_wal(started_cluster):
    database = unique_database("corrupt")
    reference_path = None
    original = None
    artifact_mutated = False
    repair_verified = False
    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(
            f"CREATE VIEW {database}.mapped AS "
            f"SELECT id FROM null('id {database}.UserId')"
        )
        reference_path = stored_object_reference_path(database, "mapped")
        original = read_file(reference_path)
        assert len(original) > 8
        corrupted = bytearray(original)
        corrupted[-1] ^= 0x01

        node.stop_clickhouse()
        # Set before writing the damaged artifact so a partial write or write
        # exception still takes the restoration path in `finally`.
        artifact_mutated = True
        replace_file_atomically(reference_path, bytes(corrupted))
        assert read_file(reference_path) == bytes(corrupted)
        node.start_clickhouse()

        assert read_file(reference_path) == original
        assert q(f"SELECT count() FROM {database}.mapped").strip() == "0"
        assert f"{database}.UserId" in q(f"SHOW CREATE VIEW {database}.mapped")

        # The repair is an ordinary durable schema-WAL transition, not a
        # process-local tolerance of corrupt bytes.
        node.restart_clickhouse()
        assert read_file(reference_path) == original
        assert q(f"SELECT count() FROM {database}.mapped").strip() == "0"
        repair_verified = True
    finally:
        def restore_artifact_and_server():
            if artifact_mutated and not repair_verified:
                if node.get_process_pid("clickhouse") is not None:
                    node.stop_clickhouse()
                replace_file_atomically(reference_path, original)
                node.start_clickhouse()
            elif node.get_process_pid("clickhouse") is None:
                node.start_clickhouse()

        run_cleanup_steps(
            restore_artifact_and_server,
            lambda: q(f"DROP DATABASE IF EXISTS {database} SYNC"),
        )
