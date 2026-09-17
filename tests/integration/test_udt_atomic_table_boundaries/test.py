"""table admission and physicalization real-server coverage for UDT table compatibility boundaries."""

import json
import shlex
import threading
import uuid

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/graphite_rollup.xml"],
    user_configs=["configs/udt.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)

ENABLED = {"allow_experimental_user_defined_types": 1}
CONFIG = "/etc/clickhouse-server/users.d/udt.xml"
SETTING_ON = "<allow_experimental_user_defined_types>1</allow_experimental_user_defined_types>"
SETTING_OFF = "<allow_experimental_user_defined_types>0</allow_experimental_user_defined_types>"
NIL_UUID = "00000000-0000-0000-0000-000000000000"
ALIAS_ANALYSIS_FAILPOINT = "udt_inferred_schema_pause_after_legacy_analysis"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown(ignore_fatal=True)


def q(sql, *, user="default", settings=ENABLED, database=None):
    return node.query(sql, user=user, settings=settings, database=database)


def error(sql, *, user="default", settings=ENABLED, database=None):
    result = node.query_and_get_error(
        sql, user=user, settings=settings, database=database
    )
    assert result, sql
    return result


def assert_error_contains(
    sql, *fragments, user="default", settings=ENABLED, database=None
):
    actual = error(
        sql, user=user, settings=settings, database=database
    ).lower()
    for fragment in fragments:
        assert fragment.lower() in actual, actual
    return actual


def rows_json(sql, *, user="default", settings=ENABLED):
    output = q(f"{sql} FORMAT JSONEachRow", user=user, settings=settings)
    return [json.loads(line) for line in output.split("\n") if line]


def unique_database(label):
    return f"udt_boundary_{label}_{uuid.uuid4().hex[:8]}"


def sql_string(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def native_sha256(sql):
    command = (
        "/usr/bin/clickhouse client --query "
        + shlex.quote(f"{sql} FORMAT Native")
        + " | sha256sum | cut -d' ' -f1"
    )
    return node.exec_in_container(
        ["bash", "-o", "pipefail", "-c", command]
    ).strip()


def test_backup_and_restore_fail_closed_for_mapped_tables(started_cluster):
    database = unique_database("backup")
    backup = f"udt_boundary_physical_{uuid.uuid4().hex}"
    physical_after_authority_backup = (
        f"udt_boundary_physical_after_authority_{uuid.uuid4().hex}"
    )
    mapped_backup = f"udt_boundary_mapped_{uuid.uuid4().hex}"
    database_backup = f"udt_boundary_database_{uuid.uuid4().hex}"

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(
            f"CREATE TABLE {database}.physical (id UInt64, value String) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.physical VALUES (10, 'ten'), (20, 'twenty')")

        # Positive control: the backup backend and ordinary Atomic restore path work.
        q(f"BACKUP TABLE {database}.physical TO File('{backup}') FORMAT Null")
        q(
            f"RESTORE TABLE {database}.physical AS {database}.restored "
            f"FROM File('{backup}') FORMAT Null"
        )
        assert q(f"SELECT sum(id) FROM {database}.restored").strip() == "30"

        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(
            f"CREATE TABLE {database}.mapped (id {database}.UserId, value String) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.mapped VALUES (1, 'one'), (2, 'two')")

        # Once an Atomic database owns active or durable UDT authority, the
        # database-wide backup/restore contract fails closed before selecting
        # individual tables. This protects one indivisible authority image.
        assert_error_contains(
            f"BACKUP TABLE {database}.physical "
            f"TO File('{physical_after_authority_backup}')",
            "backup of an active or durable atomic user-defined type authority",
        )
        assert_error_contains(
            f"BACKUP TABLE {database}.mapped TO File('{mapped_backup}')",
            "backup of an active or durable atomic user-defined type authority",
        )
        assert_error_contains(
            f"BACKUP DATABASE {database} TO File('{database_backup}')",
            "backup of an active or durable atomic user-defined type authority",
        )
        assert_error_contains(
            f"RESTORE TABLE {database}.physical AS {database}.blocked_restore "
            f"FROM File('{backup}')",
            "restore into an active or durable atomic user-defined type authority",
        )

        # Every rejection is pre-mutation: existing tables and provenance survive.
        assert q(f"EXISTS TABLE {database}.blocked_restore").strip() == "0"
        assert q(f"SELECT sum(id) FROM {database}.physical").strip() == "30"
        assert q(f"SELECT sum(id) FROM {database}.mapped").strip() == "3"
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table = 'mapped' "
            "AND udt_declared_type != ''"
        ).strip() == "1"
    finally:
        q(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_attach_and_detach_variants_preserve_exact_mapped_identity(started_cluster):
    database = unique_database("attach")

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(
            f"CREATE TABLE {database}.mapped (id {database}.UserId) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.mapped VALUES (1), (2)")
        q(
            f"CREATE TABLE {database}.physical (id UInt64) "
            "ENGINE = MergeTree ORDER BY id"
        )
        q(f"INSERT INTO {database}.physical VALUES (7)")

        mapped_uuid = q(
            "SELECT toString(uuid) FROM system.tables "
            f"WHERE database = '{database}' AND name = 'mapped'"
        ).strip()
        mapped_data_hash = native_sha256(
            f"SELECT * FROM {database}.mapped ORDER BY id"
        )
        mapped_provenance = q(
            "SELECT name, udt_declared_type, toString(udt_uuid) "
            "FROM system.columns "
            f"WHERE database = '{database}' AND table = 'mapped' "
            "ORDER BY position FORMAT TSV"
        )

        for statement, expected in (
            (
                f"DETACH TABLE {database}.mapped PERMANENTLY",
                "cannot detach permanently mapped table",
            ),
            (
                f"DETACH TABLE {database}.mapped ON CLUSTER 'missing_boundary_cluster'",
                "cannot detach on cluster mapped table",
            ),
        ):
            assert_error_contains(statement, expected)
            assert q(f"EXISTS TABLE {database}.mapped").strip() == "1"
            assert q(f"SELECT sum(id) FROM {database}.mapped").strip() == "3"

        q(f"DETACH TABLE {database}.mapped")
        assert q(f"EXISTS TABLE {database}.mapped").strip() == "0"
        assert_error_contains(
            f"SHOW CREATE TABLE {database}.mapped",
            "detached mapped object",
            "exact live user-defined type binding",
        )
        for suffix in ("AS NOT REPLICATED", "AS REPLICATED"):
            assert_error_contains(
                f"ATTACH TABLE {database}.mapped {suffix}",
                "only exact short attach after a temporary detach is supported",
            )
            assert q(f"EXISTS TABLE {database}.mapped").strip() == "0"

        q(f"ATTACH TABLE {database}.mapped")
        assert q(f"EXISTS TABLE {database}.mapped").strip() == "1"
        assert native_sha256(
            f"SELECT * FROM {database}.mapped ORDER BY id"
        ) == mapped_data_hash
        assert q(
            "SELECT toString(uuid) FROM system.tables "
            f"WHERE database = '{database}' AND name = 'mapped'"
        ).strip() == mapped_uuid
        assert q(
            "SELECT name, udt_declared_type, toString(udt_uuid) "
            "FROM system.columns "
            f"WHERE database = '{database}' AND table = 'mapped' "
            "ORDER BY position FORMAT TSV"
        ) == mapped_provenance

        assert_error_contains(
            f"DETACH DATABASE {database}",
            "cannot detach database",
            "mapped user-defined type tables",
        )
        assert_error_contains(
            f"DETACH DATABASE {database} PERMANENTLY",
            "detach permanently is not implemented for databases",
        )
        assert_error_contains(
            f"DETACH DATABASE {database} ON CLUSTER 'missing_boundary_cluster'",
            "cannot detach on cluster database",
            "mapped user-defined type tables",
        )
        assert q(f"EXISTS DATABASE {database}").strip() == "1"

        assert_error_contains(
            f"ATTACH TABLE {database}.manual (id {database}.UserId) ENGINE = Memory",
            "invalid logical provenance source",
        )
        assert q(f"EXISTS TABLE {database}.manual").strip() == "0"

        # The guard is scoped to mapped metadata; ordinary detach/attach remains intact.
        q(f"DETACH TABLE {database}.physical")
        assert q(f"EXISTS TABLE {database}.physical").strip() == "0"
        q(f"SHOW CREATE TABLE {database}.physical FORMAT Null")
        q(f"ATTACH TABLE {database}.physical")
        assert q(f"SELECT sum(id) FROM {database}.physical").strip() == "7"
    finally:
        q(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_engine_admission_matrix(started_cluster):
    database = unique_database("engines")
    supported = {
        "memory": (
            f"id {database}.UserId",
            "Memory",
            "(1), (2)",
        ),
        "merge_tree": (
            f"id {database}.UserId",
            "MergeTree ORDER BY id",
            "(1), (2)",
        ),
        "collapsing": (
            f"id {database}.UserId, sign Int8",
            "CollapsingMergeTree(sign) ORDER BY id",
            "(1, 1), (2, 1)",
        ),
        "replacing": (
            f"id {database}.UserId, version UInt64",
            "ReplacingMergeTree(version) ORDER BY id",
            "(1, 1), (2, 1)",
        ),
        "coalescing": (
            f"id {database}.UserId, value Nullable(UInt64)",
            "CoalescingMergeTree ORDER BY id",
            "(1, 10), (2, 20)",
        ),
        "aggregating": (
            f"id {database}.UserId, value UInt64",
            "AggregatingMergeTree ORDER BY id",
            "(1, 10), (2, 20)",
        ),
        "summing": (
            f"id {database}.UserId, value UInt64",
            "SummingMergeTree(value) ORDER BY id",
            "(1, 10), (2, 20)",
        ),
        "graphite": (
            f"id {database}.UserId, metric String, value Float64, "
            "timestamp UInt32, date Date, updated UInt32",
            "GraphiteMergeTree('graphite_rollup') "
            "PARTITION BY toYYYYMM(date) ORDER BY (metric, timestamp, id)",
            "(1, 'boundary.one', 10., 1700000000, '2023-11-14', 1), "
            "(2, 'boundary.two', 20., 1700000000, '2023-11-14', 1)",
        ),
        "versioned_collapsing": (
            f"id {database}.UserId, sign Int8, version UInt64",
            "VersionedCollapsingMergeTree(sign, version) ORDER BY id",
            "(1, 1, 1), (2, 1, 1)",
        ),
    }
    unsupported = {
        "tiny_log": "TinyLog",
        "null_engine": "Null",
        "replicated": (
            "ReplicatedMergeTree('/clickhouse/udt-boundary/{uuid}', 'replica') "
            "ORDER BY id"
        ),
        "shared": (
            "SharedMergeTree('/clickhouse/udt-boundary/{uuid}', 'replica') "
            "ORDER BY id"
        ),
    }

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")

        def assert_engine_state(table, expected_sum, expected_declared_type):
            assert q(f"SELECT sum(id) FROM {database}.{table}").strip() == expected_sum
            assert q(
                "SELECT type, udt_declared_type FROM system.columns "
                f"WHERE database = '{database}' AND table = '{table}' AND name = 'id' "
                "FORMAT TSV"
            ) == f"UInt64\t{expected_declared_type}\n"

        for table, (columns, engine, values) in supported.items():
            q(
                f"CREATE TABLE {database}.{table} ({columns}) "
                f"ENGINE = {engine}"
            )
            q(f"INSERT INTO {database}.{table} VALUES {values}")
            assert_engine_state(table, "3", f"{database}.UserId")

        for table, engine in unsupported.items():
            assert_error_contains(
                f"CREATE TABLE {database}.{table} (id {database}.UserId) "
                f"ENGINE = {engine}",
                "support only memory and non-replicated mergetree-family engines",
            )
            assert q(f"EXISTS TABLE {database}.{table}").strip() == "0"

        assert_error_contains(
            f"CREATE TABLE {database}.distributed_reject "
            "ON CLUSTER 'missing_boundary_cluster' "
            f"(id {database}.UserId) ENGINE = Memory",
            "do not support create on cluster",
        )
        assert q(f"EXISTS TABLE {database}.distributed_reject").strip() == "0"

        node.restart_clickhouse()
        for table in supported:
            expected_sum = "0" if table == "memory" else "3"
            assert_engine_state(table, expected_sum, f"{database}.UserId")

        [plan] = rows_json(
            f"PHYSICALIZE TYPE REFERENCES DATABASE {database} DRY RUN"
        )
        assert plan["scope_count"] == len(supported)
        assert plan["manifest_count"] >= len(supported)
        for table in supported:
            assert f"TABLE `{table}`" in plan["loss_summary"]
        q(
            "PHYSICALIZE TYPE REFERENCES APPLY TOKEN "
            + sql_string(plan["apply_token"])
        )

        for table in supported:
            expected_sum = "0" if table == "memory" else "3"
            assert_engine_state(table, expected_sum, "")
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' "
            "AND (udt_declared_type != '' OR notEmpty(udt_references))"
        ).strip() == "0"
        assert q(
            "SELECT count() FROM system.user_defined_types "
            f"WHERE database = '{database}' AND name = 'UserId'"
        ).strip() == "1"

        node.restart_clickhouse()
        for table in supported:
            expected_sum = "0" if table == "memory" else "3"
            assert_engine_state(table, expected_sum, "")
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' "
            "AND (udt_declared_type != '' OR notEmpty(udt_references))"
        ).strip() == "0"
    finally:
        q(f"DROP DATABASE IF EXISTS {database} SYNC")








def test_restricted_introspection_does_not_leak_udt_identity(started_cluster):
    database = unique_database("access")
    reader = f"udt_boundary_reader_{uuid.uuid4().hex[:8]}"

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.PublicId AS UInt64")
        q(f"CREATE TYPE {database}.SecretId AS UInt64")
        q(
            f"CREATE TABLE {database}.events "
            f"(public_id {database}.PublicId, secret_id {database}.SecretId, note String) "
            "ENGINE = MergeTree ORDER BY public_id"
        )
        q(f"CREATE USER {reader} IDENTIFIED WITH no_password")
        q(f"GRANT SELECT ON system.columns TO {reader}")
        q(f"GRANT SELECT ON system.user_defined_types TO {reader}")
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}'",
            user=reader,
        ).strip() == "0"
        assert q(
            "SELECT count() FROM system.user_defined_types "
            f"WHERE database = '{database}'",
            user=reader,
        ).strip() == "0"

        q(f"GRANT SHOW TABLES ON {database}.events TO {reader}")
        q(f"GRANT SHOW COLUMNS(public_id) ON {database}.events TO {reader}")

        restricted = rows_json(
            "SELECT name, type, udt_declared_type, toString(udt_uuid) AS udt_uuid, "
            "length(udt_references) AS references "
            "FROM system.columns "
            f"WHERE database = '{database}' AND table = 'events' ORDER BY position",
            user=reader,
        )
        assert restricted == [
            {
                "name": "public_id",
                "type": "UInt64",
                "udt_declared_type": "",
                "udt_uuid": NIL_UUID,
                "references": 0,
            }
        ]
        assert q(
            "SELECT count() FROM system.user_defined_types "
            f"WHERE database = '{database}'",
            user=reader,
        ).strip() == "0"

        for statement in (
            f"SHOW CREATE TABLE {database}.events",
            f"DESCRIBE TABLE {database}.events",
        ):
            denied = error(statement, user=reader)
            assert "SHOW COLUMNS" in denied
            assert f"{database}.PublicId" not in denied
            assert f"{database}.SecretId" not in denied

        q(f"GRANT SHOW COLUMNS ON {database}.events TO {reader}")
        q(f"GRANT SHOW TYPES ON {database}.* TO {reader}")
        visible = rows_json(
            "SELECT name, udt_declared_type, toString(udt_uuid) AS udt_uuid "
            "FROM system.columns "
            f"WHERE database = '{database}' AND table = 'events' ORDER BY position",
            user=reader,
        )
        assert [row["name"] for row in visible] == ["public_id", "secret_id", "note"]
        assert visible[0]["udt_declared_type"] == f"{database}.PublicId"
        assert visible[1]["udt_declared_type"] == f"{database}.SecretId"
        assert visible[0]["udt_uuid"] != NIL_UUID
        assert visible[1]["udt_uuid"] != NIL_UUID
        assert visible[2]["udt_declared_type"] == ""
        assert visible[2]["udt_uuid"] == NIL_UUID
        show_create = q(f"SHOW CREATE TABLE {database}.events", user=reader)
        assert f"{database}.PublicId" in show_create
        assert f"{database}.SecretId" in show_create
        described = q(
            f"DESCRIBE TABLE {database}.events FORMAT TSV",
            user=reader,
        )
        assert f"public_id\t{database}.PublicId\t" in described
        assert f"secret_id\t{database}.SecretId\t" in described
        assert q(
            "SELECT count() FROM system.user_defined_types "
            f"WHERE database = '{database}'",
            user=reader,
        ).strip() == "2"
    finally:
        q(f"DROP DATABASE IF EXISTS {database} SYNC")
        q(f"DROP USER IF EXISTS {reader}")


def test_wide_mapped_schema_reads_after_disabled_restart(started_cluster):
    database = unique_database("wide")
    reader = f"udt_boundary_wide_reader_{uuid.uuid4().hex[:8]}"
    column_count = 128
    config_disabled = False

    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        for index in range(4):
            q(f"CREATE TYPE {database}.Wide{index} AS UInt64")

        mapped_columns = ", ".join(
            f"c{index} {database}.Wide{index % 4}" for index in range(column_count)
        )
        physical_columns = ", ".join(
            f"c{index} UInt64" for index in range(column_count)
        )
        values = ", ".join(str(index + 1) for index in range(column_count))
        q(
            f"CREATE TABLE {database}.wide_mapped ({mapped_columns}) "
            "ENGINE = MergeTree ORDER BY c0"
        )
        q(
            f"CREATE TABLE {database}.wide_physical ({physical_columns}) "
            "ENGINE = MergeTree ORDER BY c0"
        )
        q(f"INSERT INTO {database}.wide_mapped VALUES ({values})")
        q(f"INSERT INTO {database}.wide_physical VALUES ({values})")

        assert q(
            "SELECT count(), countIf(udt_declared_type != ''), uniqExact(udt_uuid), "
            "countIf(type = 'UInt64') FROM system.columns "
            f"WHERE database = '{database}' AND table = 'wide_mapped'"
        ).strip() == f"{column_count}\t{column_count}\t4\t{column_count}"
        assert q(
            f"SELECT toTypeName(c0), toTypeName(c63), toTypeName(c127), "
            f"c0 + c63 + c127 FROM {database}.wide_mapped"
        ).strip() == "UInt64\tUInt64\tUInt64\t193"
        assert native_sha256(
            f"SELECT * FROM {database}.wide_mapped ORDER BY c0"
        ) == native_sha256(f"SELECT * FROM {database}.wide_physical ORDER BY c0")

        actions = q(
            f"EXPLAIN actions = 1 SELECT c0 + c63 + c127 "
            f"FROM {database}.wide_mapped"
        )
        for index in range(4):
            assert f"{database}.Wide{index}" not in actions

        q(f"CREATE USER {reader} IDENTIFIED WITH no_password")
        q(f"GRANT SELECT ON {database}.wide_mapped TO {reader}")
        q(f"GRANT SELECT ON system.user_defined_types TO {reader}")
        assert q(
            f"SELECT c0 + c63 + c127 FROM {database}.wide_mapped",
            user=reader,
        ).strip() == "193"
        assert q(
            "SELECT count() FROM system.user_defined_types "
            f"WHERE database = '{database}'",
            user=reader,
        ).strip() == "0"

        node.replace_in_config(CONFIG, SETTING_ON, SETTING_OFF)
        config_disabled = True
        node.restart_clickhouse()

        # Stored data executes from its physical schema without feature admission,
        # SHOW TYPES, or USAGE TYPE. Logical provenance remains attached for rollback.
        assert q(
            f"SELECT c0 + c63 + c127 FROM {database}.wide_mapped",
            user=reader,
            settings={},
        ).strip() == "193"
        assert native_sha256(
            f"SELECT * FROM {database}.wide_mapped ORDER BY c0"
        ) == native_sha256(f"SELECT * FROM {database}.wide_physical ORDER BY c0")
        assert q(
            "SELECT countIf(udt_declared_type != '') FROM system.columns "
            f"WHERE database = '{database}' AND table = 'wide_mapped'",
            settings={},
        ).strip() == str(column_count)
        show_create = q(
            f"SHOW CREATE TABLE {database}.wide_mapped",
            settings={},
        )
        for index in range(4):
            assert f"{database}.Wide{index}" in show_create
        mapped_uuid = q(
            "SELECT toString(uuid) FROM system.tables "
            f"WHERE database = '{database}' AND name = 'wide_mapped'",
            settings={},
        ).strip()
        mapped_data_hash = native_sha256(
            f"SELECT * FROM {database}.wide_mapped ORDER BY c0"
        )
        mapped_provenance = q(
            "SELECT name, udt_declared_type, toString(udt_uuid) "
            "FROM system.columns "
            f"WHERE database = '{database}' AND table = 'wide_mapped' "
            "ORDER BY position FORMAT TSV",
            settings={},
        )
        q(f"DETACH TABLE {database}.wide_mapped", settings={})
        assert q(
            f"EXISTS TABLE {database}.wide_mapped",
            settings={},
        ).strip() == "0"
        q(f"ATTACH TABLE {database}.wide_mapped", settings={})
        assert q(
            "SELECT toString(uuid) FROM system.tables "
            f"WHERE database = '{database}' AND name = 'wide_mapped'",
            settings={},
        ).strip() == mapped_uuid
        assert q(
            "SELECT name, udt_declared_type, toString(udt_uuid) "
            "FROM system.columns "
            f"WHERE database = '{database}' AND table = 'wide_mapped' "
            "ORDER BY position FORMAT TSV",
            settings={},
        ) == mapped_provenance
        assert native_sha256(
            f"SELECT * FROM {database}.wide_mapped ORDER BY c0"
        ) == mapped_data_hash
        assert_error_contains(
            f"ALTER TABLE {database}.wide_mapped "
            f"ADD COLUMN rejected {database}.Wide0",
            "disabled",
            settings={},
        )
        assert q(
            "SELECT count() FROM system.columns "
            f"WHERE database = '{database}' AND table = 'wide_mapped' "
            "AND name = 'rejected'",
            settings={},
        ).strip() == "0"
    finally:
        if config_disabled:
            node.replace_in_config(CONFIG, SETTING_OFF, SETTING_ON)
            node.restart_clickhouse()
        q(f"DROP DATABASE IF EXISTS {database} SYNC")
        q(f"DROP USER IF EXISTS {reader}")
