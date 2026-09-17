"""Real-server restart coverage for durable definitions-only Atomic UDT authority."""

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["configs/allow_experimental_user_defined_types.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)

DATABASE = "udt_atomic_definition_lifecycle"
DISABLED = {"allow_experimental_user_defined_types": 0}
ENABLED = {"allow_experimental_user_defined_types": 1}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def query(sql, *, settings=ENABLED):
    return node.query(sql, settings=settings)


def definition_snapshot():
    return query(
        f"""
        SELECT
            name,
            toString(uuid),
            revision,
            underlying_type,
            definition_hash,
            create_query,
            comment,
            status
        FROM system.user_defined_types
        WHERE database = '{DATABASE}'
        ORDER BY name
        FORMAT TSV
        """
    )


def test_definition_authority_survives_restart(started_cluster):
    test_failed = False
    try:
        query(f"DROP DATABASE IF EXISTS {DATABASE} SYNC")
        query(f"CREATE DATABASE {DATABASE} ENGINE = Atomic")

        disabled_error = node.query_and_get_error(
            f"CREATE TYPE {DATABASE}.MustNotExist AS UInt8",
            settings=DISABLED,
        )
        assert "Code: 344" in disabled_error, disabled_error
        assert "User-defined type lifecycle is disabled" in disabled_error, disabled_error
        assert query(
            f"SELECT count() FROM system.user_defined_types "
            f"WHERE database = '{DATABASE}' AND name = 'MustNotExist'"
        ) == "0\n"

        query(
            f"CREATE TYPE {DATABASE}.UserId AS UInt64 "
            "COMMENT 'stable external identifier'"
        )
        query(f"CREATE TYPE {DATABASE}.UserIdList AS Array({DATABASE}.UserId)")

        before_restart = definition_snapshot()
        assert "UserId\t" in before_restart
        assert "UserIdList\t" in before_restart
        assert "Array(UInt64)" in before_restart
        assert before_restart.count("\tACTIVE\n") == 2

        user_id_uuid = query(
            f"SELECT toString(uuid) FROM system.user_defined_types "
            f"WHERE database = '{DATABASE}' AND name = 'UserId'"
        ).strip()
        assert user_id_uuid

        node.restart_clickhouse()

        assert definition_snapshot() == before_restart
        assert query(f"SHOW CREATE TYPE {DATABASE}.UserId").strip() == (
            f"CREATE TYPE {DATABASE}.UserId AS UInt64 COMMENT \\'stable external identifier\\'"
        )
        described = dict(
            row.split("\t", 1)
            for row in query(f"DESCRIBE TYPE {DATABASE}.UserId FORMAT TSV").splitlines()
        )
        assert described["uuid"] == user_id_uuid

        query(f"ALTER TYPE {DATABASE}.UserId RENAME TO AccountId")
        renamed_uuid = query(
            f"SELECT toString(uuid) FROM system.user_defined_types "
            f"WHERE database = '{DATABASE}' AND name = 'AccountId'"
        ).strip()
        assert renamed_uuid == user_id_uuid

        restrict_error = node.query_and_get_error(
            f"DROP TYPE {DATABASE}.AccountId RESTRICT",
            settings=ENABLED,
        )
        assert "definition mutation is restricted by a graph dependent" in restrict_error, restrict_error

        query(f"DROP TYPE {DATABASE}.UserIdList RESTRICT")
        query(f"DROP TYPE {DATABASE}.AccountId RESTRICT")
        assert query(
            f"SELECT count() FROM system.user_defined_types WHERE database = '{DATABASE}'"
        ) == "0\n"
    except BaseException:
        test_failed = True
        raise
    finally:
        try:
            if node.get_process_pid("clickhouse server") is None:
                node.start_clickhouse()
            query(f"DROP DATABASE IF EXISTS {DATABASE} SYNC")
        except Exception:
            if not test_failed:
                raise
