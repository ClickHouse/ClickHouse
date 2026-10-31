import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/access.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_cache_events():
    return tuple(
        map(
            int,
            node.query(
                "SELECT sumIf(value, event = 'EffectiveAccessRightsCalculations'), "
                "sumIf(value, event = 'EffectiveAccessRightsCacheHits') "
                "FROM system.events"
            ).split(),
        )
    )


@pytest.mark.parametrize("with_roles", [False, True])
def test_reuse_after_permission_changes(started_cluster, with_roles):
    # This server belongs only to this module, uses local access storage, and has
    # no concurrent access changes. Global counters include notifications even if
    # a background thread delivers them; per-query `ProfileEvents` do not.
    suffix = "roles" if with_roles else "user"
    user = f"cache_{suffix}"
    databases = [f"cache_{suffix}_{i}" for i in range(3)]
    role_a, role_b = f"cache_{suffix}_a", f"cache_{suffix}_b"
    target = f"{databases[0]}.target"

    try:
        for database in databases:
            node.query(f"CREATE DATABASE {database}")
        node.query(f"CREATE TABLE {target} (x UInt8) ENGINE = Memory")
        node.query(f"CREATE USER {user} IDENTIFIED WITH no_password")

        if with_roles:
            node.query(f"CREATE ROLE {role_a}, {role_b}")
            node.query(f"GRANT {role_a}, {role_b} TO {user}")
            current_roles = [role_a, role_b, role_a, role_b]
            current_databases = ["default", *databases]
            # Authentication can also retain contexts with the default {A, B}
            # role set, in addition to the explicit {A} and {B} sets.
            max_calculations = 3
        else:
            current_roles = ["NONE", "NONE"]
            current_databases = ["default", databases[0]]
            max_calculations = 1

        sessions = []
        for i, (database, roles) in enumerate(zip(current_databases, current_roles)):
            params = {
                "database": database,
                "session_id": f"{user}_{i}",
                "session_timeout": 600,
            }
            node.http_query(
                f"SET ROLE {roles}", user=user, params=params, method="POST"
            )
            params["session_check"] = 1
            sessions.append(params)
            assert (
                node.http_query(
                    f"CHECK GRANT SELECT ON {target}",
                    user=user,
                    params=params,
                    method="POST",
                )
                == "0\n"
            )

        # The baseline snapshot includes its own access-context initialization.
        # Keep administrator query parameters unchanged during measurement.
        # Bounds allow different numbers of retained contexts, but require reuse.
        for statement, expected in [("GRANT", "1\n"), ("REVOKE", "0\n")]:
            before = get_cache_events()
            preposition = "TO" if statement == "GRANT" else "FROM"
            node.query(f"{statement} SELECT ON {target} {preposition} {user}")
            after = get_cache_events()
            calculations, hits = (end - start for start, end in zip(before, after))
            assert 1 <= calculations <= max_calculations, (before, after)
            assert hits >= 1, (before, after)

            for params in sessions:
                assert (
                    node.http_query(
                        f"CHECK GRANT SELECT ON {target}",
                        user=user,
                        params=params,
                        method="POST",
                    )
                    == expected
                )
    finally:
        node.query(f"DROP USER IF EXISTS {user}")
        if with_roles:
            node.query(f"DROP ROLE IF EXISTS {role_a}, {role_b}")
        for database in databases:
            node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
