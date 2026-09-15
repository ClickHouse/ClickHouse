from time import sleep

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# A per-authentication-method `GRANTS` limit is captured at login and re-checked before every query on
# the synchronous path (`Session::checkIfUserIsStillValid`). Deferred execution paths (asynchronous
# insert flush, `QueryRunner` invoker jobs) rebuild the context from the captured identity and have no
# session, so they must carry the method's `VALID UNTIL` and fail closed if it has passed. Otherwise a
# token could enqueue work just before expiry and have the server flush it afterwards.
#
# This test drives the asynchronous insert flush: an insert is queued (fire-and-forget) while the token
# is still valid, with a flush deferred (a large busy timeout) past the token's expiry. The queued data
# must be dropped, while an otherwise identical insert made with a non-expiring token must land.
node = cluster.add_instance("node", stay_alive=True)

# The busy timeout defers the flush well past the expired token's `VALID UNTIL`, so the flush happens
# under an already-expired credential. Kept comfortably larger than the token lifetime below.
BUSY_TIMEOUT_MS = 12000
# Lifetime of the expiring token, measured from user creation. The insert is pushed within a fraction of
# a second of creation (so the push itself is always under a valid credential), but the deferred flush at
# push + BUSY_TIMEOUT_MS lands well after this.
EXPIRING_LIFETIME_S = 5


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def async_insert_settings():
    return {
        "async_insert": 1,
        # Fire-and-forget: the push returns immediately (proving the credential is valid at push time),
        # and the flush happens later, in the background, under the replayed identity.
        "wait_for_async_insert": 0,
        "async_insert_busy_timeout_min_ms": BUSY_TIMEOUT_MS,
        "async_insert_busy_timeout_max_ms": BUSY_TIMEOUT_MS,
    }


def test_expired_token_does_not_flush_deferred_insert(start_cluster):
    node.query("DROP USER IF EXISTS u_deferred_expired")
    node.query("DROP USER IF EXISTS u_deferred_valid")
    node.query("DROP TABLE IF EXISTS default.t_deferred_expired")
    node.query("DROP TABLE IF EXISTS default.t_deferred_valid")

    node.query("CREATE TABLE default.t_deferred_expired (x UInt64) ENGINE = MergeTree ORDER BY x")
    node.query("CREATE TABLE default.t_deferred_valid (x UInt64) ENGINE = MergeTree ORDER BY x")

    # A token whose authentication method expires shortly after creation, limited to its target table.
    expiry = node.query(f"SELECT toString(now() + INTERVAL {EXPIRING_LIFETIME_S} SECOND)").strip()
    node.query(f"CREATE USER u_deferred_expired IDENTIFIED WITH sha256_password BY 'pw' VALID UNTIL '{expiry}' GRANTS (INSERT ON default.t_deferred_expired)")
    node.query("GRANT INSERT ON default.t_deferred_expired TO u_deferred_expired")

    # A control token that never expires (far-future `VALID UNTIL`), limited to its own target table.
    node.query("CREATE USER u_deferred_valid IDENTIFIED WITH sha256_password BY 'pw' VALID UNTIL '2999-01-01 00:00:00' GRANTS (INSERT ON default.t_deferred_valid)")
    node.query("GRANT INSERT ON default.t_deferred_valid TO u_deferred_valid")

    # Both pushes happen while the credentials are valid, so neither is rejected at push time. The rows
    # sit in the async insert queue until the deferred flush at push + BUSY_TIMEOUT_MS.
    node.query(
        "INSERT INTO default.t_deferred_expired VALUES (1)",
        user="u_deferred_expired",
        password="pw",
        settings=async_insert_settings(),
    )
    node.query(
        "INSERT INTO default.t_deferred_valid VALUES (1)",
        user="u_deferred_valid",
        password="pw",
        settings=async_insert_settings(),
    )

    # Wait until the deferred flush has certainly happened (push + busy timeout), which is after the
    # expiring token's `VALID UNTIL` but before the control token's.
    sleep(BUSY_TIMEOUT_MS / 1000 + 6)

    # The expired token's queued insert must have been dropped when the flush ran under the expired
    # credential (fail closed). Without carrying/checking `VALID UNTIL` in the deferred path, the flush
    # would have executed and the row would be present.
    assert node.query("SELECT count() FROM default.t_deferred_expired").strip() == "0"
    # The control token never expired, so its deferred flush executed normally.
    assert node.query("SELECT count() FROM default.t_deferred_valid").strip() == "1"

    node.query("DROP USER u_deferred_expired")
    node.query("DROP USER u_deferred_valid")
    node.query("DROP TABLE default.t_deferred_expired")
    node.query("DROP TABLE default.t_deferred_valid")


def test_expired_token_does_not_run_deferred_query_runner_job(start_cluster):
    node.query("DROP USER IF EXISTS u_deferred_query_runner")
    node.query("DROP TABLE IF EXISTS default.runner_deferred_expiry")
    node.query("DROP TABLE IF EXISTS default.t_deferred_query_runner")

    node.query("CREATE TABLE default.t_deferred_query_runner (x UInt64) ENGINE = MergeTree ORDER BY x")
    node.query(
        "CREATE TABLE default.runner_deferred_expiry (query String, settings Map(String, String)) "
        "ENGINE = QueryRunner SETTINGS mode = 'asynchronous', threads = 1 SQL SECURITY INVOKER"
    )

    # Occupy the runner's only worker while the limited credential expires. Its queued job must then
    # be rejected by `StorageQueryRunner::makeJobContext` before the query is executed.
    node.query(
        "INSERT INTO default.runner_deferred_expiry VALUES "
        "('SELECT sleepEachRow(1) FROM numbers(8) SETTINGS max_block_size = 1', "
        "{'log_comment': 'deferred_query_runner_blocker'})"
    )

    expiry = node.query(f"SELECT toString(now() + INTERVAL {EXPIRING_LIFETIME_S} SECOND)").strip()
    node.query(
        "CREATE USER u_deferred_query_runner IDENTIFIED WITH sha256_password BY 'pw' "
        f"VALID UNTIL '{expiry}' "
        "GRANTS (INSERT ON default.runner_deferred_expiry, INSERT ON default.t_deferred_query_runner)"
    )
    node.query("GRANT INSERT ON default.runner_deferred_expiry TO u_deferred_query_runner")
    node.query("GRANT INSERT ON default.t_deferred_query_runner TO u_deferred_query_runner")

    node.query(
        "INSERT INTO default.runner_deferred_expiry VALUES "
        "('INSERT INTO default.t_deferred_query_runner VALUES (1)', "
        "{'log_comment': 'deferred_query_runner_expired'})",
        user="u_deferred_query_runner",
        password="pw",
    )

    node.query("SYSTEM WAIT QUERY RUNNER default.runner_deferred_expiry")
    assert node.query("SELECT count() FROM default.t_deferred_query_runner").strip() == "0"

    node.query("DROP USER u_deferred_query_runner")
    node.query("DROP TABLE default.runner_deferred_expiry")
    node.query("DROP TABLE default.t_deferred_query_runner")


def test_credential_grants_survive_deferred_query_runner_job(start_cluster):
    """The deferred `QueryRunner` job must run under the *credential's* grant limit, not the user's full rights.

    The user is granted `INSERT` on both target tables, but the authentication method used to submit the
    jobs lists only one of them in its `GRANTS` clause. If `StorageQueryRunner::makeJobContext` stopped
    replaying `authentication_grants`, the deferred job would widen back to the full user and the insert
    into the unlisted table would land.
    """
    node.query("DROP USER IF EXISTS u_query_runner_grants")
    node.query("DROP TABLE IF EXISTS default.runner_grants")
    node.query("DROP TABLE IF EXISTS default.t_query_runner_listed")
    node.query("DROP TABLE IF EXISTS default.t_query_runner_unlisted")

    node.query("CREATE TABLE default.t_query_runner_listed (x UInt64) ENGINE = MergeTree ORDER BY x")
    node.query("CREATE TABLE default.t_query_runner_unlisted (x UInt64) ENGINE = MergeTree ORDER BY x")
    node.query(
        "CREATE TABLE default.runner_grants (query String, settings Map(String, String)) "
        "ENGINE = QueryRunner SETTINGS mode = 'asynchronous', threads = 1 SQL SECURITY INVOKER"
    )

    # The credential may write to the runner and to the *listed* table only; the unlisted table is
    # deliberately absent from the `GRANTS` clause.
    node.query(
        "CREATE USER u_query_runner_grants IDENTIFIED WITH sha256_password BY 'pw' "
        "GRANTS (INSERT ON default.runner_grants, INSERT ON default.t_query_runner_listed)"
    )
    # The user itself is granted `INSERT` on *both* targets, so only the credential limit can deny the
    # second job - which makes the assertion below non-vacuous.
    node.query("GRANT INSERT ON default.runner_grants TO u_query_runner_grants")
    node.query("GRANT INSERT ON default.t_query_runner_listed TO u_query_runner_grants")
    node.query("GRANT INSERT ON default.t_query_runner_unlisted TO u_query_runner_grants")

    # Both jobs are accepted at push time: pushing only requires `INSERT` on the runner table, which the
    # credential is allowed to do. The inner queries run later, under the replayed identity.
    node.query(
        "INSERT INTO default.runner_grants VALUES "
        "('INSERT INTO default.t_query_runner_listed VALUES (1)', {'log_comment': 'query_runner_grants_listed'}), "
        "('INSERT INTO default.t_query_runner_unlisted VALUES (1)', {'log_comment': 'query_runner_grants_unlisted'})",
        user="u_query_runner_grants",
        password="pw",
    )

    node.query("SYSTEM WAIT QUERY RUNNER default.runner_grants")

    # The listed table is inside the credential's grant limit, so its deferred job executed.
    assert node.query("SELECT count() FROM default.t_query_runner_listed").strip() == "1"
    # The unlisted one is outside it, so its deferred job was denied even though the user could do it.
    assert node.query("SELECT count() FROM default.t_query_runner_unlisted").strip() == "0"

    node.query("DROP USER u_query_runner_grants")
    node.query("DROP TABLE default.runner_grants")
    node.query("DROP TABLE default.t_query_runner_listed")
    node.query("DROP TABLE default.t_query_runner_unlisted")
