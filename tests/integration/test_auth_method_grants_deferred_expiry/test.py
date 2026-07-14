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
