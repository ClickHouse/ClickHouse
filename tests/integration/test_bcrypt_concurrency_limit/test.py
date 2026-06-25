import concurrent.futures
import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Limited node: max_concurrent_bcrypt_authentications = 1.
limited = cluster.add_instance(
    "limited",
    main_configs=["configs/limit.xml"],
    stay_alive=True,
)

# Default node: no setting -> unlimited (historical behavior preserved).
unlimited = cluster.add_instance(
    "unlimited",
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_throttled(node):
    value = node.query(
        "SELECT value FROM system.events WHERE event = 'BcryptAuthenticationThrottled'"
    ).strip()
    return int(value) if value else 0


def make_user(node, name, password):
    node.query(f"DROP USER IF EXISTS {name}")
    node.query(f"CREATE USER {name} IDENTIFIED WITH bcrypt_password BY '{password}'")


def test_repeated_identical_credentials_not_throttled(started_cluster):
    # Scenario (a): a healthy client retrying the SAME correct credentials must never be throttled,
    # because every attempt after the first is a bcrypt cache hit and the cache-hit path never
    # touches the concurrency limiter.
    make_user(limited, "alice", "correct-horse")

    before = get_throttled(limited)

    # Warm the cache (first attempt is the only possible miss), then hammer with identical creds.
    limited.query("SELECT 1", user="alice", password="correct-horse")

    def attempt(_):
        return limited.query("SELECT 1", user="alice", password="correct-horse").strip()

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(attempt, range(200)))

    assert all(r == "1" for r in results)
    # Cache hits must not have produced any throttling, even at concurrency 1.
    assert get_throttled(limited) == before


def test_concurrent_distinct_password_flood_is_capped(started_cluster):
    # Scenario (b): a flood of DISTINCT wrong passwords (cache misses) against a bcrypt user must be
    # capped at the configured concurrency. With the limit at 1, a parallel burst forces rejections,
    # which are observable via the BcryptAuthenticationThrottled profile event. All attempts still
    # fail with the generic authentication error (no information disclosure).
    make_user(limited, "bob", "bob-secret")

    before = get_throttled(limited)

    def flood(i):
        # Distinct password each time -> always a cache miss -> always reaches bcrypt_checkpw.
        return limited.query_and_get_error(
            "SELECT 1", user="bob", password=f"wrong-{i}"
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
        errors = list(pool.map(flood, range(256)))

    # Every attempt fails, and all share the same generic message (throttled and genuinely-wrong
    # are indistinguishable to the client).
    assert all("Authentication failed" in e for e in errors)

    # The flood must have tripped the concurrency cap at least once.
    assert get_throttled(limited) > before


def test_zero_limit_preserves_unlimited_behavior(started_cluster):
    # Scenario (c): with the setting absent (default 0 = unlimited), the same distinct-password flood
    # never throttles -- behavior is identical to before this change.
    make_user(unlimited, "carol", "carol-secret")

    def flood(i):
        return unlimited.query_and_get_error(
            "SELECT 1", user="carol", password=f"wrong-{i}"
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
        errors = list(pool.map(flood, range(256)))

    assert all("Authentication failed" in e for e in errors)
    # No throttling whatsoever in unlimited mode.
    assert get_throttled(unlimited) == 0


def test_throttled_bcrypt_method_falls_through_to_next_method(started_cluster):
    # Scenario (d): a user may list several authentication methods as alternatives. When an earlier
    # bcrypt method is throttled, the limiter denial must behave like a *failed* bcrypt method (so the
    # auth loop tries the next method), not abort the whole login. Otherwise a valid login via a later
    # method (here plaintext) would be wrongly rejected whenever the bcrypt method happens to be
    # throttled. Regression test for clickhouse-gh[bot] review on AuthenticationData.cpp.
    #
    # The bug only manifests on a bcrypt cache MISS that is throttled, so each probe must use a
    # DISTINCT, never-before-seen correct password (a repeated password becomes a bcrypt cache hit,
    # which bypasses the limiter and would mask the bug). We therefore create many multi-method users,
    # each with its own password, and log into each exactly once while a separate user's bcrypt slot
    # is saturated. Pre-fix: nearly all of these correct logins fail. Post-fix: none do.
    n_users = 30
    for i in range(n_users):
        limited.query(f"DROP USER IF EXISTS m_user_{i}")
        # bcrypt method is listed first, so a login attempt reaches bcrypt before plaintext.
        limited.query(
            f"CREATE USER m_user_{i} IDENTIFIED WITH bcrypt_password BY 'bc-{i}', "
            f"plaintext_password BY 'pt-{i}'"
        )

    # A dedicated bcrypt-only user drives the saturating flood (distinct cache-missing passwords),
    # keeping the single global bcrypt slot busy while the probes below run.
    limited.query("DROP USER IF EXISTS flooder")
    limited.query("CREATE USER flooder IDENTIFIED WITH bcrypt_password BY 'flood-secret'")

    stop = threading.Event()

    def saturate(worker_id):
        i = 0
        while not stop.is_set():
            # Distinct password -> cache miss -> contends for the (single) bcrypt slot.
            limited.query_and_get_error(
                "SELECT 1", user="flooder", password=f"flood-{worker_id}-{i}"
            )
            i += 1

    before = get_throttled(limited)
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        floods = [pool.submit(saturate, w) for w in range(16)]
        try:
            # Give the flood a moment to saturate the slot, then probe each fresh user's correct
            # plaintext password exactly once. With the bcrypt method throttled, the login must still
            # succeed via the plaintext method (fall-through). Pre-fix code throws on throttle and
            # aborts the loop, so these correct logins are rejected.
            time.sleep(2)
            failures = []
            for i in range(n_users):
                try:
                    if (
                        limited.query(
                            "SELECT 1", user=f"m_user_{i}", password=f"pt-{i}"
                        ).strip()
                        != "1"
                    ):
                        failures.append(i)
                except Exception as e:
                    failures.append((i, str(e)[:80]))
        finally:
            stop.set()
            for f in floods:
                f.result()

    # Sanity: the background flood actually exercised the throttle path (otherwise the test would
    # pass vacuously without ever throttling the bcrypt method).
    assert get_throttled(limited) > before, "throttle path was never exercised"

    # The core assertion: no correct plaintext login was rejected because bcrypt was throttled.
    assert not failures, f"valid plaintext logins rejected while bcrypt was throttled: {failures}"

    # A genuinely wrong password against a multi-method user must still be rejected (the fall-through
    # must not turn into an accept-anything path).
    err = limited.query_and_get_error("SELECT 1", user="m_user_0", password="totally-wrong")
    assert "Authentication failed" in err
