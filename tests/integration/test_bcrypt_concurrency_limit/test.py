import concurrent.futures

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
