import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        instance.query(
            "CREATE TABLE IF NOT EXISTS default.mrt (id UInt64, v UInt64) "
            "ENGINE = MergeTree ORDER BY id"
        )
        instance.query("INSERT INTO default.mrt SELECT number, number FROM numbers(10)")
        yield cluster
    finally:
        cluster.shutdown()


def test_pinned_tier_rejects_alter_mutation(started_cluster):
    """
    Tier 1 pinned in the profile with `<readonly/>` cannot be lowered in the session,
    so an `ALTER TABLE ... UPDATE` is guaranteed to fail even if the user tries `SET`.
    """
    err = instance.query_and_get_error(
        "ALTER TABLE default.mrt UPDATE v = v + 1 WHERE id = 0",
        user="restrict_alter_user",
    )
    assert "mutations_restrict" in err

    override_err = instance.query_and_get_error(
        "SET mutations_restrict = 0",
        user="restrict_alter_user",
    )
    assert "readonly" in override_err.lower() or "constraint" in override_err.lower()


def test_pinned_tier_1_still_allows_lightweight_and_metadata(started_cluster):
    """
    Tier 1 does not touch lightweight DELETE / UPDATE, metadata-only ALTER, INSERT, or SELECT.
    """
    instance.query("DELETE FROM default.mrt WHERE id = 100", user="restrict_alter_user")
    instance.query(
        "ALTER TABLE default.mrt ADD COLUMN IF NOT EXISTS extra1 UInt8 DEFAULT 0",
        user="restrict_alter_user",
    )
    instance.query("INSERT INTO default.mrt(id, v) VALUES (1000, 1000)", user="restrict_alter_user")
    assert instance.query(
        "SELECT count() FROM default.mrt WHERE id = 1000",
        user="restrict_alter_user",
    ).strip() == "1"


def test_pinned_tier_2_rejects_lightweight_too(started_cluster):
    """
    Tier 2 pinned in the profile rejects lightweight DELETE, lightweight UPDATE, and mutation ALTER.
    """
    for query in [
        "ALTER TABLE default.mrt UPDATE v = v + 1 WHERE id = 0",
        "DELETE FROM default.mrt WHERE id = 0",
        "UPDATE default.mrt SET v = 0 WHERE id = 0 SETTINGS enable_lightweight_update = 1",
    ]:
        err = instance.query_and_get_error(query, user="restrict_all_user")
        assert "mutations_restrict" in err, f"expected mutations_restrict rejection for {query!r}, got: {err}"


def test_soft_default_can_be_overridden(started_cluster):
    """
    When the profile sets `mutations_restrict = 1` without a `<readonly/>` constraint, a user
    can lower it in-session to run a mutation-producing ALTER (the safety-catch pattern).
    """
    err = instance.query_and_get_error(
        "ALTER TABLE default.mrt UPDATE v = v + 1 WHERE id = 0",
        user="default_restrict_user",
    )
    assert "mutations_restrict" in err

    instance.query(
        "ALTER TABLE default.mrt UPDATE v = v + 1 WHERE id = 0 "
        "SETTINGS mutations_restrict = 0, mutations_sync = 2",
        user="default_restrict_user",
    )
