import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# `dup_user` is defined in two separate access storages (`storage_a` and `storage_b`).
# A full scan over `findAll<User>` emits a row for every storage, so `system.users`
# must report the user once per storage. The fast path that `system.users` takes for
# `name = '...'` and `name IN (...)` predicates has to keep the same semantics instead
# of collapsing the duplicates.
node = cluster.add_instance(
    "node",
    main_configs=["configs/access_storages.xml"],
    extra_configs=["configs/users_a.xml", "configs/users_b.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def rows(predicate):
    # Sort so the comparison does not depend on row order between the fast path and
    # the full scan.
    return node.query(
        f"SELECT id, name, storage FROM system.users WHERE {predicate} ORDER BY id, name, storage"
    )


def test_fast_path_matches_full_scan_for_duplicate_names():
    # The fallback (full-scan) path: `LIKE` is not extracted as a literal name predicate.
    full_scan = rows("name LIKE 'dup\\_user'")

    # `dup_user` exists in two storages, so we expect two rows.
    assert full_scan.count("dup_user") == 2

    # Fast path with equality must return exactly the same rows as the full scan.
    assert rows("name = 'dup_user'") == full_scan

    # Fast path with `IN` must return exactly the same rows as the full scan.
    assert rows("name IN ('dup_user')") == full_scan


def test_fast_path_matches_full_scan_for_mixed_in():
    # `only_a` is unique to one storage, `dup_user` is in both: three rows total.
    full_scan = rows("name LIKE 'dup\\_user' OR name LIKE 'only\\_a'")

    assert rows("name IN ('dup_user', 'only_a')") == full_scan
    assert full_scan.count("dup_user") == 2
    assert full_scan.count("only_a") == 1
