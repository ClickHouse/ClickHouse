import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node1")

# A part whose skip indices live in `skp_idx.packed` can also carry a standalone `skp_idx_*` file
# that no `checksums.txt` entry and no archive member accounts for. Building that shape needs a
# write into the part directory, which functional tests must not do, so it lives here.
#
# `packed_skip_index_max_bytes` is a MergeTree default, so both halves coexist under stock
# settings: an archive plus an unaccounted standalone file.


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def part_path(table, part="all_1_1_0"):
    return node.query(
        f"SELECT path FROM system.parts WHERE database = currentDatabase()"
        f" AND table = '{table}' AND name = '{part}'"
    ).strip()


def active_part_path(table):
    return node.query(
        f"SELECT path FROM system.parts WHERE database = currentDatabase()"
        f" AND table = '{table}' AND active ORDER BY name LIMIT 1"
    ).strip()


def sh(command):
    return node.exec_in_container(
        ["bash", "-c", command], privileged=True, user="root"
    ).strip()


def file_exists(path):
    return sh(f'test -e "{path}" && echo 1 || echo 0') == "1"


def make_packed_part(table, index_ddl, extra_columns=""):
    """A wide part with skip indices bundled into skp_idx.packed. Returns its path."""
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (k UInt64, a UInt64, b UInt64{extra_columns}, {index_ddl})
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 packed_skip_index_max_bytes = 1048576, index_granularity = 100,
                 replace_long_file_name_to_hash = 0,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """
    )
    node.query(
        f"INSERT INTO {table} SELECT number, number, number FROM numbers(2000)"
    )
    path = part_path(table)
    # Guard the fixture itself: without an archive both assertions below are vacuous.
    assert file_exists(f"{path}/skp_idx.packed"), "fixture built no packed archive"
    return path


def make_unpacked_part(table, index_ddl):
    """The same shape with packing off, so the part carries no archive at all. Returns its path."""
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (k UInt64, a UInt64, b UInt64, {index_ddl})
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 packed_skip_index_max_bytes = 0, index_granularity = 100,
                 replace_long_file_name_to_hash = 0,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """
    )
    node.query(
        f"INSERT INTO {table} SELECT number, number, number FROM numbers(2000)"
    )
    path = part_path(table)
    # Guard the fixture: with an archive present this would exercise the packed path instead.
    assert not file_exists(f"{path}/skp_idx.packed"), "fixture built a packed archive"
    return path


def inject_orphan(path, name, size=64, source="skp_idx.packed"):
    """A standalone skip-index file with no checksums entry and no archive member."""
    sh(f'head -c {size} "{path}/{source}" > "{path}/{name}"')
    # Owned by the server user, so a leak fails the assertion below on its own merits rather
    # than on a permission error from the injection.
    sh(f'chown --reference="{path}/{source}" "{path}/{name}"')
    assert file_exists(f"{path}/{name}")


def test_orphan_is_not_a_materialized_index(start_cluster):
    """A query over a part carrying an orphan must still answer, and must not count it."""
    path = make_packed_part(
        "t_read",
        "INDEX mm_a a TYPE minmax GRANULARITY 1",
    )
    # mm_b is declared but never materialized, so nothing accounts for skp_idx_mm_b.idx2.
    node.query("ALTER TABLE t_read ADD INDEX mm_b b TYPE minmax GRANULARITY 1")
    inject_orphan(path, "skp_idx_mm_b.idx2")
    node.query("DETACH TABLE t_read")
    node.query("ATTACH TABLE t_read")

    assert node.query("SELECT count() FROM t_read WHERE b = 1500") == "1\n"

    # mm_a is a real archive member and must keep pruning; mm_b is an orphan and reads as absent.
    assert (
        node.query(
            "SELECT name, data_compressed_bytes > 0 FROM system.data_skipping_indices"
            " WHERE database = currentDatabase() AND table = 't_read' AND name = 'mm_a'"
        )
        == "mm_a\t1\n"
    )
    assert (
        node.query(
            "SELECT count() > 0 FROM (EXPLAIN indexes = 1"
            " SELECT count() FROM t_read WHERE a = 1500)"
            " WHERE explain ILIKE '%Granules: 1/20%'"
        )
        == "1\n"
    )


def test_orphan_is_not_hardlinked_by_mutation(start_cluster):
    """A mutation must strip the orphan rather than carry it into the new part."""
    path = make_packed_part(
        "t_mutate",
        "INDEX mm_b b TYPE minmax GRANULARITY 1",
    )
    # minmax moved .idx (v1) -> .idx2 (v2); a stale v1 file may sit beside the packed v2 one.
    inject_orphan(path, "skp_idx_mm_b.idx")

    # Mutating the indexed column puts mm_b in indices_to_recalc, which is what consults the
    # substream list that decides whether the stale file is hardlinked forward.
    node.query(
        "ALTER TABLE t_mutate UPDATE b = b + 0 WHERE 1 SETTINGS mutations_sync = 2"
    )

    new_path = active_part_path("t_mutate")
    assert new_path != path
    assert not file_exists(f"{new_path}/skp_idx_mm_b.idx")
    assert node.query("SELECT count() FROM t_mutate WHERE b = 1500") == "1\n"
    assert (
        node.query("CHECK TABLE t_mutate SETTINGS check_query_single_value_result = 1")
        == "1\n"
    )


def test_orphan_on_unpacked_part_is_not_counted(start_cluster):
    """Without an archive the part is outside this probe's scope, so the orphan stays invisible."""
    path = make_unpacked_part(
        "t_unpacked",
        "INDEX mm_a a TYPE minmax GRANULARITY 1",
    )
    # mm_b is declared but never materialized, so nothing accounts for skp_idx_mm_b.idx2.
    node.query("ALTER TABLE t_unpacked ADD INDEX mm_b b TYPE minmax GRANULARITY 1")
    inject_orphan(path, "skp_idx_mm_b.idx2", source="skp_idx_mm_a.idx2")
    node.query("DETACH TABLE t_unpacked")
    node.query("ATTACH TABLE t_unpacked")

    # mm_a is checksummed and sized; mm_b exists only as an unaccounted file and reads as absent,
    # so its bytes never enter the totals that only a checksummed index can later subtract.
    assert (
        node.query(
            "SELECT name, data_compressed_bytes > 0 FROM system.data_skipping_indices"
            " WHERE database = currentDatabase() AND table = 't_unpacked' ORDER BY name"
        )
        == "mm_a\t1\nmm_b\t0\n"
    )
