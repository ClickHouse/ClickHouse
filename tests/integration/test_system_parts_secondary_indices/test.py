#!/usr/bin/env python3
"""Tests that `system.parts.secondary_indices_materialized` reports only index
data the part actually owns and the reader can actually use.

Converted from stateless tests (which must not modify the server's data on disk):
  - 04870_system_parts_secondary_indices_orphan_index_file.sh
  - 05045_system_parts_secondary_indices_missing_marks.sh
"""

import base64
import shlex

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def bash(node, command):
    return node.exec_in_container(["bash", "-c", command], privileged=True, user="root")


def get_active_part_path(node, table):
    path = node.query(
        f"SELECT path FROM system.parts WHERE database = 'default' AND table = '{table}' AND active AND rows > 0 LIMIT 1"
    ).strip()
    # ensure that path is absolute before touching anything under it
    assert path.startswith("/"), f"Path is relative: {path}"
    return path


def materialized_indices(node, table):
    return node.query(f"""
        SELECT secondary_indices_materialized
        FROM system.parts
        WHERE database = 'default' AND table = '{table}' AND active AND rows > 0
        ORDER BY name
        """).strip()


def test_orphan_index_file(started_cluster):
    # Converted from stateless test 04870_system_parts_secondary_indices_orphan_index_file.sh.
    #
    # The column must count only index data the part actually owns. A part in the
    # released-bug shape of #109595 (see 04427_mutate_some_columns_drop_index_corrupted_idx)
    # carries `skp_idx_<name>.*` files in its directory while `checksums.txt` has no entry
    # for them: the index was dropped and re-added, so it is not materialized in that part
    # until `ALTER TABLE ... MATERIALIZE INDEX` rebuilds it. Reporting it as materialized
    # because the loose file happens to exist would make the column lie about exactly
    # those legacy parts.
    node.query("DROP TABLE IF EXISTS t_orphan SYNC")
    node.query("""
        CREATE TABLE t_orphan
        (
            k UInt64,
            v UInt64,
            INDEX mm_v v TYPE minmax GRANULARITY 1
        )
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 index_granularity = 100, replace_long_file_name_to_hash = 0,
                 packed_skip_index_max_bytes = 0,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """)
    node.query("INSERT INTO t_orphan (k, v) SELECT number, number FROM numbers(2000)")

    assert materialized_indices(node, "t_orphan") == "['mm_v']"

    # Save the freshly written index files, then drop and re-add the index so the
    # active part has no `skp_idx_mm_v` entries in `checksums.txt` any more.
    active = get_active_part_path(node, "t_orphan")
    bash(node, f"cp {shlex.quote(active + 'skp_idx_mm_v.idx2')} /tmp/saved.idx2")
    bash(node, f"cp {shlex.quote(active + 'skp_idx_mm_v.cmrk2')} /tmp/saved.cmrk2")

    node.query("ALTER TABLE t_orphan DROP INDEX mm_v SETTINGS mutations_sync = 2")
    node.query("ALTER TABLE t_orphan ADD INDEX mm_v v TYPE minmax GRANULARITY 1")

    assert materialized_indices(node, "t_orphan") == "[]"

    # Re-inject the saved files as orphans: present on disk, absent from checksums.
    corrupt = get_active_part_path(node, "t_orphan")
    bash(node, f"cp /tmp/saved.idx2 {shlex.quote(corrupt + 'skp_idx_mm_v.idx2')}")
    bash(node, f"cp /tmp/saved.cmrk2 {shlex.quote(corrupt + 'skp_idx_mm_v.cmrk2')}")

    # The orphan file is on disk, but the index is still not materialized here.
    assert (
        bash(
            node,
            f"ls {shlex.quote(corrupt)}skp_idx_mm_v.* > /dev/null 2>&1 && echo yes || echo no",
        ).strip()
        == "yes"
    )
    assert materialized_indices(node, "t_orphan") == "[]"

    # The first `MATERIALIZE INDEX` only drops the orphan files (04427): the index is
    # not on disk any more, so it is not selected for recalculation. The second one
    # rebuilds it, and only then is it materialized -- which also shows the check is
    # not vacuously false.
    node.query(
        "ALTER TABLE t_orphan MATERIALIZE INDEX mm_v SETTINGS mutations_sync = 2"
    )
    assert materialized_indices(node, "t_orphan") == "[]"
    node.query(
        "ALTER TABLE t_orphan MATERIALIZE INDEX mm_v SETTINGS mutations_sync = 2"
    )
    assert materialized_indices(node, "t_orphan") == "['mm_v']"

    node.query("DROP TABLE t_orphan SYNC")


def test_packed_index(started_cluster):
    # Converted from stateless test 04870_system_parts_secondary_indices_orphan_index_file.sh.
    #
    # A packed index has no per-file entry in `checksums.txt` of its own -- its data is
    # a member of the part's `skp_idx.packed`, which is what is checksummed. It is
    # materialized all the same, so the check must not read the missing entry as absence.
    node.query("DROP TABLE IF EXISTS t_packed SYNC")
    node.query("""
        CREATE TABLE t_packed
        (
            k UInt64,
            v UInt64,
            INDEX mm_v v TYPE minmax GRANULARITY 1
        )
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 index_granularity = 100, replace_long_file_name_to_hash = 0,
                 packed_skip_index_max_bytes = 1048576,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """)
    node.query("INSERT INTO t_packed (k, v) SELECT number, number FROM numbers(2000)")

    packed_part = get_active_part_path(node, "t_packed")
    assert (
        bash(
            node,
            f"test -f {shlex.quote(packed_part + 'skp_idx.packed')} && echo yes || echo no",
        ).strip()
        == "yes"
    )
    assert materialized_indices(node, "t_packed") == "['mm_v']"

    node.query("DROP TABLE t_packed SYNC")


def test_packed_orphan_file(started_cluster):
    # A packed part must own its index data through the archive, not through whatever
    # `skp_idx_*` file happens to lie next to `skp_idx.packed`. The archive fallback of
    # the ownership check has to ask for archive membership specifically: the generic
    # `existsFile` of the part storage falls back to a loose file on disk when the archive
    # does not hold the name, which would make a stray copy of another part's index files
    # -- listed neither in `checksums.txt` nor in the archive -- count as materialized and
    # route queries into those orphan bytes.
    node.query("DROP TABLE IF EXISTS t_packed_orphan SYNC")
    node.query("DROP TABLE IF EXISTS t_loose_donor SYNC")
    node.query("""
        CREATE TABLE t_packed_orphan
        (
            k UInt64,
            v UInt64,
            w UInt64,
            INDEX mm_v v TYPE minmax GRANULARITY 1
        )
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 index_granularity = 100, replace_long_file_name_to_hash = 0,
                 packed_skip_index_max_bytes = 1048576,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """)
    node.query(
        "INSERT INTO t_packed_orphan (k, v, w) SELECT number, number, number FROM numbers(2000)"
    )
    node.query("SYSTEM STOP MERGES t_packed_orphan")

    # A second index, on a column outside the primary key so that only this index could prune
    # granules for a filter on it, declared but not materialized in the existing part.
    node.query("ALTER TABLE t_packed_orphan ADD INDEX mm_w w TYPE minmax GRANULARITY 1")
    assert materialized_indices(node, "t_packed_orphan") == "['mm_v']"

    # Real, decodable per-file index files for `mm_w` come from a donor table with the
    # same data and layout, written without packing.
    node.query("""
        CREATE TABLE t_loose_donor
        (
            k UInt64,
            v UInt64,
            w UInt64,
            INDEX mm_w w TYPE minmax GRANULARITY 1
        )
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 index_granularity = 100, replace_long_file_name_to_hash = 0,
                 packed_skip_index_max_bytes = 0,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """)
    node.query(
        "INSERT INTO t_loose_donor (k, v, w) SELECT number, number, number FROM numbers(2000)"
    )
    donor_part = get_active_part_path(node, "t_loose_donor")
    packed_part = get_active_part_path(node, "t_packed_orphan")
    assert (
        bash(
            node,
            f"test -f {shlex.quote(packed_part + 'skp_idx.packed')} && echo yes || echo no",
        ).strip()
        == "yes"
    )

    # Drop the loose orphans next to the archive: on disk, but in neither `checksums.txt`
    # nor `skp_idx.packed`.
    bash(node, f"cp {shlex.quote(donor_part)}skp_idx_mm_w.* {shlex.quote(packed_part)}")
    assert (
        bash(
            node,
            f"ls {shlex.quote(packed_part)}skp_idx_mm_w.* > /dev/null 2>&1 && echo yes || echo no",
        ).strip()
        == "yes"
    )

    # Still only the archived index is materialized; the orphans are not owned by the part.
    assert materialized_indices(node, "t_packed_orphan") == "['mm_v']"

    # The read path shares the gate: a query on the not-yet-materialized index answers from the
    # data without applying it, so all 20 granules of the 2000-row part stay. The orphan files
    # hold a valid minmax index of the very same data, so had they been taken for the part's own,
    # the plan would show a single granule instead. `MATERIALIZE INDEX` afterwards is what makes
    # the index materialized -- so the check is not vacuously false.
    plan = node.query(
        "EXPLAIN indexes = 1 SELECT count() FROM t_packed_orphan WHERE w = 1500"
    )
    assert "Name: mm_w" in plan, plan
    assert "Granules: 20/20" in plan, plan
    assert (
        node.query("SELECT count() FROM t_packed_orphan WHERE w = 1500").strip() == "1"
    )

    # As in test_orphan_index_file, the first `MATERIALIZE INDEX` only removes the orphan files
    # (04427), the second one rebuilds the index -- and only then is it materialized.
    node.query("SYSTEM START MERGES t_packed_orphan")
    node.query(
        "ALTER TABLE t_packed_orphan MATERIALIZE INDEX mm_w SETTINGS mutations_sync = 2"
    )
    assert (
        bash(
            node,
            f"ls {shlex.quote(get_active_part_path(node, 't_packed_orphan'))}skp_idx_mm_w.* > /dev/null 2>&1 && echo yes || echo no",
        ).strip()
        == "no"
    )
    assert materialized_indices(node, "t_packed_orphan") == "['mm_v']"
    node.query(
        "ALTER TABLE t_packed_orphan MATERIALIZE INDEX mm_w SETTINGS mutations_sync = 2"
    )
    assert materialized_indices(node, "t_packed_orphan") == "['mm_v','mm_w']"

    node.query("DROP TABLE t_loose_donor SYNC")
    node.query("DROP TABLE t_packed_orphan SYNC")


def rewrite_checksums_without_file(node, part_path, file_to_drop):
    """Re-emit the part's `checksums.txt` without the entry for `file_to_drop`.

    Decompresses the version-4 body, parses the version-3 binary it holds, and
    re-emits the checksums as plain-text format version 2 (which the server still
    reads), keeping the true sizes and hashes of every other file.
    """
    v3 = base64.b64decode(
        bash(
            node,
            f"tail -c +29 {shlex.quote(part_path + 'checksums.txt')} "
            f"| clickhouse compressor --decompress | base64 -w0",
        )
    )

    pos = 0

    def read_varuint():
        nonlocal pos
        result, shift = 0, 0
        while True:
            b = v3[pos]
            pos += 1
            result |= (b & 0x7F) << shift
            if not b & 0x80:
                return result
            shift += 7

    def read_u64():
        nonlocal pos
        value = int.from_bytes(v3[pos : pos + 8], "little")
        pos += 8
        return value

    entries = []
    for _ in range(read_varuint()):
        name_len = read_varuint()
        name = v3[pos : pos + name_len].decode()
        pos += name_len
        file_size = read_varuint()
        hash_low, hash_high = read_u64(), read_u64()
        is_compressed = v3[pos]
        pos += 1
        uncompressed = None
        if is_compressed:
            uncompressed = (read_varuint(), read_u64(), read_u64())
        entries.append(
            (name, file_size, hash_low, hash_high, is_compressed, uncompressed)
        )

    entries = [e for e in entries if e[0] != file_to_drop]
    v2 = "checksums format version: 2\n"
    v2 += f"{len(entries)} files:\n"
    for name, file_size, hash_low, hash_high, is_compressed, uncompressed in entries:
        v2 += f"{name}\n\tsize: {file_size}\n\thash: {hash_low} {hash_high}\n\tcompressed: {1 if is_compressed else 0}\n"
        if is_compressed:
            v2 += f"\tuncompressed size: {uncompressed[0]}\n\tuncompressed hash: {uncompressed[1]} {uncompressed[2]}\n"

    encoded = base64.b64encode(v2.encode()).decode()
    bash(
        node,
        f"echo {encoded} | base64 -d > {shlex.quote(part_path + 'checksums.txt')}",
    )


def test_missing_marks_file(started_cluster):
    # Converted from stateless test 05045_system_parts_secondary_indices_missing_marks.sh.
    #
    # The column must require the marks file of every index substream, not just the
    # data file: `MergeTreeIndexReader` loads a marks file for each stream it opens,
    # so a part whose `checksums.txt` lists `skp_idx_<name>.idx2` but not the matching
    # marks file has an unusable index and reporting it as materialized would be a
    # false positive.
    #
    # The shape is fabricated by rewriting the part's `checksums.txt` without the
    # marks entry and reloading the part with `DETACH TABLE` / `ATTACH TABLE`.
    node.query("DROP TABLE IF EXISTS t_missing_marks SYNC")
    node.query("""
        CREATE TABLE t_missing_marks
        (
            k UInt64,
            v UInt64,
            INDEX mm_v v TYPE minmax GRANULARITY 1
        )
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 index_granularity = 100, replace_long_file_name_to_hash = 0,
                 packed_skip_index_max_bytes = 0,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """)
    node.query(
        "INSERT INTO t_missing_marks (k, v) SELECT number, number FROM numbers(2000)"
    )

    assert materialized_indices(node, "t_missing_marks") == "['mm_v']"

    # While the part is intact the index is applied and drops all but one granule; this is the
    # control for the same query after the marks file is removed below.
    healthy_plan = node.query(
        "EXPLAIN indexes = 1 SELECT count() FROM t_missing_marks WHERE v = 1500"
    )
    assert "Granules: 1/20" in healthy_plan, healthy_plan

    part_path = get_active_part_path(node, "t_missing_marks")
    marks_file = bash(
        node, f"basename $(ls {shlex.quote(part_path)}skp_idx_mm_v.*mrk*)"
    ).strip()

    # No background rewrite of the part while its files are edited underneath it.
    node.query("SYSTEM STOP MERGES t_missing_marks")
    node.query("DETACH TABLE t_missing_marks")

    rewrite_checksums_without_file(node, part_path, marks_file)
    bash(node, f"rm {shlex.quote(part_path + marks_file)}")

    node.query("ATTACH TABLE t_missing_marks")

    # The index data file is still owned (listed in checksums.txt and on disk), but
    # its marks file is neither: the reader cannot use this index, so it must not be
    # reported as materialized.
    assert (
        bash(
            node,
            f"ls {shlex.quote(part_path)}skp_idx_mm_v.idx* > /dev/null 2>&1 && echo yes || echo no",
        ).strip()
        == "yes"
    )
    assert materialized_indices(node, "t_missing_marks") == "[]"

    # The column and the read path share one gate (`IMergeTreeIndex::getDeserializedFormat`), so a
    # query filtering on the indexed column must agree with what the column reports: the index is
    # skipped and the query answers from the data, instead of routing into the unusable index and
    # failing on the missing marks file.
    assert (
        node.query("SELECT count() FROM t_missing_marks WHERE v = 1500").strip() == "1"
    )
    plan = node.query(
        "EXPLAIN indexes = 1 SELECT count() FROM t_missing_marks WHERE v = 1500"
    )
    # 2000 rows with `index_granularity = 100`: the index is not applied to this part, so
    # none of its 20 granules are dropped. Without the shared gate the query would instead
    # route into the index and fail on the missing marks file.
    assert "Name: mm_v" in plan, plan
    assert "Granules: 20/20" in plan, plan
    assert "Granules: 1/20" not in plan, plan

    node.query("DROP TABLE t_missing_marks SYNC")
