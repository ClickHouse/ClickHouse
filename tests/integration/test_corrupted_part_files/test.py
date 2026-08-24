#!/usr/bin/env python3
"""Tests for files inside part directories being damaged or removed, and how
loading / fetching / CHECK TABLE reacts.

Converted from stateless tests (which must not modify the server's data on disk):
  - 02253_empty_part_checksums.sh
  - 02255_broken_parts_chain_on_start.sh
  - 02444_async_broken_outdated_part_loading.sh
  - 04151_unique_key_sst_rebuild_on_load.sh
  - 04235_corrupted_columns_substreams_detection.sh
  - 04323_text_index_marks_empty_part.sh
  - 04506_packed_part_fetch_checksum.sh
  - 02346_text_index_corrupted_positions.sh
"""

import shlex

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", with_zookeeper=True, stay_alive=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def bash(node, command):
    return node.exec_in_container(["bash", "-c", command], privileged=True, user="root")


def get_part_path(node, table, part_name):
    path = node.query(f"SELECT path FROM system.parts WHERE database = 'default' AND table = '{table}' AND name = '{part_name}'").strip()
    # ensure that path is absolute before removing anything under it
    assert path.startswith("/"), f"Path is relative: {path}"
    return path


def get_active_part_path(node, table):
    path = node.query(f"SELECT path FROM system.parts WHERE database = 'default' AND table = '{table}' AND active").strip()
    assert path.startswith("/"), f"Path is relative: {path}"
    return path


def file_exists(node, path):
    return bash(node, f"test -f {shlex.quote(path)} && echo yes || echo no").strip() == "yes"


def file_nonempty(node, path):
    return bash(node, f"test -s {shlex.quote(path)} && echo yes || echo no").strip() == "yes"


def file_size(node, path):
    return int(bash(node, f"stat -c%s {shlex.quote(path)}").strip())


def test_empty_part_checksums(started_cluster):
    # Converted from stateless test 02253_empty_part_checksums.sh.
    # add_minmax_index_for_numeric_columns=0: Adds extra files, which changes the hashes
    # min_bytes_for_full_part_storage=0: the test deletes the part dir on local fs; packed storage would
    # pack it into a single data.packed archive, so the part check then fails to open it
    node1.query("DROP TABLE IF EXISTS rmt_empty_part SYNC")
    node1.query(
        """
        CREATE TABLE rmt_empty_part (a UInt8, b Int16, c Float32, d String, e Array(UInt8), f Nullable(UUID), g Tuple(UInt8, UInt16))
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_corrupted_part_files/empty_part_checksums', '1') ORDER BY a PARTITION BY b % 10
        SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 0, cleanup_delay_period_random_add = 0, compress_marks = 1, compress_primary_key = 1, serialization_info_version = 'basic',
        cleanup_thread_preferred_points_per_iteration = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, remove_empty_parts = 0, replace_long_file_name_to_hash = 0,
        add_minmax_index_for_numeric_columns = 0
        """
    )

    node1.query(
        "INSERT INTO rmt_empty_part SELECT rand(1), 0, 1 / rand(3), toString(rand(4)), [rand(5), rand(6)], rand(7) % 2 ? NULL : generateUUIDv4(), (rand(8), rand(9)) FROM numbers(1000)",
        settings={"insert_keeper_fault_injection_probability": 0},
    )

    assert node1.query("CHECK TABLE rmt_empty_part SETTINGS check_query_single_value_result = 1") == "1\n"
    assert node1.query("SELECT count() FROM rmt_empty_part") == "1000\n"

    path = get_part_path(node1, "rmt_empty_part", "0_0_0_0")
    bash(node1, f"rm -rf {path}")

    # detach the broken part, replace it with empty one
    assert node1.query("CHECK TABLE rmt_empty_part SETTINGS check_query_single_value_result = 1") == "0\n"
    assert node1.query("SELECT count() FROM rmt_empty_part") == "0\n"

    node1.query("SYSTEM SYNC REPLICA rmt_empty_part", settings={"receive_timeout": 60})

    # the empty part should pass the check
    assert node1.query("CHECK TABLE rmt_empty_part SETTINGS check_query_single_value_result = 1") == "1\n"
    assert node1.query("SELECT count() FROM rmt_empty_part") == "0\n"

    assert (
        node1.query("SELECT name, part_type, hash_of_all_files, hash_of_uncompressed_files, uncompressed_hash_of_compressed_files FROM system.parts WHERE database = 'default' AND table = 'rmt_empty_part'")
        == "0_0_0_0\tWide\t85adbaf60cad8c08f040d4cb27830cf4\te73297470a3016870e8f281b48b2dd68\tb324ada5cd6bb14402c1e59200bd003a\n"
    )

    node1.query("DROP TABLE rmt_empty_part SYNC")


def test_broken_parts_chain_on_start(started_cluster):
    # Converted from stateless test 02255_broken_parts_chain_on_start.sh.
    node1.query("DROP TABLE IF EXISTS rmt1 SYNC")
    node1.query("DROP TABLE IF EXISTS rmt2 SYNC")

    node1.query(
        "CREATE TABLE rmt1 (a int, b int) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_corrupted_part_files/broken_parts_chain', 'r1') ORDER BY a SETTINGS old_parts_lifetime = 100500"
    )
    node1.query(
        "CREATE TABLE rmt2 (a int, b int) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_corrupted_part_files/broken_parts_chain', 'r2') ORDER BY a SETTINGS old_parts_lifetime = 100500"
    )

    node1.query("INSERT INTO rmt1 VALUES (1, 1), (1, 2), (1, 3)", settings={"insert_keeper_fault_injection_probability": 0})
    node1.query("ALTER TABLE rmt1 UPDATE b = b * 10 WHERE 1 SETTINGS mutations_sync = 1")
    node1.query("SYSTEM SYNC REPLICA rmt2")
    assert node1.query("SELECT 1, *, _part FROM rmt2 ORDER BY b") == "1\t1\t10\tall_0_0_0_1\n1\t1\t20\tall_0_0_0_1\n1\t1\t30\tall_0_0_0_1\n"

    # Break both parts of the mutation chain: the mutated (active) part and its source (outdated) part.
    for part_name in ["all_0_0_0", "all_0_0_0_1"]:
        path = get_part_path(node1, "rmt1", part_name)
        bash(node1, f"rm -f {path}data.bin")

    # The original emulated a server restart with DETACH TABLE ... SYNC + ATTACH TABLE
    # (the test is about broken parts chain "on start"), so a real restart is the faithful equivalent.
    node1.restart_clickhouse()

    # Retry because right after restart the replica may still be initializing (readonly).
    node1.query_with_retry("SYSTEM SYNC REPLICA rmt1")
    assert node1.query("SELECT 1, *, _part FROM rmt1 ORDER BY b") == "1\t1\t10\tall_0_0_0_1\n1\t1\t20\tall_0_0_0_1\n1\t1\t30\tall_0_0_0_1\n"

    node1.query("TRUNCATE TABLE rmt1")

    # The original filtered system.replicas by its unique test database only; the table filter is the
    # equivalent isolation in the shared 'default' database.
    assert node1.query("SELECT table, lost_part_count FROM system.replicas WHERE database = 'default' AND table IN ('rmt1', 'rmt2') AND lost_part_count != 0") == ""

    node1.query("DROP TABLE rmt1 SYNC")
    node1.query("DROP TABLE rmt2 SYNC")


def test_async_broken_outdated_part_loading(started_cluster):
    # Converted from stateless test 02444_async_broken_outdated_part_loading.sh.
    node1.query("DROP TABLE IF EXISTS rmt_outdated SYNC")
    node1.query("CREATE TABLE rmt_outdated (n int) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_corrupted_part_files/async_broken_outdated', '1') ORDER BY n SETTINGS old_parts_lifetime = 600")

    node1.query("INSERT INTO rmt_outdated VALUES (1)", settings={"insert_keeper_fault_injection_probability": 0})
    node1.query("INSERT INTO rmt_outdated VALUES (2)", settings={"insert_keeper_fault_injection_probability": 0})

    node1.query("SYSTEM SYNC REPLICA rmt_outdated PULL")
    node1.query("OPTIMIZE TABLE rmt_outdated FINAL", settings={"optimize_throw_if_noop": 1})
    node1.query("SYSTEM SYNC REPLICA rmt_outdated")
    assert node1.query("SELECT 1, *, _part FROM rmt_outdated ORDER BY n") == "1\t1\tall_0_1_1\n1\t2\tall_0_1_1\n"

    # Break the outdated part all_1_1_0 (kept on disk by old_parts_lifetime), which is covered
    # by the active merged part all_0_1_1.
    # Note: the original had `rm -f "$path/*.bin"` where the quotes prevented glob expansion,
    # so the removal was accidentally a no-op; here the glob expands and the outdated part is
    # actually broken, as the test intends.
    path = get_part_path(node1, "rmt_outdated", "all_1_1_0")
    bash(node1, f"rm -f {path}*.bin")

    # DETACH TABLE ... SYNC is kept as-is (not a restart): its interplay with the asynchronous
    # loading of outdated parts is the mechanism under test.
    node1.query("DETACH TABLE rmt_outdated SYNC")
    node1.query("ATTACH TABLE rmt_outdated")
    assert node1.query("SELECT 2, *, _part FROM rmt_outdated ORDER BY n") == "2\t1\tall_0_1_1\n2\t2\tall_0_1_1\n"

    node1.query("TRUNCATE TABLE rmt_outdated")

    node1.query("DETACH TABLE rmt_outdated SYNC")
    node1.query("ATTACH TABLE rmt_outdated")

    # The original filtered system.replicas by its unique test database only; the table filter is the
    # equivalent isolation in the shared 'default' database.
    assert node1.query("SELECT table, lost_part_count FROM system.replicas WHERE database = 'default' AND table = 'rmt_outdated' AND lost_part_count != 0") == ""

    node1.query("DROP TABLE rmt_outdated SYNC")


def test_unique_key_sst_rebuild_on_load(started_cluster):
    # Converted from stateless test 04151_unique_key_sst_rebuild_on_load.sh.
    #
    # UNIQUE KEY: load-time dense-index lifecycle.
    #
    # 1. A part that reaches disk without its `unique_key_index.sst` (e.g. a freeze
    #    taken before UK shipped, or a sidecar lost on restore) is repaired on load:
    #    DETACH/ATTACH re-runs loadDataParts, which rebuilds the SST. The part stays
    #    active and its data is fully readable.
    # 2. Fail-closed contract: a non-empty UK part whose dense index cannot be
    #    rebuilt (missing UK column / unreadable rows / no RocksDB) is detached as
    #    broken instead of activated. The rebuild-failure path is covered by the
    #    USE_ROCKSDB=0 gtest (writeDenseIndexOnInsert / ensureValidDenseIndex throw)
    #    and the CORRUPTED_DATA gtests; reproducing it via stateless filesystem
    #    corruption trips the earlier checksum-consistency check first.
    #    TODO(unique-key): add a fault-injection stateless variant that loads the
    #    columns cleanly but fails the UK rebuild, asserting system.detached_parts.
    # 3. A present-but-corrupt SST is NOT trusted on presence. The sidecar carries no
    #    checksums.txt entry, so a truncated/corrupt/stale file survives startup and
    #    would only fail at probe time. Load-time validation (raw SstFileReader Open +
    #    VerifyChecksum + num_entries==rows_count) detects the damage, removes the
    #    file, and rebuilds it. Three corruption cases below:
    #      a. zero-byte truncation      -> Open corruption; discriminates presence-only.
    #      b. partial (half) truncation -> Open corruption (footer at file end lost).
    #      c. valid SST with wrong count -> Open + VerifyChecksum PASS; only the
    #         num_entries != rows_count check catches it (discriminates that check).
    #    A transient (I/O) validation failure is classified separately and must NOT
    #    delete/rebuild the file — it raises UNIQUE_KEY_DENSE_INDEX_UNREADABLE so the
    #    load fails for retry. Inducing a real transient (FD/OOM) failure in a
    #    stateless test is not practical, so that path is covered by code structure +
    #    reasoning, not asserted here.
    # 4. Readonly startup (`table_readonly = 1`) still validates but cannot
    #    remove/rebuild/detach: a corrupt SST fails the ATTACH (fail closed) with
    #    the file left untouched; a valid SST loads fine readonly.
    #
    # DETACH/ATTACH is kept (not a server restart): the original's own comments frame
    # ATTACH as the mechanism that re-runs loadDataParts.
    node1.query("DROP TABLE IF EXISTS uk_rebuild_load SYNC")

    node1.query(
        """
        CREATE TABLE uk_rebuild_load (id UInt64, v String)
        ENGINE = MergeTree
        UNIQUE KEY (id)
        ORDER BY (id)
        SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1
        """,
        settings={"allow_experimental_unique_key": 1},
    )

    node1.query("INSERT INTO uk_rebuild_load VALUES (10, 'a'), (20, 'b'), (30, 'c')")

    data_path = get_active_part_path(node1, "uk_rebuild_load")
    assert file_exists(node1, data_path + "unique_key_index.sst")  # sst_present_before_detach

    # Detach so the part files are quiescent, drop the SST sidecar, then reattach.
    node1.query("DETACH TABLE uk_rebuild_load")
    bash(node1, f"rm -f {data_path}unique_key_index.sst")
    node1.query("ATTACH TABLE uk_rebuild_load")

    # Part survived load and the SST was rebuilt; data is intact.
    assert node1.query("SELECT count() FROM system.parts WHERE database = 'default' AND table = 'uk_rebuild_load' AND active") == "1\n"  # active_parts_after_attach

    new_path = get_active_part_path(node1, "uk_rebuild_load")
    assert file_exists(node1, new_path + "unique_key_index.sst")  # sst_present_after_attach

    assert node1.query("SELECT id, v FROM uk_rebuild_load ORDER BY id") == "10\ta\n20\tb\n30\tc\n"  # rows_after_attach

    # --- Corrupt-SST recovery: truncate the (valid, rebuilt) SST to zero bytes to
    # simulate a corrupt/truncated sidecar, then reattach. Presence alone must not be
    # trusted: load-time validation detects the damage, removes the file, and rebuilds
    # it. Discriminator: a zero-byte file would survive the old presence-only fast path
    # (present but empty); the fix leaves a present, non-empty, valid SST.
    node1.query("DETACH TABLE uk_rebuild_load")
    bash(node1, f": > {new_path}unique_key_index.sst")
    assert not file_nonempty(node1, new_path + "unique_key_index.sst")  # sst_nonempty_before_corrupt_attach
    # The rebuild-from-corrupt path intentionally logs a WARNING ("corrupt/unreadable
    # ... removing and rebuilding"); the original silenced server logs for this one
    # ATTACH so the stateless harness's stderr check did not flag the expected message.
    node1.query("ATTACH TABLE uk_rebuild_load", settings={"send_logs_level": "error"})

    assert node1.query("SELECT count() FROM system.parts WHERE database = 'default' AND table = 'uk_rebuild_load' AND active") == "1\n"  # active_parts_after_corrupt_attach

    final_path = get_active_part_path(node1, "uk_rebuild_load")
    assert file_exists(node1, final_path + "unique_key_index.sst")  # sst_present_after_corrupt_attach
    assert file_nonempty(node1, final_path + "unique_key_index.sst")  # sst_nonempty_after_corrupt_attach

    assert node1.query("SELECT id, v FROM uk_rebuild_load ORDER BY id") == "10\ta\n20\tb\n30\tc\n"  # rows_after_corrupt_attach

    # --- Partial truncation: keep only the first half of the (rebuilt) SST. The
    # footer / metaindex live at the file end, so a tail truncation trips a RocksDB
    # corruption status at Open — detected and rebuilt. (This is caught before the
    # num_entries check; the valid-but-wrong-count case below covers that path.)
    part_path = get_active_part_path(node1, "uk_rebuild_load")
    full_size = file_size(node1, part_path + "unique_key_index.sst")
    node1.query("DETACH TABLE uk_rebuild_load")
    bash(node1, f"head -c {full_size // 2} {part_path}unique_key_index.sst > {part_path}unique_key_index.sst.trunc && mv {part_path}unique_key_index.sst.trunc {part_path}unique_key_index.sst")
    node1.query("ATTACH TABLE uk_rebuild_load", settings={"send_logs_level": "error"})
    part_path = get_active_part_path(node1, "uk_rebuild_load")
    assert file_size(node1, part_path + "unique_key_index.sst") == full_size  # sst_full_size_after_partial_attach
    assert node1.query("SELECT id, v FROM uk_rebuild_load ORDER BY id") == "10\ta\n20\tb\n30\tc\n"  # rows_after_partial_attach

    # --- Valid-but-wrong-count SST: this is what discriminates the num_entries check.
    # Build a genuinely valid 1-entry SST from a scratch table and swap it onto the
    # 3-row part. It opens and every block checksum verifies, so Open + VerifyChecksum
    # both ACCEPT it; only `num_entries (1) != rows_count (3)` flags it as corrupt.
    # ATTACH must rebuild it — assert the on-disk SST no longer equals the swapped-in
    # 1-entry file. (Without the num_entries check this stays the trusted 1-entry file.)
    node1.query("DROP TABLE IF EXISTS uk_scratch_one SYNC")
    node1.query(
        """
        CREATE TABLE uk_scratch_one (id UInt64, v String)
        ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id)
        SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1
        """,
        settings={"allow_experimental_unique_key": 1},
    )
    node1.query("INSERT INTO uk_scratch_one VALUES (99, 'z')")
    one_part = get_active_part_path(node1, "uk_scratch_one")
    one_sst = "/tmp/test_corrupted_part_files_one_entry.sst"
    bash(node1, f"cp {one_part}unique_key_index.sst {one_sst}")
    node1.query("DROP TABLE uk_scratch_one SYNC")

    part_path = get_active_part_path(node1, "uk_rebuild_load")
    node1.query("DETACH TABLE uk_rebuild_load")
    bash(node1, f"cp {one_sst} {part_path}unique_key_index.sst")
    node1.query("ATTACH TABLE uk_rebuild_load", settings={"send_logs_level": "error"})
    part_path = get_active_part_path(node1, "uk_rebuild_load")
    assert bash(node1, f"cmp -s {one_sst} {part_path}unique_key_index.sst && echo no || echo yes").strip() == "yes"  # wrongcount_sst_rebuilt
    assert node1.query("SELECT id, v FROM uk_rebuild_load ORDER BY id") == "10\ta\n20\tb\n30\tc\n"  # rows_after_wrongcount_attach
    bash(node1, f"rm -f {one_sst}")

    node1.query("DROP TABLE uk_rebuild_load SYNC")

    # --- Readonly startup: with `table_readonly = 1` the load still VALIDATES the
    # SST (read-only I/O) but cannot remove/rebuild/detach (all writes). A corrupt
    # SST must fail the ATTACH (fail closed, error names the readonly cause) and the
    # file must be left untouched; restoring the valid SST lets the readonly ATTACH
    # succeed. Previously the readonly gate skipped validation entirely (fail-open).
    node1.query("DROP TABLE IF EXISTS uk_ro SYNC")
    node1.query(
        """
        CREATE TABLE uk_ro (id UInt64, v String)
        ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id)
        SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1
        """,
        settings={"allow_experimental_unique_key": 1},
    )
    node1.query("INSERT INTO uk_ro VALUES (1, 'x'), (2, 'y')")
    node1.query("ALTER TABLE uk_ro MODIFY SETTING table_readonly = 1")
    ro_path = get_active_part_path(node1, "uk_ro")
    node1.query("DETACH TABLE uk_ro")
    bash(node1, f"cp {ro_path}unique_key_index.sst {ro_path}unique_key_index.sst.keep")
    bash(node1, f": > {ro_path}unique_key_index.sst")
    assert "UNIQUE_KEY_DENSE_INDEX_UNREADABLE" in node1.query_and_get_error("ATTACH TABLE uk_ro")  # readonly_attach_with_corrupt_sst_fails
    assert file_exists(node1, ro_path + "unique_key_index.sst")  # corrupt_sst_left_in_place
    bash(node1, f"mv {ro_path}unique_key_index.sst.keep {ro_path}unique_key_index.sst")
    node1.query("ATTACH TABLE uk_ro")
    assert node1.query("SELECT count() FROM uk_ro") == "2\n"  # readonly_attach_after_restore
    node1.query("ALTER TABLE uk_ro MODIFY SETTING table_readonly = 0")
    node1.query("DROP TABLE uk_ro SYNC")


def test_corrupted_columns_substreams_detection(started_cluster):
    # Converted from stateless test 04235_corrupted_columns_substreams_detection.sh.
    #
    # Test that corrupted columns_substreams.txt (from a historical rename bug) is detected
    # and safely discarded at load time, allowing the part to work correctly without it.
    # Tests both simple types (Array) and types with dynamic substreams (JSON).

    # ---- Test 1: Array(UInt32) column ----

    node1.query("DROP TABLE IF EXISTS t_corrupted_substreams SYNC")

    node1.query(
        """
        CREATE TABLE t_corrupted_substreams
        (
            id UInt64,
            arr Array(UInt32)
        )
        ENGINE = MergeTree ORDER BY id
        SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
                 enable_block_number_column = 0, enable_block_offset_column = 0,
                 replace_long_file_name_to_hash = 0, ratio_of_defaults_for_sparse_serialization = 1
        """
    )

    node1.query("INSERT INTO t_corrupted_substreams SELECT number, [number, number + 1] FROM numbers(10)")

    # Data before corruption.
    assert node1.query("SELECT count(), sum(id), sum(length(arr)) FROM t_corrupted_substreams") == "10\t45\t20\n"

    # Get the data path of the active part.
    data_path = get_active_part_path(node1, "t_corrupted_substreams")

    # Detach the table so we can modify files on disk.
    node1.query("DETACH TABLE t_corrupted_substreams")

    # Corrupt columns_substreams.txt by writing substream names that simulate the rename bug:
    # substream names like "arrwrong" instead of "arr" or "arr.size0".
    corrupted_content = "columns substreams version: 1\n2 columns:\n1 substreams for column `id`:\n\tid\n1 substreams for column `arr`:\n\tarrwrongprefix\n"
    bash(node1, f"printf '%s' {shlex.quote(corrupted_content)} > {data_path}columns_substreams.txt")

    # Attach the table - this triggers loadColumnsSubstreams which should detect the corruption,
    # log a warning, and discard the corrupted data.
    node1.query("ATTACH TABLE t_corrupted_substreams")

    # Data after attach with corrupted file.
    assert node1.query("SELECT count(), sum(id), sum(length(arr)) FROM t_corrupted_substreams") == "10\t45\t20\n"

    # CHECK TABLE should also work (falls back to enumerateStreams since columns_substreams was discarded).
    assert node1.query("CHECK TABLE t_corrupted_substreams SETTINGS check_query_single_value_result = 1") == "1\n"

    # DETACH/ATTACH partition should also work.
    node1.query("ALTER TABLE t_corrupted_substreams DETACH PARTITION tuple()")
    node1.query("ALTER TABLE t_corrupted_substreams ATTACH PARTITION tuple()")

    # Data after partition reattach.
    assert node1.query("SELECT count(), sum(id), sum(length(arr)) FROM t_corrupted_substreams") == "10\t45\t20\n"

    node1.query("DROP TABLE t_corrupted_substreams SYNC")

    # ---- Test 2: JSON column (dynamic substreams, exercises enumerate_dynamic_streams = false fallback) ----

    node1.query("DROP TABLE IF EXISTS t_corrupted_substreams_json SYNC")

    node1.query(
        """
        CREATE TABLE t_corrupted_substreams_json
        (
            id UInt64,
            data JSON
        )
        ENGINE = MergeTree ORDER BY id
        SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
                 enable_block_number_column = 0, enable_block_offset_column = 0,
                 replace_long_file_name_to_hash = 0, ratio_of_defaults_for_sparse_serialization = 1
        """,
        settings={"allow_experimental_json_type": 1},
    )

    node1.query("""INSERT INTO t_corrupted_substreams_json VALUES (1, '{"a": 1, "b": "hello"}'), (2, '{"a": 2, "c": [1, 2, 3]}')""")

    # JSON data before corruption.
    assert node1.query("SELECT id, data.a FROM t_corrupted_substreams_json ORDER BY id") == "1\t1\n2\t2\n"

    # Get the data path of the active part.
    data_path_json = get_active_part_path(node1, "t_corrupted_substreams_json")

    # Detach the table so we can modify files on disk.
    node1.query("DETACH TABLE t_corrupted_substreams_json")

    # Corrupt columns_substreams.txt by writing a wrong prefix for the data column substreams.
    corrupted_content_json = "columns substreams version: 1\n2 columns:\n1 substreams for column `id`:\n\tid\n1 substreams for column `data`:\n\tdatawrongprefix.object_structure\n"
    bash(node1, f"printf '%s' {shlex.quote(corrupted_content_json)} > {data_path_json}columns_substreams.txt")

    # Attach the table - corruption detected, file discarded, falls back to enumerate_dynamic_streams = false.
    node1.query("ATTACH TABLE t_corrupted_substreams_json")

    # JSON data after attach with corrupted file.
    assert node1.query("SELECT id, data.a FROM t_corrupted_substreams_json ORDER BY id") == "1\t1\n2\t2\n"

    assert node1.query("CHECK TABLE t_corrupted_substreams_json SETTINGS check_query_single_value_result = 1") == "1\n"

    # DETACH/ATTACH partition should also work.
    node1.query("ALTER TABLE t_corrupted_substreams_json DETACH PARTITION tuple()")
    node1.query("ALTER TABLE t_corrupted_substreams_json ATTACH PARTITION tuple()")

    # JSON data after partition reattach.
    assert node1.query("SELECT id, data.a FROM t_corrupted_substreams_json ORDER BY id") == "1\t1\n2\t2\n"

    node1.query("DROP TABLE t_corrupted_substreams_json SYNC")


def test_text_index_marks_empty_part(started_cluster):
    # Converted from stateless test 04323_text_index_marks_empty_part.sh.
    node1.query("DROP TABLE IF EXISTS t_text_idx_empty SYNC")

    node1.query(
        """
        CREATE TABLE t_text_idx_empty
        (
            s FixedString(37),
            INDEX idx s TYPE text(tokenizer = array()) GRANULARITY 100000000
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        -- min_bytes_for_full_part_storage=0: the test edits/removes raw part files (skp_idx_idx.mrk4,
        -- checksums.txt); a packed part keeps them inside the single data.packed archive, not on disk.
        -- remove_empty_parts=0: the whole test operates on the empty part left by the DELETE mutation;
        -- otherwise the cleanup thread may drop it before the checks below see it in system.parts.
        SETTINGS prewarm_mark_cache = true, compress_marks = 0, min_bytes_for_full_part_storage = 0,
                 remove_empty_parts = 0
        """
    )

    node1.query("INSERT INTO t_text_idx_empty SELECT toFixedString(toString(number), 37) FROM numbers(5)")
    node1.query("INSERT INTO t_text_idx_empty SELECT toFixedString(toString(number + 5), 37) FROM numbers(5)")
    node1.query("OPTIMIZE TABLE t_text_idx_empty FINAL")
    node1.query("ALTER TABLE t_text_idx_empty DELETE WHERE 1 SETTINGS mutations_sync = 2")

    data_path = node1.query("SELECT data_paths[1] FROM system.tables WHERE database = 'default' AND table = 't_text_idx_empty'").strip()
    part_name = node1.query("SELECT name FROM system.parts WHERE database = 'default' AND table = 't_text_idx_empty' AND active").strip()
    part_dir = data_path + part_name

    assert node1.query("SELECT rows, marks FROM system.parts WHERE database = 'default' AND table = 't_text_idx_empty' AND active") == "0\t0\n"

    node1.query("DETACH TABLE t_text_idx_empty")

    bash(node1, f"printf '\\x00' >> {part_dir}/skp_idx_idx.mrk4")
    bash(node1, f"rm {part_dir}/checksums.txt")

    node1.query("ATTACH TABLE t_text_idx_empty", settings={"send_logs_level": "fatal"})

    node1.query("SYSTEM PREWARM MARK CACHE t_text_idx_empty")
    assert node1.query("SELECT count() FROM t_text_idx_empty") == "0\n"
    assert node1.query("SELECT count() FROM t_text_idx_empty WHERE has(['anything'], s)") == "0\n"

    node1.query("DROP TABLE t_text_idx_empty SYNC")


def test_text_index_corrupted_positions(started_cluster):
    # Converted from stateless test 02346_text_index_corrupted_positions.sh.
    # A damaged positions stream (.pos) must raise an error, not answer hasPhrase from the garbage it
    # decodes. .pos is uncompressed, so the bytes edited here reach the decoder, not a checksum.
    node1.query("DROP TABLE IF EXISTS t_pos SYNC")

    node1.query(
        """
        CREATE TABLE t_pos
        (
            k UInt64,
            s String,
            INDEX txt(s) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1) GRANULARITY 1
        )
        ENGINE = MergeTree ORDER BY k
        -- min_bytes_for_full_part_storage=0: the test edits the raw skp_idx_txt.pos.idx file, which a
        -- packed part keeps inside data.packed instead of on disk.
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 100,
                 replace_long_file_name_to_hash = 0, min_bytes_for_full_part_storage = 0,
                 allow_experimental_text_index_phrase_search = 1
        """,
        settings={"enable_full_text_index": 1},
    )

    # Selective (100 of 2000 rows) so the reader takes the positional path, not the selectivity fallback.
    node1.query(
        "INSERT INTO t_pos SELECT number, if(number < 100, 'needle alpha beta',"
        " concat('hello', number % 50, ' world', number % 50)) FROM numbers(2000)"
    )

    pos = get_active_part_path(node1, "t_pos") + "skp_idx_txt.pos.idx"
    assert file_nonempty(node1, pos)

    # Kept outside the part directory: the server removes files it does not recognise from a part.
    backup = "/tmp/t_pos_positions.orig"
    bash(node1, f"cp {pos} {backup}")
    size = file_size(node1, pos)

    index_settings = {
        "use_skip_indexes": 1,
        "use_skip_indexes_on_data_read": 1,
        "query_plan_direct_read_from_text_index": 1,
        "use_query_condition_cache": 0,
    }
    query = "SELECT count() FROM t_pos WHERE hasPhrase(s, 'needle alpha')"

    def drop_caches():
        # The edits below keep the file size, so only cached content can hide them.
        node1.query(
            "SYSTEM DROP TEXT INDEX CACHES; SYSTEM DROP MARK CACHE; SYSTEM DROP UNCOMPRESSED CACHE;"
            " SYSTEM DROP MMAP CACHE; SYSTEM DROP PAGE CACHE"
        )

    def phrase_count():
        drop_caches()
        return node1.query(query, settings=index_settings).strip()

    def phrase_error():
        drop_caches()
        return node1.query_and_get_error(query, settings=index_settings)

    # Control: the intact index agrees with a plain scan, else the cases below would prove nothing.
    expected = node1.query(query, settings={"use_skip_indexes": 0}).strip()
    assert phrase_count() == expected

    # Every case keeps the file size. Shrinking it would leave the part's cached size stale, so the
    # query would fail on the seek rather than on the bytes under test.

    # Zeroed directory: the stored document count no longer matches the dictionary's.
    bash(node1, f"head -c {size} /dev/zero > {pos}")
    assert "CORRUPTED_DATA" in phrase_error()

    # Oversized declared sizes: high bits set in the directory's leading bytes inflate every count.
    bash(node1, f"cp {backup} {pos} && printf '\\xff\\xff\\xff\\xff' | dd of={pos} bs=1 seek=0 conv=notrunc status=none")
    assert "CORRUPTED_DATA" in phrase_error()

    # A block size past this token's 6-byte blob but inside the file: only a bound taken from the
    # token's own length rejects it. Byte 2 is the first token's block size, asserted so the fixture
    # fails loudly if it ever drifts.
    bash(node1, f"cp {backup} {pos}")
    assert bash(node1, f"od -An -tu1 -N 3 {pos}").split() == ["100", "1", "3"]
    bash(node1, f"printf '\\x64' | dd of={pos} bs=1 seek=2 conv=notrunc status=none")
    assert "CORRUPTED_DATA" in phrase_error()

    # Restored: the query works again, so the failures came from the bytes, not a broken table.
    bash(node1, f"cp {backup} {pos}")
    assert phrase_count() == expected

    bash(node1, f"rm -f {backup}")
    node1.query("DROP TABLE t_pos SYNC")


def test_packed_part_fetch_checksum(started_cluster):
    # Converted from stateless test 04506_packed_part_fetch_checksum.sh.
    # The source table (with the packed part, corrupted on the local filesystem) lives on node1
    # and the destination table fetches from it over the interserver protocol from node2.
    node1.query("DROP TABLE IF EXISTS packed_fetch_src SYNC")
    node2.query("DROP TABLE IF EXISTS packed_fetch_dst SYNC")

    # Source holds a packed part; destination fetches it with ALTER ... FETCH PART.
    # min_bytes_for_full_part_storage forces packed storage (the whole part in a single data.packed).
    node1.query(
        """
        CREATE TABLE packed_fetch_src (a UInt64, s String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_corrupted_part_files/packed_fetch', 'src') ORDER BY a
        SETTINGS min_bytes_for_full_part_storage = '1G', min_bytes_for_wide_part = 0, old_parts_lifetime = 100000
        """
    )
    node2.query(
        """
        CREATE TABLE packed_fetch_dst (a UInt64, s String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_corrupted_part_files/packed_fetch_dst', 'dst') ORDER BY a
        SETTINGS min_bytes_for_full_part_storage = '1G', min_bytes_for_wide_part = 0, old_parts_lifetime = 100000
        """
    )
    node1.query("INSERT INTO packed_fetch_src VALUES (1, 'hello'), (2, 'world')", settings={"insert_keeper_fault_injection_probability": 0})

    data_path = get_active_part_path(node1, "packed_fetch_src")

    # Sanity check: the part must actually be packed for this test to be meaningful.
    assert node1.query("SELECT part_storage_type FROM system.parts WHERE database = 'default' AND table = 'packed_fetch_src' AND active") == "Packed\n"

    # Corrupt a byte inside a column data file (.bin) of the single data.packed archive. checksums.txt
    # stays intact, so the part is still loadable but its contents no longer match the checksums it
    # advertises. Locate the file by the .bin extension, not by an exact stem: the on-disk stem is the
    # column name only when it is short enough; with replace_long_file_name_to_hash and a small
    # max_file_name_length the stem is replaced by its hash, but the .bin extension is kept.
    # Pick the largest .bin so the 4 bytes land squarely inside a checksummed region.
    listing = bash(node1, f"clickhouse packed-io -i {data_path}data.packed --list 2>/dev/null | awk '$1 ~ /\\.bin$/ {{ print $3, $4 }}' | sort -k2 -n | tail -1")
    bin_offset, bin_size = (int(x) for x in listing.split())
    bash(node1, f"printf '\\xAA\\xBB\\xCC\\xDD' | dd of={data_path}data.packed bs=1 seek={bin_offset + bin_size // 2} count=4 conv=notrunc 2>/dev/null")

    # Fetching the corrupted packed part must be rejected by checksum verification, exactly as it is for
    # full part storage. If verification is skipped, the corrupted part is silently accepted into
    # detached/ and would be propagated across replicas.
    node2.query_and_get_error("ALTER TABLE packed_fetch_dst FETCH PART 'all_0_0_0' FROM '/clickhouse/tables/test_corrupted_part_files/packed_fetch'")

    assert node2.query("SELECT count() FROM system.detached_parts WHERE database = 'default' AND table = 'packed_fetch_dst'") == "0\n"

    node1.query("DROP TABLE packed_fetch_src SYNC")
    node2.query("DROP TABLE packed_fetch_dst SYNC")
