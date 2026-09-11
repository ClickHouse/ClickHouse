#!/usr/bin/env python3
"""Tests for files inside part directories being damaged or removed, and how
loading / fetching / CHECK TABLE reacts.

Converted from stateless tests (which must not modify the server's data on disk):
  - 02253_empty_part_checksums.sh
  - 02255_broken_parts_chain_on_start.sh
  - 02444_async_broken_outdated_part_loading.sh
  - 04235_corrupted_columns_substreams_detection.sh
  - 04323_text_index_marks_empty_part.sh
  - 04506_packed_part_fetch_checksum.sh
  - 02346_text_index_corrupted_positions.sh
  - 04545_empty_columns_txt_not_fatal.sh
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


# The tests below are converted from stateless test 04545_empty_columns_txt_not_fatal.sh.
#
# writeColumns rewrites columns.txt in place (no atomic rename, no fsync), so an interrupted rewrite
# plus a power loss can leave a zero-byte columns.txt in a committed part directory. An empty
# columns.txt used to throw on load (NamesAndTypesList::readText begins with assertString) and
# detach the whole part as broken, losing every row of an otherwise-intact part. It must instead be
# treated like an absent columns.txt: for a wide part the column list (including any persistent
# virtual columns the part carries) is rebuilt from metadata.
#
# Each case manipulates one part's columns.txt by an absolute path captured once, so every table
# must hold exactly one active part with no covered sibling: merges are stopped and the insert block
# size is pinned so that a single insert produces a single part.

# A single part per insert, whatever the server's block-size defaults are.
ONE_PART_PER_INSERT = {
    "max_insert_threads": 1,
    "min_insert_block_size_rows": 100000,
    "min_insert_block_size_bytes": 0,
    "max_block_size": 100000,
}

# The recovery under test rebuilds a wide part's column list from the stream files present in the
# part directory, so the part must be wide and unpacked. Persistent virtual columns are off unless a
# case asks for them.
WIDE_PART_SETTINGS = """min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
                 min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
                 enable_block_number_column = 0, enable_block_offset_column = 0"""


def create_wide_part_table(table, columns, order_by, extra_settings=""):
    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"""
        CREATE TABLE {table} ({columns})
        ENGINE = MergeTree ORDER BY {order_by}
        SETTINGS {WIDE_PART_SETTINGS}{extra_settings}
        """
    )


def truncate_file(node, path):
    bash(node, f": > {shlex.quote(path)}")


def assert_no_detached_parts(table):
    # A part lost to a failed rebuild is detached as broken, and a row-count or digest oracle alone
    # can miss that: for a part produced by a mutation the entry is still in the mutations list, so
    # the server replays it from the source part and the query results match again while the part
    # under test is gone. Measured on master, which destroys the mutated part twice (first
    # CANNOT_PARSE_INPUT on the empty file, then CORRUPTED_DATA because its own rebuild drops
    # _row_exists) and still answers 500 rows with the expected digest.
    assert node1.query(f"SELECT count() FROM system.detached_parts WHERE database = 'default' AND table = '{table}'") == "0\n"


def single_wide_part_path(table):
    # The fixture is only meaningful on a single wide part in full storage: a compact part keeps its
    # column list nowhere else, and a packed part has no per-column stream files to rebuild from.
    assert node1.query(f"SELECT count() FROM system.parts WHERE database = 'default' AND table = '{table}' AND active") == "1\n"
    assert node1.query(f"SELECT part_type, part_storage_type FROM system.parts WHERE database = 'default' AND table = '{table}' AND active") == "Wide\tFull\n"
    return get_active_part_path(node1, table)


def empty_columns_txt_reload_cycle(table, digest_query, expect_digest, expect_columns_txt):
    """Empty columns.txt, then absent columns.txt, must both leave the part intact.

    The assertions are server-side (row counts and per-column digests) plus one read of the
    quiescent, detached part directory: a rebuild that only lived in memory would leave columns.txt
    empty on disk, and a rebuild that dropped a physical column would read that column back as
    default values while keeping the row count.
    """
    data_path = single_wide_part_path(table)
    assert node1.query(digest_query) == expect_digest

    # An empty (zero-byte) columns.txt must not brick the part.
    node1.query(f"DETACH TABLE {table}")
    truncate_file(node1, data_path + "columns.txt")
    node1.query(f"ATTACH TABLE {table}")
    assert_no_detached_parts(table)
    assert node1.query(digest_query) == expect_digest

    # The rebuilt list must have reached disk, so the part loads from the file next time.
    node1.query(f"DETACH TABLE {table}")
    assert bash(node1, f"cat {shlex.quote(data_path + 'columns.txt')}") == expect_columns_txt
    node1.query(f"ATTACH TABLE {table}")
    assert_no_detached_parts(table)
    assert node1.query(digest_query) == expect_digest

    # An absent columns.txt must still self-heal (regression guard for the pre-existing path).
    node1.query(f"DETACH TABLE {table}")
    bash(node1, f"rm -f {shlex.quote(data_path + 'columns.txt')}")
    node1.query(f"ATTACH TABLE {table}")
    assert_no_detached_parts(table)
    assert node1.query(digest_query) == expect_digest

    node1.query(f"DROP TABLE {table} SYNC")


def empty_columns_txt_digest_cycle(table, digest_query, expect_digest):
    """Same as above for a column whose values, not the file content, are the oracle.

    A count()-only oracle cannot catch a column dropped from the rebuilt list: the row count
    survives and every value is silently synthesized as a default.
    """
    data_path = single_wide_part_path(table)
    assert node1.query(digest_query) == expect_digest

    node1.query(f"DETACH TABLE {table}")
    truncate_file(node1, data_path + "columns.txt")
    node1.query(f"ATTACH TABLE {table}")
    assert_no_detached_parts(table)
    assert node1.query(digest_query) == expect_digest

    # Persistence proof: reload from disk and re-digest.
    node1.query(f"DETACH TABLE {table}")
    node1.query(f"ATTACH TABLE {table}")
    assert_no_detached_parts(table)
    assert node1.query(digest_query) == expect_digest

    node1.query(f"DROP TABLE {table} SYNC")


def test_empty_columns_txt_plain_part(started_cluster):
    table = "t_empty_columns"
    create_wide_part_table(table, "a UInt64, s String", "a")
    node1.query(f"SYSTEM STOP MERGES {table}")
    node1.query(f"INSERT INTO {table} SELECT number, toString(number) FROM numbers(1000)", settings=ONE_PART_PER_INSERT)

    empty_columns_txt_reload_cycle(
        table,
        f"SELECT count(), sum(a), sum(cityHash64(s)) FROM {table}",
        "1000\t499500\t12688800205083956790\n",
        "columns format version: 1\n2 columns:\n`a` UInt64\n`s` String\n",
    )


def test_empty_columns_txt_persistent_virtual_columns(started_cluster):
    # The part physically carries _block_number and _block_offset, which getAllPhysical() does not
    # report; the rebuild must append them in the order writeColumns wrote them (physical columns
    # first) or columns_substreams.txt validation detaches the part.
    table = "t_empty_columns_bn"
    create_wide_part_table(
        table,
        "a UInt64, s String",
        "a",
        ", enable_block_number_column = 1, enable_block_offset_column = 1",
    )
    # Two inserts plus OPTIMIZE FINAL produce a merged part that physically writes the two columns.
    # Merges are stopped only afterwards, so the captured part directory does not move.
    node1.query(f"INSERT INTO {table} SELECT number, toString(number) FROM numbers(500)", settings=ONE_PART_PER_INSERT)
    node1.query(f"INSERT INTO {table} SELECT number + 500, toString(number) FROM numbers(500)", settings=ONE_PART_PER_INSERT)
    node1.query(f"OPTIMIZE TABLE {table} FINAL")
    node1.query(f"SYSTEM STOP MERGES {table}")

    empty_columns_txt_reload_cycle(
        table,
        f"SELECT count(), sum(a), sum(cityHash64(s)) FROM {table}",
        "1000\t499500\t12444261028201855304\n",
        "columns format version: 1\n4 columns:\n`a` UInt64\n`s` String\n`_block_number` UInt64\n`_block_offset` UInt64\n",
    )


def test_empty_columns_txt_lightweight_delete_mask(started_cluster):
    # The rebuild must keep _row_exists, otherwise the deletion mask is silently dropped and the
    # deleted rows reappear.
    table = "t_empty_columns_ld"
    create_wide_part_table(table, "a UInt64, s String", "a")
    node1.query(f"INSERT INTO {table} SELECT number, toString(number) FROM numbers(1000)", settings=ONE_PART_PER_INSERT)
    # Materialize the deletion mask, then stop merges so the mutated part directory does not move.
    node1.query(f"DELETE FROM {table} WHERE a % 2 = 0", settings={"mutations_sync": 2})
    node1.query(f"SYSTEM STOP MERGES {table}")

    empty_columns_txt_reload_cycle(
        table,
        f"SELECT count(), sum(a), sum(cityHash64(s)) FROM {table}",
        "500\t250000\t6753304225218678229\n",
        "columns format version: 1\n3 columns:\n`a` UInt64\n`s` String\n`_row_exists` UInt8\n",
    )


def test_empty_columns_txt_tuple_column(started_cluster):
    # SerializationTuple emits only element streams and no <column>.bin, so presence detection must
    # enumerate the column's streams instead of probing one fixed path, or the whole Tuple is
    # dropped from the rebuilt list and read back as defaults.
    table = "t_empty_columns_tuple"
    create_wide_part_table(table, "a UInt64, t Tuple(x UInt64, y String)", "a")
    node1.query(f"SYSTEM STOP MERGES {table}")
    node1.query(f"INSERT INTO {table} SELECT number, (number * 2, toString(number)) FROM numbers(1000)", settings=ONE_PART_PER_INSERT)

    empty_columns_txt_digest_cycle(table, f"SELECT sum(t.x) FROM {table}", "999000\n")


def test_empty_columns_txt_map_column(started_cluster):
    # A Map column likewise has no <column>.bin: only size, key and value streams.
    table = "t_empty_columns_map"
    create_wide_part_table(table, "a UInt64, m Map(String, UInt64)", "a")
    node1.query(f"SYSTEM STOP MERGES {table}")
    node1.query(f"INSERT INTO {table} SELECT number, map('k', number * 3) FROM numbers(1000)", settings=ONE_PART_PER_INSERT)

    empty_columns_txt_digest_cycle(table, f"SELECT sum(m['k']) FROM {table}", "1498500\n")


def test_empty_columns_txt_bucketed_map_column(started_cluster):
    # A bucketed Map writes m.buckets_info, m.0.size0, m.0.keys, ..., none of which match the
    # default serialization's streams (m.size0, m.keys, ...). Presence detection must not assume the
    # default serialization, or the column is judged absent and dropped, and columns_substreams.txt
    # validation then detaches the part.
    table = "t_empty_columns_map_bucketed"
    create_wide_part_table(
        table,
        "a UInt64, m Map(String, UInt64)",
        "a",
        ", map_serialization_version = 'with_buckets',"
        " map_serialization_version_for_zero_level_parts = 'with_buckets',"
        " max_buckets_in_map = 11, map_buckets_strategy = 'constant'",
    )
    node1.query(f"SYSTEM STOP MERGES {table}")
    node1.query(f"INSERT INTO {table} SELECT number, map('k', number * 3) FROM numbers(1000)", settings=ONE_PART_PER_INSERT)

    empty_columns_txt_digest_cycle(table, f"SELECT sum(m['k']) FROM {table}", "1498500\n")


def test_empty_columns_txt_shared_nested_offsets(started_cluster):
    # With share_nested_offsets a Nested column added by ALTER (n.b) has no data of its own; its
    # only on-disk stream is the offsets stream owned by n.a. The rebuild must decide presence from
    # a column's own streams, not from any stream that merely exists, or n.b is wrongly included and
    # columns_substreams.txt validation detaches the part.
    table = "t_empty_columns_nested"
    create_wide_part_table(table, "id UInt64, `n.a` Array(UInt64)", "id", ", share_nested_offsets = 1")
    node1.query(f"SYSTEM STOP MERGES {table}")
    node1.query(f"INSERT INTO {table} SELECT number, [number, number + 1] FROM numbers(1000)", settings=ONE_PART_PER_INSERT)
    node1.query(f"ALTER TABLE {table} ADD COLUMN `n.b` Array(String)")

    data_path = single_wide_part_path(table)
    assert node1.query(f"SELECT sum(arraySum(n.a)) FROM {table}") == "1000000\n"

    node1.query(f"DETACH TABLE {table}")
    truncate_file(node1, data_path + "columns.txt")
    node1.query(f"ATTACH TABLE {table}")

    # A single active part proves validation passed, i.e. the data-less n.b was not included.
    assert node1.query(f"SELECT count() FROM system.parts WHERE database = 'default' AND table = '{table}' AND active") == "1\n"
    assert_no_detached_parts(table)
    assert node1.query(f"SELECT sum(arraySum(n.a)) FROM {table}") == "1000000\n"

    # Persistence proof: reload from disk and re-digest.
    node1.query(f"DETACH TABLE {table}")
    node1.query(f"ATTACH TABLE {table}")
    assert_no_detached_parts(table)
    assert node1.query(f"SELECT sum(arraySum(n.a)) FROM {table}") == "1000000\n"

    node1.query(f"DROP TABLE {table} SYNC")


def test_empty_columns_txt_discarded_substreams_refused(started_cluster):
    # columns_substreams.txt present but discarded as corrupted: recovery must refuse instead of
    # inferring presence from the default serialization. A bucketed Map is stored as m.buckets_info,
    # m.0.keys, ..., so the default streams are absent, the column would be judged missing, and
    # writeColumns would persist that omission, leaving an intact column reading back as all-default
    # values. Refusing detaches the part, which is recoverable.
    #
    # The asserted outcome (nothing active, the part kept in detached) is also what a server that
    # simply fails to parse the empty file produces, so this case pins the deliberate refusal; it is
    # not an oracle for the recovery itself.
    table = "t_empty_columns_discarded"
    create_wide_part_table(
        table,
        "a UInt64, m Map(String, UInt64)",
        "a",
        ", map_serialization_version = 'with_buckets',"
        " map_serialization_version_for_zero_level_parts = 'with_buckets',"
        " max_buckets_in_map = 11, map_buckets_strategy = 'constant'",
    )
    node1.query(f"SYSTEM STOP MERGES {table}")
    node1.query(f"INSERT INTO {table} SELECT number, map('k', number * 3) FROM numbers(1000)", settings=ONE_PART_PER_INSERT)

    data_path = single_wide_part_path(table)
    assert node1.query(f"SELECT sum(m['k']) FROM {table}") == "1498500\n"

    node1.query(f"DETACH TABLE {table}")
    # Give the first substream a prefix that does not match its column: the rename-bug corruption
    # that loadColumnsSubstreams discards for wide parts. Rewriting whichever substream comes first
    # keeps this independent of the stream names the serialization versions produce.
    bash(node1, f"sed -i '0,/^\\t/s/^\\t.*/\\tnot_a_valid_prefix/' {shlex.quote(data_path + 'columns_substreams.txt')}")
    truncate_file(node1, data_path + "columns.txt")
    node1.query(f"ATTACH TABLE {table}")

    assert node1.query(f"SELECT count() FROM system.parts WHERE database = 'default' AND table = '{table}' AND active") == "0\n"
    # The part is kept for recovery rather than deleted.
    assert node1.query(f"SELECT count() FROM system.detached_parts WHERE database = 'default' AND table = '{table}'") == "1\n"

    node1.query(f"DROP TABLE {table} SYNC")


def test_empty_columns_txt_without_substreams_file(started_cluster):
    # columns_substreams.txt is the primary presence oracle, but a part predating it must still
    # recover by enumerating each column's own streams. Remove both files so the fallback is
    # exercised, on a Tuple that a single-fixed-path probe would wrongly drop.
    table = "t_empty_columns_fallback"
    create_wide_part_table(table, "a UInt64, t Tuple(x UInt64, y String)", "a")
    node1.query(f"SYSTEM STOP MERGES {table}")
    node1.query(f"INSERT INTO {table} SELECT number, (number * 2, toString(number)) FROM numbers(1000)", settings=ONE_PART_PER_INSERT)

    data_path = single_wide_part_path(table)
    assert node1.query(f"SELECT sum(t.x) FROM {table}") == "999000\n"

    node1.query(f"DETACH TABLE {table}")
    truncate_file(node1, data_path + "columns.txt")
    bash(node1, f"rm -f {shlex.quote(data_path + 'columns_substreams.txt')}")
    node1.query(f"ATTACH TABLE {table}")
    assert_no_detached_parts(table)
    assert node1.query(f"SELECT sum(t.x) FROM {table}") == "999000\n"

    # Persistence proof: reload from disk and re-digest.
    node1.query(f"DETACH TABLE {table}")
    node1.query(f"ATTACH TABLE {table}")
    assert_no_detached_parts(table)
    assert node1.query(f"SELECT sum(t.x) FROM {table}") == "999000\n"

    node1.query(f"DROP TABLE {table} SYNC")


def test_empty_columns_txt_without_substreams_file_shared_offsets(started_cluster):
    # On the legacy no-substreams path a column's presence is decided by enumerating its own
    # streams. With share_nested_offsets the offsets stream is named after the Nested table, so it
    # exists as soon as any sibling has data: accepting it lists a data-less n.b in the rebuilt
    # columns.txt and CHECK TABLE then reports NO_FILE_IN_DATA_PART.
    table = "t_empty_columns_shared_offsets"
    create_wide_part_table(table, "id UInt64, `n.a` Array(UInt64)", "id", ", share_nested_offsets = 1")
    node1.query(f"SYSTEM STOP MERGES {table}")
    node1.query(f"INSERT INTO {table} SELECT number, [number, number + 1] FROM numbers(1000)", settings=ONE_PART_PER_INSERT)
    node1.query(f"ALTER TABLE {table} ADD COLUMN `n.b` Array(String)")

    data_path = single_wide_part_path(table)
    node1.query(f"DETACH TABLE {table}")
    truncate_file(node1, data_path + "columns.txt")
    bash(node1, f"rm -f {shlex.quote(data_path + 'columns_substreams.txt')}")
    node1.query(f"ATTACH TABLE {table}")

    # CHECK TABLE returns 1 for a table with no parts at all, so assert the part is still there.
    assert_no_detached_parts(table)
    assert node1.query(f"SELECT sum(arraySum(n.a)) FROM {table}") == "1000000\n"
    assert node1.query(f"CHECK TABLE {table} SETTINGS check_query_single_value_result = 1") == "1\n"
    node1.query(f"DROP TABLE {table} SYNC")

    # Control: the same shape with n.b written with data must keep n.b, so the rule above rejects
    # only streams the column does not own.
    control = "t_empty_columns_shared_offsets_data"
    create_wide_part_table(control, "id UInt64, `n.a` Array(UInt64), `n.b` Array(String)", "id", ", share_nested_offsets = 1")
    node1.query(f"SYSTEM STOP MERGES {control}")
    node1.query(f"INSERT INTO {control} SELECT number, [number, number + 1], ['x', 'y'] FROM numbers(1000)", settings=ONE_PART_PER_INSERT)

    control_path = single_wide_part_path(control)
    node1.query(f"DETACH TABLE {control}")
    truncate_file(node1, control_path + "columns.txt")
    bash(node1, f"rm -f {shlex.quote(control_path + 'columns_substreams.txt')}")
    node1.query(f"ATTACH TABLE {control}")

    assert_no_detached_parts(control)
    assert node1.query(f"SELECT sum(arraySum(n.a)), sum(length(n.b)) FROM {control}") == "1000000\t2000\n"
    assert node1.query(f"CHECK TABLE {control} SETTINGS check_query_single_value_result = 1") == "1\n"
    node1.query(f"DROP TABLE {control} SYNC")


def test_empty_columns_txt_projection_part(started_cluster):
    # A projection lists the parent virtuals it stores among its own physical columns, so a rebuild
    # that appends the persistent virtuals unconditionally writes `_block_number` twice; the
    # projection then fails to load with DUPLICATE_COLUMN and is marked broken.
    table = "t_empty_columns_projection"
    create_wide_part_table(
        table,
        "id UInt64, v UInt64",
        "id",
        ", enable_block_number_column = 1, enable_block_offset_column = 1,"
        " allow_commit_order_projection = 1, deduplicate_merge_projection_mode = 'rebuild'",
    )
    node1.query(f"ALTER TABLE {table} ADD PROJECTION p (SELECT id, v, _block_number ORDER BY v)")
    node1.query(f"INSERT INTO {table} SELECT number, number * 2 FROM numbers(300)", settings=ONE_PART_PER_INSERT)
    node1.query(f"INSERT INTO {table} SELECT number + 300, number FROM numbers(300)", settings=ONE_PART_PER_INSERT)
    # Merge so the projection is materialized in one part, then freeze the layout.
    node1.query(f"OPTIMIZE TABLE {table} FINAL")
    node1.query(f"SYSTEM STOP MERGES {table}")

    proj_path = node1.query(f"SELECT path FROM system.projection_parts WHERE database = 'default' AND table = '{table}' AND active").strip()
    assert proj_path.startswith("/"), f"Projection path is relative: {proj_path}"
    assert node1.query(f"SELECT count(), sum(v) FROM {table}") == "600\t134550\n"

    node1.query(f"DETACH TABLE {table}")
    truncate_file(node1, proj_path + "columns.txt")
    node1.query(f"ATTACH TABLE {table}")

    assert node1.query(f"SELECT count() FROM system.projection_parts WHERE database = 'default' AND table = '{table}' AND active AND NOT is_broken") == "1\n"
    assert bash(node1, f"grep -c '^`_block_number`' {shlex.quote(proj_path + 'columns.txt')}").strip() == "1"
    assert node1.query(f"SELECT count(), sum(v) FROM {table}") == "600\t134550\n"

    node1.query(f"DROP TABLE {table} SYNC")
