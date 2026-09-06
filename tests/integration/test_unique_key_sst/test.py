import logging
import shlex

import pytest

from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/storage_policy.xml"],
    stay_alive=True,
    with_minio=True,
)

UK_SETTINGS = {"allow_experimental_unique_key": "1"}

EXPECTED_ROWS = "10\ta\n20\tb\n30\tc\n"


def bash(node, command):
    return node.exec_in_container(["bash", "-c", command], privileged=True, user="root")


def get_active_part_path(node, table):
    path = node.query(
        f"SELECT path FROM system.parts WHERE database = 'default' AND table = '{table}' AND active"
    ).strip()
    assert path.startswith("/"), f"Path is relative: {path}"
    return path


def file_exists(node, path):
    return bash(node, f"test -f {shlex.quote(path)} && echo yes || echo no").strip() == "yes"


def file_size(node, path):
    return int(bash(node, f"stat -c%s {shlex.quote(path)}").strip())


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_unique_key_sst_roundtrip_on_s3(started_cluster):
    # UNIQUE KEY on S3: SST sidecar read/write through IDataPartStorage.
    # The SST reader/writer goes through IDataPartStorage::readFile/writeFile,
    # so it works on any disk type. This test verifies the round-trip on S3.
    node.query("DROP TABLE IF EXISTS uk_s3")
    node.query(
        """
        CREATE TABLE uk_s3 (id UInt64, v String)
        ENGINE = MergeTree
        UNIQUE KEY (id)
        ORDER BY (id)
        SETTINGS disk = 's3_disk', min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1
        """,
        settings=UK_SETTINGS,
    )
    node.query(
        "INSERT INTO uk_s3 VALUES (10, 'a'), (20, 'b'), (30, 'c')",
        settings=UK_SETTINGS,
    )
    assert node.query("SELECT id, v FROM uk_s3 ORDER BY id") == EXPECTED_ROWS

    # DETACH + ATTACH: load-time validation reads the SST sidecar back from
    # S3 through IDataPartStorage.
    node.query("DETACH TABLE uk_s3 SYNC")
    node.query("ATTACH TABLE uk_s3", settings=UK_SETTINGS)
    assert (
        node.query(
            """
            SELECT count() FROM system.parts
            WHERE database = currentDatabase() AND table = 'uk_s3' AND active
            """
        )
        == "1\n"
    )
    assert node.query("SELECT id, v FROM uk_s3 ORDER BY id") == EXPECTED_ROWS

    node.query("DROP TABLE uk_s3")


def test_unique_key_on_s3_survives_restart(started_cluster):
    # Restart the whole node: parts are re-loaded from S3 and the SST sidecar
    # is re-read on startup.
    node.query("DROP TABLE IF EXISTS uk_s3")
    node.query(
        """
        CREATE TABLE uk_s3 (id UInt64, v String)
        ENGINE = MergeTree
        UNIQUE KEY (id)
        ORDER BY (id)
        SETTINGS disk = 's3_disk', min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1
        """,
        settings=UK_SETTINGS,
    )
    node.query(
        "INSERT INTO uk_s3 VALUES (10, 'a'), (20, 'b'), (30, 'c')",
        settings=UK_SETTINGS,
    )

    node.restart_clickhouse()
    assert node.query("SELECT id, v FROM uk_s3 ORDER BY id") == EXPECTED_ROWS

    node.query("DROP TABLE uk_s3")


def test_unique_key_switch_policy_to_s3(started_cluster):
    # Route a UNIQUE KEY table's parts to remote storage after creation. UK
    # tables reject `ALTER ... PARTITION` (so no `MOVE PARTITION`) and
    # `MODIFY SETTING disk` can't switch a local table to S3 (its single-disk
    # temp policy fails checkCompatibleWith), so the only supported path is
    # `MODIFY SETTING storage_policy` to `s3_extending` - a policy that keeps
    # the old `default` volume/disk (for checkCompatibleWith) and puts S3 first,
    # so new inserts (and their `unique_key_index.sst`) land on S3.
    node.query("DROP TABLE IF EXISTS uk_local SYNC")
    node.query(
        """
        CREATE TABLE uk_local (id UInt64, v String)
        ENGINE = MergeTree
        UNIQUE KEY (id)
        ORDER BY (id)
        SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1
        """,
        settings=UK_SETTINGS,
    )
    node.query(
        "INSERT INTO uk_local VALUES (10, 'a'), (20, 'b'), (30, 'c')",
        settings=UK_SETTINGS,
    )

    node.query(
        "ALTER TABLE uk_local MODIFY SETTING storage_policy = 's3_extending'",
        settings=UK_SETTINGS,
    )

    # The pre-ALTER part stays on the local disk and remains readable.
    assert node.query("SELECT id, v FROM uk_local ORDER BY id") == EXPECTED_ROWS

    # A new insert writes its part (and `unique_key_index.sst`) onto the S3 disk.
    node.query("INSERT INTO uk_local VALUES (40, 'd')", settings=UK_SETTINGS)

    # Locate the new part by the row it holds (`_part`) rather than counting S3
    # parts: a background merge could fold both parts into one at any time, and
    # that merged part is reserved from volume 0 (S3) too - so this assertion
    # holds whether or not a merge has happened.
    assert node.query(
        "SELECT disk_name FROM system.parts WHERE database = 'default' AND table = 'uk_local'"
        " AND active AND name = (SELECT _part FROM uk_local WHERE id = 40)"
    ).strip() == "s3_disk"

    # And it reads back through the S3-backed SST.
    assert node.query("SELECT id, v FROM uk_local ORDER BY id") == "10\ta\n20\tb\n30\tc\n40\td\n"

    node.query("DROP TABLE uk_local SYNC")


def test_unique_key_sst_checksums(started_cluster):
    # Converted from stateless test 04836_unique_key_sst_checksums.sh (stateless
    # tests must not modify the server's data on disk).
    #
    # UNIQUE KEY load-time dense-index lifecycle on the local disk, now that
    # `unique_key_index.sst` is recorded in `checksums.txt`:
    # 1. Normal round-trip: a valid SST survives DETACH + ATTACH.
    # 2. Size-preserving corruption: passes the load-time size check (hashes are not
    #    verified at load), caught by the SST validation, rebuilt.
    # 3. Missing SST: rejected by the size check, part detached as broken (fail closed;
    #    no rebuild - the checksum entry makes it a plain consistency failure, unlike
    #    the no-entry repair covered by test_corrupted_part_files).
    # 4. Readonly startup: rebuild is impossible, so a corrupt SST fails the ATTACH
    #    with UNIQUE_KEY_DENSE_INDEX_UNREADABLE and leaves the file untouched.
    #
    # The table carries a sparsely-serialized all-default UK column, so the Section 2
    # rebuild also covers the `ColumnSparse` densify path in `readUniqueKeyColumns`.
    node.query("DROP TABLE IF EXISTS uk_sst_checksums SYNC")

    # Wide part + sparse serialization forcing: compact parts have no per-column
    # serialization kinds, so only a wide part stores the all-default column `a`
    # sparsely; `id` keeps the compound key unique.
    node.query(
        """
        CREATE TABLE uk_sst_checksums (a UInt64, id UInt64, v String)
        ENGINE = MergeTree
        UNIQUE KEY (a, id)
        ORDER BY (a, id)
        SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1, ratio_of_defaults_for_sparse_serialization = 0.9
        """,
        settings=UK_SETTINGS,
    )

    node.query("INSERT INTO uk_sst_checksums SELECT 0, number, toString(number) FROM numbers(500)")

    # sparse_uk_column_stored
    assert node.query(
        "SELECT if(serialization_kind = 'Sparse', 'yes', 'no') FROM system.parts_columns"
        " WHERE database = 'default' AND table = 'uk_sst_checksums' AND active AND column = 'a'"
    ) == "yes\n"

    part_path = get_active_part_path(node, "uk_sst_checksums")

    # sst_present
    assert file_exists(node, part_path + "unique_key_index.sst")

    # sst_in_checksums
    assert bash(node, f"grep -qa unique_key_index.sst {shlex.quote(part_path + 'checksums.txt')} && echo yes || echo no").strip() == "yes"

    node.query("DETACH TABLE uk_sst_checksums")
    node.query("ATTACH TABLE uk_sst_checksums", settings={"send_logs_level": "error"})

    # active_parts_after_attach / rows_after_attach
    assert node.query("SELECT count() FROM system.parts WHERE database = 'default' AND table = 'uk_sst_checksums' AND active") == "1\n"
    assert node.query("SELECT count(), sum(id) FROM uk_sst_checksums") == "500\t124750\n"

    # --- Size-preserving corruption: overwrite bytes in the middle without changing
    # the length, so the load-time size check passes and the SST validation detects
    # the damage and rebuilds the file.
    part_path = get_active_part_path(node, "uk_sst_checksums")
    full_size = file_size(node, part_path + "unique_key_index.sst")
    sst_path = shlex.quote(part_path + "unique_key_index.sst")
    node.query("DETACH TABLE uk_sst_checksums")
    bash(node, f"printf 'XXXXXXXXXXXXXXXX' | dd of={sst_path} bs=1 seek={full_size // 2} conv=notrunc status=none")

    # sst_size_unchanged_after_damage
    assert file_size(node, part_path + "unique_key_index.sst") == full_size

    node.query("ATTACH TABLE uk_sst_checksums", settings={"send_logs_level": "error"})

    # active_parts_after_corrupt_rebuild / rows_after_corrupt_rebuild
    assert node.query("SELECT count() FROM system.parts WHERE database = 'default' AND table = 'uk_sst_checksums' AND active") == "1\n"
    assert node.query("SELECT count(), sum(id) FROM uk_sst_checksums") == "500\t124750\n"

    # --- Missing SST: the checksum entry turns it into a plain consistency failure.
    part_path = get_active_part_path(node, "uk_sst_checksums")
    sst_path = shlex.quote(part_path + "unique_key_index.sst")
    node.query("DETACH TABLE uk_sst_checksums")
    bash(node, f"rm -f {sst_path}")
    # `none` (not `error`): the load logs <Error> "Part is broken" while moving
    # the part to detached, and that level would be shipped to the client.
    node.query("ATTACH TABLE uk_sst_checksums", settings={"send_logs_level": "none"})

    # active_parts_after_missing_sst / detached_parts_after_missing_sst
    assert node.query("SELECT count() FROM system.parts WHERE database = 'default' AND table = 'uk_sst_checksums' AND active") == "0\n"
    assert node.query("SELECT count() > 0 FROM system.detached_parts WHERE database = 'default' AND table = 'uk_sst_checksums'") == "1\n"

    node.query("DROP TABLE uk_sst_checksums SYNC")

    # --- Readonly startup. Default part format (compact for this small insert) to
    # cover that path too. Validation still runs (read-only I/O) but rebuild is
    # impossible, so a corrupt SST must fail the ATTACH. The corruption preserves
    # the size, otherwise the part is rejected before the readonly branch.
    node.query("DROP TABLE IF EXISTS uk_sst_ro SYNC")
    node.query(
        """
        CREATE TABLE uk_sst_ro (id UInt64, v String)
        ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id)
        """,
        settings=UK_SETTINGS,
    )
    node.query("INSERT INTO uk_sst_ro SELECT number, toString(number) FROM numbers(200)")
    node.query("ALTER TABLE uk_sst_ro MODIFY SETTING table_readonly = 1")

    ro_path = get_active_part_path(node, "uk_sst_ro")
    ro_full_size = file_size(node, ro_path + "unique_key_index.sst")
    ro_sst = shlex.quote(ro_path + "unique_key_index.sst")
    ro_sst_keep = shlex.quote(ro_path + "unique_key_index.sst.keep")
    node.query("DETACH TABLE uk_sst_ro")
    bash(node, f"cp {ro_sst} {ro_sst_keep}")
    bash(node, f"printf 'XXXXXXXXXXXXXXXX' | dd of={ro_sst} bs=1 seek={ro_full_size // 2} conv=notrunc status=none")

    # readonly_attach_with_corrupt_sst_fails
    assert "UNIQUE_KEY_DENSE_INDEX_UNREADABLE" in node.query_and_get_error("ATTACH TABLE uk_sst_ro")

    # corrupt_sst_left_in_place
    assert file_size(node, ro_path + "unique_key_index.sst") == ro_full_size

    bash(node, f"mv {ro_sst_keep} {ro_sst}")
    node.query("ATTACH TABLE uk_sst_ro", settings={"send_logs_level": "error"})

    # readonly_attach_after_restore
    assert node.query("SELECT count() FROM uk_sst_ro") == "200\n"

    node.query("ALTER TABLE uk_sst_ro MODIFY SETTING table_readonly = 0")
    node.query("DROP TABLE uk_sst_ro SYNC")
