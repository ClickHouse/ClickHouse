"""Tests for backups placed or tampered with directly on the server's "backups" disk:
canned backups made by old ClickHouse versions are restored, and `.backup` metadata
files are edited to test validation on RESTORE.

Converted from stateless tests because stateless tests must not modify the server's
data on disk (including the backups disk).
"""

import hashlib
import os.path

import pytest

from helpers.cluster import ClickHouseCluster, run_and_check

script_dir = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/backups_disk.xml"],
    external_dirs=["/backups/"],
)

# The root of the "backups" disk inside the container (see configs/backups_disk.xml).
BACKUPS_DISK_ROOT = "/backups"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def install_predefined_backup(src_backup_filename, dest_dir):
    """Copies a canned backup from the "backups" folder next to this test into the
    node's "backups" disk and returns the path to the backup relative to that disk.
    This replaces tests/queries/0_stateless/helpers/install_predefined_backup.sh
    (which symlinked the backup into a database-named subdirectory of the disk)."""
    dest_relative_path = f"{dest_dir}/{src_backup_filename}"
    dest_path = f"{BACKUPS_DISK_ROOT}/{dest_relative_path}"
    # copy_file_to_container passes the file base64-encoded on the `docker exec` command
    # line, which fails with E2BIG for backups of this size; use `docker cp` instead.
    node.exec_in_container(
        ["bash", "-c", f"mkdir -p $(dirname {dest_path})"],
        privileged=True,
        user="root",
    )
    run_and_check(
        [
            "docker",
            "cp",
            os.path.join(script_dir, "backups", src_backup_filename),
            f"{node.docker_id}:{dest_path}",
        ]
    )
    return dest_relative_path


def remove_from_backups_disk(relative_path_pattern):
    """Removes installed or created backups from the "backups" disk."""
    node.exec_in_container(
        ["bash", "-c", f"rm -rf {BACKUPS_DISK_ROOT}/{relative_path_pattern}"],
        privileged=True,
        user="root",
    )


def test_restore_table_with_broken_part(started_cluster):
    # Converted from stateless test 02864_restore_table_with_broken_part.sh.
    # In this test we restore from "backups/with_broken_part.zip".
    backup_name = install_predefined_backup("with_broken_part.zip", "test_restore_table_with_broken_part")

    node.query("DROP TABLE IF EXISTS tbl")

    # First try to restore with the setting `restore_broken_parts_as_detached` set to false.
    err = node.query_and_get_error(f"RESTORE TABLE default.tbl AS tbl FROM Disk('backups', '{backup_name}')")
    assert "data.bin doesn't exist" in err
    assert "while restoring part all_2_2_0" in err

    node.query("DROP TABLE IF EXISTS tbl")

    # Then try to restore with the setting `restore_broken_parts_as_detached` set to true.
    result = node.query(f"RESTORE TABLE default.tbl AS tbl FROM Disk('backups', '{backup_name}') SETTINGS restore_broken_parts_as_detached = true")
    assert result.split("\t")[1].strip() == "RESTORED"

    assert node.query("SELECT * FROM tbl ORDER BY x") == "1\n3\n"
    assert node.query("SELECT name, reason FROM system.detached_parts WHERE database = 'default' AND table = 'tbl'") == "broken-from-backup_all_2_2_0\tbroken-from-backup\n"

    node.query("DROP TABLE tbl SYNC")
    remove_from_backups_disk("test_restore_table_with_broken_part")


def test_restore_from_old_backup_with_matview_inner_table_metadata(started_cluster):
    # Converted from stateless test
    # 03001_restore_from_old_backup_with_matview_inner_table_metadata.sh.
    # In this test we restore from "backups/old_backup_with_matview_inner_table_metadata.zip".
    backup_name = install_predefined_backup(
        "old_backup_with_matview_inner_table_metadata.zip",
        "test_restore_matview_inner_table_metadata",
    )

    node.query("DROP TABLE IF EXISTS mv")
    node.query("DROP TABLE IF EXISTS src")

    result = node.query(f"RESTORE DATABASE mydb AS default FROM Disk('backups', '{backup_name}') SETTINGS allow_different_database_def=true")
    assert "RESTORED" in result

    assert node.query("SELECT toDateTime(timestamp, 'UTC') AS ts, c12 FROM mv ORDER BY ts") == ("2024-02-22 07:00:00\t00\n2024-02-22 07:00:01\t11\n2024-02-22 07:00:02\t22\n")

    node.query("DROP TABLE mv SYNC")
    node.query("DROP TABLE src SYNC")
    remove_from_backups_disk("test_restore_matview_inner_table_metadata")


def test_backup_and_clear_old_temporary_directories(started_cluster):
    # Converted from stateless test 03214_backup_and_clear_old_temporary_directories.sh.
    # In this test we restore from "backups/mt_250_parts.zip".
    backup_name = install_predefined_backup("mt_250_parts.zip", "test_clear_old_temporary_directories")

    node.query("DROP TABLE IF EXISTS manyparts")
    node.query("CREATE TABLE manyparts (x Int64) ENGINE=MergeTree ORDER BY tuple() SETTINGS merge_tree_clear_old_temporary_directories_interval_seconds=1, temporary_directories_lifetime=1")

    # RESTORE must protect its temporary directories from removing.
    result = node.query(f"RESTORE TABLE default.mt_250_parts AS manyparts FROM Disk('backups', '{backup_name}') SETTINGS allow_different_table_def=true")
    assert "RESTORED" in result

    assert node.query("SELECT count(), sum(x) FROM manyparts") == "250\t31375\n"

    node.query("DROP TABLE manyparts SYNC")
    remove_from_backups_disk("test_clear_old_temporary_directories")


def test_old_backup_without_access_entities_dependents(started_cluster):
    # Converted from stateless test 03231_old_backup_without_access_entities_dependents.sh.
    # In this test we restore from "backups/old_backup_without_access_entities_dependents.zip".
    backup_name = install_predefined_backup(
        "old_backup_without_access_entities_dependents.zip",
        "test_old_backup_without_access_entities_dependents",
    )

    node.query("DROP USER IF EXISTS user_03231")
    node.query("DROP ROLE IF EXISTS role_a_03231, role_b_03231")

    node.query(f"RESTORE ALL FROM Disk('backups', '{backup_name}') FORMAT Null")

    assert node.query("SHOW CREATE USER user_03231") == "CREATE USER user_03231 IDENTIFIED WITH no_password DEFAULT ROLE role_a_03231 SETTINGS custom_x = \\'x\\'\n"
    assert node.query("SHOW GRANTS FOR user_03231") == "GRANT role_a_03231 TO user_03231\n"
    assert node.query("SHOW CREATE ROLE role_a_03231") == "CREATE ROLE role_a_03231\n"
    assert node.query("SHOW GRANTS FOR role_a_03231") == "GRANT INSERT ON *.* TO role_a_03231\nGRANT role_b_03231 TO role_a_03231\n"
    assert node.query("SHOW CREATE ROLE role_b_03231") == "CREATE ROLE role_b_03231\n"
    assert node.query("SHOW GRANTS FOR role_b_03231") == "GRANT SELECT ON *.* TO role_b_03231\n"

    node.query("DROP USER user_03231")
    node.query("DROP ROLE role_a_03231, role_b_03231")
    remove_from_backups_disk("test_old_backup_without_access_entities_dependents")


def test_restore_validates_backup_entry_paths(started_cluster):
    # Converted from stateless test 04054_backup_restore_validate_entry_paths.sh.
    # Test that RESTORE rejects backup entries with path traversal sequences (../).
    node.query("DROP TABLE IF EXISTS tbl_backup_traversal")
    node.query("CREATE TABLE tbl_backup_traversal (id UInt64, data String) ENGINE = MergeTree ORDER BY id")
    node.query("INSERT INTO tbl_backup_traversal VALUES (1, 'hello')")

    # Tests 9 and 10 need an entry that RESTORE actually lists, so their names must address a
    # real part. `recreate_table` reproduces this same single-insert part.
    part = node.query(
        "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'tbl_backup_traversal' AND active"
    ).strip()
    assert part, "no active part to address"

    extra_content = "EXTRA_FILE_CONTENT_HERE"
    extra_size = len(extra_content)
    extra_checksum = hashlib.md5(extra_content.encode()).hexdigest()
    extra_data_path = "data/default/tbl_backup_traversal/extra_payload.bin"

    def inject_and_restore(suffix, injected_name, expected_error, injected_data_file=None, companion_names=()):
        """Creates a backup, injects an extra file entry into its .backup metadata, and
        attempts to restore. Expects the specified error (e.g. INSECURE_PATH,
        BACKUP_DAMAGED). `injected_data_file` defaults to `extra_data_path`.
        Each name in `companion_names` gets its own entry with the same size, checksum and
        data file: a name that RESTORE reads back under a different key needs that key too."""
        if injected_data_file is None:
            injected_data_file = extra_data_path
        bname = f"test_validate_entry_paths_{suffix}"

        node.query(f"BACKUP TABLE tbl_backup_traversal TO Disk('backups', '{bname}')")

        bpath = f"{BACKUPS_DISK_ROOT}/{bname}"
        injected_entries = "".join(
            f"<file><name>{name}</name><size>{extra_size}</size><checksum>{extra_checksum}</checksum><data_file>{injected_data_file}</data_file></file>"
            for name in [injected_name, *companion_names]
        )
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"mkdir -p {bpath}/{os.path.dirname(extra_data_path)} && echo -n '{extra_content}' > {bpath}/{extra_data_path} && sed -i 's|</contents>|{injected_entries}</contents>|' {bpath}/.backup",
            ],
            privileged=True,
            user="root",
        )

        node.query("DROP TABLE IF EXISTS tbl_backup_traversal")
        err = node.query_and_get_error(f"RESTORE TABLE tbl_backup_traversal FROM Disk('backups', '{bname}')")
        assert expected_error in err

    def recreate_table():
        """Recreates the table from scratch between tests. The drop is required: test 8 leaves a
        restored table behind, and inserting into it would add a second part, so the part name
        cached above would no longer be the only part a backup taken here contains."""
        node.query("DROP TABLE IF EXISTS tbl_backup_traversal SYNC")
        node.query("CREATE TABLE tbl_backup_traversal (id UInt64, data String) ENGINE = MergeTree ORDER BY id")
        node.query("INSERT INTO tbl_backup_traversal VALUES (1, 'hello')")

    # Test 1: relative path traversal in <name>.
    inject_and_restore(
        "rel",
        "data/default/tbl_backup_traversal/all_0_0_0/../../../../../../../tmp/backup_traversal_test_output.txt",
        "INSECURE_PATH",
    )

    # Verify the file was NOT written outside the backup directory.
    assert not node.path_exists("/tmp/backup_traversal_test_output.txt"), "FAIL: file written to /tmp/"

    # Test 2: absolute path in <name>.
    recreate_table()
    inject_and_restore("abs", "/tmp/backup_absolute_path_test_output.xml", "INSECURE_PATH")

    # Test 3: path traversal in <data_file> (source path for reading from the backup).
    recreate_table()
    inject_and_restore(
        "datafile",
        "data/default/tbl_backup_traversal/extra_payload.bin",
        "INSECURE_PATH",
        "data/default/tbl_backup_traversal/all_0_0_0/../../../../../../../etc/passwd",
    )

    # Test 4: empty <name> should be rejected as damaged.
    recreate_table()
    inject_and_restore("empty", "", "BACKUP_DAMAGED")

    # Test 5: "." as <name> should be rejected as damaged.
    recreate_table()
    inject_and_restore("dot", ".", "BACKUP_DAMAGED")

    # Test 6: bare ".." as <name>.
    recreate_table()
    inject_and_restore("dotdot", "..", "INSECURE_PATH")

    # Test 7: absolute path in <data_file>.
    recreate_table()
    inject_and_restore(
        "abs_datafile",
        "data/default/tbl_backup_traversal/extra_payload.bin",
        "INSECURE_PATH",
        "/etc/passwd",
    )

    # Test 8: normal backup/restore still works after the validation was added.
    recreate_table()
    normal_backup = "test_validate_entry_paths_normal"
    node.query(f"BACKUP TABLE tbl_backup_traversal TO Disk('backups', '{normal_backup}')")
    node.query("DROP TABLE tbl_backup_traversal")
    node.query(f"RESTORE TABLE tbl_backup_traversal FROM Disk('backups', '{normal_backup}')")
    assert node.query("SELECT * FROM tbl_backup_traversal") == "1\thello\n"

    # Test 9: a doubled separator in <name>. The name normalizes to a path inside the backup, so
    # tests 1-7 do not catch it, and the part's file list then held the rooted remainder "/tmp/...".
    # The companion entry is what makes the case bite: RESTORE reads the file back under that key.
    recreate_table()
    double_slash_target = "/tmp/backup_double_slash_test_output.txt"
    assert not node.path_exists(double_slash_target), "target must not exist before the restore"
    inject_and_restore(
        "double_slash",
        f"data/default/tbl_backup_traversal/{part}//tmp/backup_double_slash_test_output.txt",
        "INSECURE_PATH",
        companion_names=["tmp/backup_double_slash_test_output.txt"],
    )
    assert not node.path_exists(double_slash_target), "FAIL: file written outside the part directory"

    # Test 10: as many ".." as the four components of the entry's directory prefix. The name still
    # normalizes to a path inside the backup, but the remainder escapes the part's destination
    # directory, which sits four components below the disk root.
    recreate_table()
    shallow_target = "/var/lib/clickhouse/tmp/backup_shallow_dotdot_test_output.txt"
    assert not node.path_exists(shallow_target), "target must not exist before the restore"
    inject_and_restore(
        "shallow_dotdot",
        f"data/default/tbl_backup_traversal/{part}/../../../../tmp/backup_shallow_dotdot_test_output.txt",
        "INSECURE_PATH",
    )
    assert not node.path_exists(shallow_target), "FAIL: file written outside the part directory"

    # Test 11: a table with a projection still round-trips. Projection files are the legitimate
    # entries that carry a subdirectory inside the part ("<part>/p.proj/columns.txt").
    node.query("DROP TABLE IF EXISTS tbl_backup_projection SYNC")
    node.query(
        "CREATE TABLE tbl_backup_projection (id UInt64, data String, PROJECTION p (SELECT data, count() GROUP BY data)) ENGINE = MergeTree ORDER BY id"
    )
    node.query("INSERT INTO tbl_backup_projection VALUES (1, 'a'), (2, 'b'), (3, 'a')")
    projection_backup = "test_validate_entry_paths_projection"
    node.query(f"BACKUP TABLE tbl_backup_projection TO Disk('backups', '{projection_backup}')")
    node.query("DROP TABLE tbl_backup_projection SYNC")
    node.query(f"RESTORE TABLE tbl_backup_projection FROM Disk('backups', '{projection_backup}')")
    assert node.query("SELECT data, count() FROM tbl_backup_projection GROUP BY data ORDER BY data") == "a\t2\nb\t1\n"
    assert (
        node.query(
            "SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 'tbl_backup_projection' AND active"
        )
        == "1\n"
    )

    # Clean up.
    node.query("DROP TABLE IF EXISTS tbl_backup_traversal SYNC")
    node.query("DROP TABLE IF EXISTS tbl_backup_projection SYNC")
    remove_from_backups_disk("test_validate_entry_paths_*")


def test_backup_metadata_version_overflow(started_cluster):
    # Converted from stateless test 04495_backup_metadata_version_overflow.sh.
    # A .backup manifest whose <version> fits in UInt64 but not in int must be rejected,
    # not silently narrowed past the supported-version check (e.g. 4294967298 must not
    # wrap to 2 and be accepted).
    node.query("DROP TABLE IF EXISTS tbl_ver_overflow")
    node.query("CREATE TABLE tbl_ver_overflow (id UInt64) ENGINE = MergeTree ORDER BY id")
    node.query("INSERT INTO tbl_ver_overflow VALUES (1)")

    bname = "test_backup_metadata_version_overflow_ver"
    node.query(f"BACKUP TABLE tbl_ver_overflow TO Disk('backups', '{bname}')")

    # 4294967298 = 2^32 + 2: fits in UInt64 but narrows to 2 as int, which would pass
    # the range check.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"sed -i 's|<version>[0-9]*</version>|<version>4294967298</version>|' {BACKUPS_DISK_ROOT}/{bname}/.backup",
        ],
        privileged=True,
        user="root",
    )

    node.query("DROP TABLE tbl_ver_overflow")
    err = node.query_and_get_error(f"RESTORE TABLE tbl_ver_overflow FROM Disk('backups', '{bname}')")
    assert "BACKUP_VERSION_NOT_SUPPORTED" in err

    node.query("DROP TABLE IF EXISTS tbl_ver_overflow SYNC")
    remove_from_backups_disk(bname)
