# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_local = cluster.add_instance(
    "node_local",
    main_configs=["configs/config.d/storage_configuration.xml"],
    tmpfs=[
        "/test_user_files_disk1:size=100M",
    ],
)

node_s3 = cluster.add_instance(
    "node_s3",
    main_configs=["configs/config.d/remote_storage_configuration.xml"],
    with_minio=True,
    # The restart test needs to stop and start the server.
    stay_alive=True,
)

# This node deliberately sets an unusable legacy `user_files_path` together with a
# `user_files_policy`. The fact that the cluster starts at all proves the policy takes
# precedence over the local path during startup (see the precedence test below).
node_precedence = cluster.add_instance(
    "node_precedence",
    main_configs=["configs/config.d/storage_configuration_bad_local_path.xml"],
    tmpfs=[
        "/test_user_files_disk_precedence:size=100M",
    ],
)

# This node backs `user_files_policy` with a local `DiskEncrypted`. It is not remote, so a
# guard that tested only `disk->isRemote()` would (incorrectly) accept it; the bytes at the
# backing path are ciphertext, so local-only consumers that bypass `IDisk` must fail closed.
node_encrypted = cluster.add_instance(
    "node_encrypted",
    main_configs=["configs/config.d/storage_configuration_encrypted.xml"],
    tmpfs=[
        "/test_user_files_disk_encrypted_backing:size=100M",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_local_read_file_from_disk():
    """Test that `file` function can read files from the configured disk."""
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            "echo 'hello from disk1' > /test_user_files_disk1/test1.csv",
        ]
    )

    result = node_local.query("SELECT * FROM file('test1.csv', 'CSV', 'x String')")
    assert result.strip() == "hello from disk1"


def test_local_glob_pattern():
    """Test that glob patterns match files on the configured disk."""
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            "echo '1' > /test_user_files_disk1/glob_test_a.csv",
        ]
    )
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            "echo '2' > /test_user_files_disk1/glob_test_b.csv",
        ]
    )

    result = node_local.query(
        "SELECT * FROM file('glob_test_*.csv', 'CSV', 'x UInt64') ORDER BY x"
    )
    assert result.strip() == "1\n2"


def test_local_absolute_path_on_disk():
    """Test that absolute paths under the configured disk root are accepted."""
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            "echo 'abs1' > /test_user_files_disk1/abs_test.csv",
        ]
    )

    result = node_local.query(
        "SELECT * FROM file('/test_user_files_disk1/abs_test.csv', 'CSV', 'x String')"
    )
    assert result.strip() == "abs1"


def test_local_absolute_path_outside_disks_rejected():
    """Test that absolute paths outside the policy's disks are rejected."""
    with pytest.raises(Exception, match="DATABASE_ACCESS_DENIED|not inside"):
        node_local.query(
            "SELECT * FROM file('/etc/passwd', 'CSV', 'x String')"
        )


def test_local_absolute_path_equal_to_disk_root():
    """An absolute path equal to the disk root (with or without trailing slash)
    must resolve to the disk root itself, not be rejected as an outside path."""
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            "echo 'root_match' > /test_user_files_disk1/root_match.csv",
        ]
    )

    # With trailing slash: directory listing should expand to the file inside.
    result = node_local.query(
        "SELECT * FROM file('/test_user_files_disk1/', 'CSV', 'x String') WHERE x = 'root_match'"
    )
    assert result.strip() == "root_match"

    # Without trailing slash: same behavior expected.
    result = node_local.query(
        "SELECT * FROM file('/test_user_files_disk1', 'CSV', 'x String') WHERE x = 'root_match'"
    )
    assert result.strip() == "root_match"


@pytest.mark.parametrize(
    "path",
    [
        "../../etc/passwd",
        "foo/../../../etc/passwd",
        "./../../etc/passwd",
        "/test_user_files_disk1/../../../etc/passwd",
    ],
)
def test_local_path_traversal_rejected(path):
    """Test that disk-relative '..' traversal is rejected (does not escape the disk root)."""
    with pytest.raises(Exception, match="DATABASE_ACCESS_DENIED|escapes"):
        node_local.query(
            f"SELECT * FROM file('{path}', 'CSV', 'x String')"
        )


def test_local_symlink_escape_rejected():
    """An in-root symlink pointing outside the disk root must not be followed.

    Lexical normalization alone (`..` rejection) does not catch this case: the
    symlink target is resolved by the local filesystem at I/O time, so a path
    like `escape_link/passwd` would otherwise read `/etc/passwd` even though
    every component looks valid.
    """
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            "ln -snf /etc /test_user_files_disk1/escape_link",
        ]
    )
    try:
        with pytest.raises(Exception, match="DATABASE_ACCESS_DENIED|outside"):
            node_local.query(
                "SELECT * FROM file('escape_link/passwd', 'CSV', 'x String')"
            )
        with pytest.raises(Exception, match="DATABASE_ACCESS_DENIED|outside"):
            node_local.query(
                "SELECT * FROM file('/test_user_files_disk1/escape_link/passwd', 'CSV', 'x String')"
            )
        # Glob with an in-root symlink prefix must also fail closed - otherwise
        # the iteration would call `iterateDirectory`/`getFileSize` on entries
        # outside the disk root and merely drop matches in a post-filter,
        # leaking metadata about files like `/etc/*` even when the query
        # returns no rows.
        result = node_local.query_and_get_error(
            "SELECT * FROM file('escape_link/*', 'CSV', 'x String')"
        )
        assert "DATABASE_ACCESS_DENIED" in result or "FILE_DOESNT_EXIST" in result
    finally:
        node_local.exec_in_container(
            ["bash", "-c", "rm -f /test_user_files_disk1/escape_link"]
        )


def test_local_scalar_file_function():
    """The scalar `file(path)` function has its own `user_files_policy` branch in
    `FunctionFile`, separate from the `file` table function / `StorageFile`. Cover
    a success case, the symlink-escape rejection, and the optional-default fallback
    so a regression in that branch is caught."""
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            "echo -n 'scalar contents' > /test_user_files_disk1/scalar_test.txt",
        ]
    )

    # Success: relative path resolves through the configured disk.
    assert node_local.query("SELECT file('scalar_test.txt')").strip() == "scalar contents"

    # Absolute path equal to the disk-rooted file resolves the same way.
    assert (
        node_local.query("SELECT file('/test_user_files_disk1/scalar_test.txt')").strip()
        == "scalar contents"
    )

    # `..` traversal is rejected lexically.
    with pytest.raises(Exception, match="DATABASE_ACCESS_DENIED|escapes"):
        node_local.query("SELECT file('../../etc/passwd')")

    # In-root symlink pointing outside the disk root is blocked by the symlink-aware check.
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            "ln -snf /etc /test_user_files_disk1/scalar_escape_link",
        ]
    )
    try:
        with pytest.raises(Exception, match="DATABASE_ACCESS_DENIED|outside"):
            node_local.query("SELECT file('scalar_escape_link/passwd')")
    finally:
        node_local.exec_in_container(
            ["bash", "-c", "rm -f /test_user_files_disk1/scalar_escape_link"]
        )

    # Optional default is returned on failure instead of throwing.
    result = node_local.query("SELECT file('nonexistent_scalar.txt', 'fallback')")
    assert result.strip() == "fallback"


def test_local_insert_into_file():
    """Test that writing files uses the first disk by default."""
    node_local.query(
        "INSERT INTO FUNCTION file('write_test.csv', 'CSV', 'x UInt64') SELECT 42"
    )

    result = node_local.query(
        "SELECT * FROM file('write_test.csv', 'CSV', 'x UInt64')"
    )
    assert result.strip() == "42"

    # Verify the file was written to the first disk
    output = node_local.exec_in_container(
        ["bash", "-c", "cat /test_user_files_disk1/write_test.csv"]
    )
    assert "42" in output


def test_s3_write_and_read():
    """Test that file() function can write to and read from an S3-backed disk."""
    node_s3.query(
        "INSERT INTO FUNCTION file('s3_test.csv', 'CSV', 'x UInt64') SELECT 123"
    )

    result = node_s3.query("SELECT * FROM file('s3_test.csv', 'CSV', 'x UInt64')")
    assert result.strip() == "123"


def test_s3_write_and_read_multiple_rows():
    """Test writing and reading multiple rows via S3-backed disk."""
    node_s3.query(
        "INSERT INTO FUNCTION file('s3_multi.csv', 'CSV', 'x UInt64, y String') "
        "SELECT number, toString(number) FROM numbers(10)"
    )

    result = node_s3.query(
        "SELECT count() FROM file('s3_multi.csv', 'CSV', 'x UInt64, y String')"
    )
    assert result.strip() == "10"


def test_s3_read_nonexistent_file():
    """Test that reading a non-existent file from S3 produces an error."""
    with pytest.raises(Exception, match="FILE_DOESNT_EXIST|Cannot stat|doesn't exist"):
        node_s3.query(
            "SELECT * FROM file('nonexistent_file_12345.csv', 'CSV', 'x UInt64')"
        )


def test_s3_glob_pattern():
    """Test glob patterns on S3-backed disk."""
    node_s3.query(
        "INSERT INTO FUNCTION file('s3_glob_a.csv', 'CSV', 'x UInt64') SELECT 1"
    )
    node_s3.query(
        "INSERT INTO FUNCTION file('s3_glob_b.csv', 'CSV', 'x UInt64') SELECT 2"
    )

    result = node_s3.query(
        "SELECT * FROM file('s3_glob_*.csv', 'CSV', 'x UInt64') ORDER BY x"
    )
    assert result.strip() == "1\n2"


def test_local_time_virtual_column():
    """Test that _time virtual column returns a real timestamp for disk-backed files."""
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            "echo 'mtime_test' > /test_user_files_disk1/mtime_test.csv",
        ]
    )

    result = node_local.query(
        "SELECT _time > toDateTime('2000-01-01') "
        "FROM file('mtime_test.csv', 'CSV', 'x String')"
    )
    assert result.strip() == "1"


def test_s3_time_virtual_column():
    """Same as above but for S3-backed disk (covers the DiskObjectStorage path)."""
    node_s3.query(
        "INSERT INTO FUNCTION file('s3_mtime.csv', 'CSV', 'x UInt64') SELECT 7"
    )
    result = node_s3.query(
        "SELECT _time > toDateTime('2000-01-01') "
        "FROM file('s3_mtime.csv', 'CSV', 'x UInt64')"
    )
    assert result.strip() == "1"


def test_local_insert_appends_to_existing_file():
    """Append-capable formats (CSV/TSV) must not silently overwrite an existing file."""
    node_local.query(
        "INSERT INTO FUNCTION file('append_test.csv', 'CSV', 'x UInt64') SELECT 1"
    )
    node_local.query(
        "INSERT INTO FUNCTION file('append_test.csv', 'CSV', 'x UInt64') SELECT 2"
    )

    result = node_local.query(
        "SELECT count() FROM file('append_test.csv', 'CSV', 'x UInt64')"
    )
    assert result.strip() == "2"


def test_local_insert_truncate_on_insert():
    """`engine_file_truncate_on_insert=1` must overwrite existing data."""
    node_local.query(
        "INSERT INTO FUNCTION file('truncate_test.csv', 'CSV', 'x UInt64') SELECT 1"
    )
    node_local.query(
        "INSERT INTO FUNCTION file('truncate_test.csv', 'CSV', 'x UInt64') SELECT 2 "
        "SETTINGS engine_file_truncate_on_insert = 1"
    )

    result = node_local.query(
        "SELECT * FROM file('truncate_test.csv', 'CSV', 'x UInt64')"
    )
    assert result.strip() == "2"


def test_local_schema_inference_from_disk():
    """Schema inference must read through `IDisk` so it works for non-local backends.

    Without disk-aware schema inference, `ReadBufferFromFileIterator` falls back to
    `stat`/`ReadBufferFromFile` on the absolute disk path, which fails for object
    storage. The local case still exercises the disk-aware code path because
    `user_files_policy` is configured.
    """
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            "echo -e 'a,b\\n1,foo\\n2,bar' > /test_user_files_disk1/infer.csv",
        ]
    )

    result = node_local.query(
        "SELECT count() FROM file('infer.csv', 'CSVWithNames')"
    )
    assert result.strip() == "2"


def test_s3_schema_inference_from_disk():
    """Same as above for the S3-backed disk: schema inference must go through `IDisk`."""
    node_s3.query(
        "INSERT INTO FUNCTION file('s3_infer.csv', 'CSVWithNames', 'a UInt64, b String') "
        "SELECT number, toString(number) FROM numbers(3)"
    )
    result = node_s3.query(
        "SELECT count() FROM file('s3_infer.csv', 'CSVWithNames')"
    )
    assert result.strip() == "3"


def test_local_partitioned_insert_rejected():
    """`INSERT ... PARTITION BY` must be rejected with a clear error under user_files_policy
    until disk-aware partitioned writes are implemented (silent fall-through to the local
    filesystem would bypass the configured backend)."""
    with pytest.raises(Exception, match="NOT_IMPLEMENTED|not supported with user_files_policy"):
        node_local.query(
            "INSERT INTO FUNCTION file('part_{_partition_id}.csv', 'CSV', 'x UInt64') "
            "PARTITION BY x SELECT number FROM numbers(3)"
        )


def test_local_partitioned_insert_rejected_when_glob_target_exists():
    """The partitioned-write rejection must not be bypassable by a pre-existing file.

    Regression for `getPathsListOnDisk`: it expanded `{_partition_id}` as an ordinary
    `{...}` enum glob (to the literal `_partition_id`) before the write-side rejection saw
    it. If a file matching that expansion (`part__partition_id.csv`) already existed on the
    disk, the pattern resolved to that single literal file, so `path_for_partitioned_write`
    no longer contained the wildcard, `INSERT ... PARTITION BY` was no longer recognized as a
    partitioned write, and all data was silently written into the wrong file instead of
    throwing the intended unsupported-mode exception."""
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            # The literal name that `{_partition_id}` expands to.
            "echo 'preexisting' > /test_user_files_disk1/part__partition_id.csv",
        ]
    )

    with pytest.raises(Exception, match="NOT_IMPLEMENTED|not supported with user_files_policy"):
        node_local.query(
            "INSERT INTO FUNCTION file('part_{_partition_id}.csv', 'CSV', 'x UInt64') "
            "PARTITION BY x SELECT number FROM numbers(3)"
        )

    # The pre-existing file must be untouched: with the bug it would have been overwritten
    # with the inserted rows (0, 1, 2) instead of the query being rejected.
    output = node_local.exec_in_container(
        ["bash", "-c", "cat /test_user_files_disk1/part__partition_id.csv"]
    )
    assert output.strip() == "preexisting", output


def test_local_truncate_table_routes_through_disk():
    """`TRUNCATE TABLE` for `File` engine must route through `IDisk` so it actually
    empties the underlying file. Without disk-aware truncation, `fs::exists`/`::truncate`
    on the absolute disk path would silently no-op for non-local backends (and even for
    local disks, this exercises the disk-routing path used for object storage)."""
    node_local.query("DROP TABLE IF EXISTS truncate_routed")
    node_local.query(
        "CREATE TABLE truncate_routed (x UInt64) ENGINE = File(CSV, 'truncate_routed.csv')"
    )
    node_local.query("INSERT INTO truncate_routed VALUES (1), (2), (3)")
    assert node_local.query("SELECT count() FROM truncate_routed").strip() == "3"

    node_local.query("TRUNCATE TABLE truncate_routed")
    assert node_local.query("SELECT count() FROM truncate_routed").strip() == "0"
    node_local.query("DROP TABLE truncate_routed")


def test_s3_truncate_table_routes_through_disk():
    """Same as above for the S3-backed disk: `TRUNCATE TABLE` must remove the
    underlying object instead of relying on local `fs::exists`/`::truncate`,
    which would silently no-op against an object-storage backend."""
    node_s3.query("DROP TABLE IF EXISTS s3_truncate_routed")
    node_s3.query(
        "CREATE TABLE s3_truncate_routed (x UInt64) ENGINE = File(CSV, 's3_truncate_routed.csv')"
    )
    node_s3.query("INSERT INTO s3_truncate_routed VALUES (10), (20)")
    assert node_s3.query("SELECT count() FROM s3_truncate_routed").strip() == "2"

    node_s3.query("TRUNCATE TABLE s3_truncate_routed")
    assert node_s3.query("SELECT count() FROM s3_truncate_routed").strip() == "0"
    node_s3.query("DROP TABLE s3_truncate_routed")


def test_local_filesystem_database():
    """The `Filesystem` database engine must resolve a relative path against the
    configured `user_files_policy` disk root and read tables through `IDisk`."""
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            "mkdir -p /test_user_files_disk1/fsdb && echo '42' > /test_user_files_disk1/fsdb/t.csv",
        ]
    )
    node_local.query("DROP DATABASE IF EXISTS test_fs_db")
    node_local.query("CREATE DATABASE test_fs_db ENGINE = Filesystem('fsdb')")
    assert node_local.query("SELECT * FROM test_fs_db.`t.csv`").strip() == "42"
    node_local.query("DROP DATABASE test_fs_db")


def test_s3_filesystem_database():
    """Same for the S3-backed disk: a relative `Filesystem` database path must keep
    the disk-root prefix so `splitUserFilesAbsolutePath` matches, instead of being
    mangled into a host-absolute path by `fs::absolute` (which would make valid
    directories on the disk look missing)."""
    node_s3.query(
        "INSERT INTO FUNCTION file('fsdb_s3/t.csv', 'CSV', 'x UInt64') SELECT 42"
    )
    node_s3.query("DROP DATABASE IF EXISTS test_fs_db_s3")
    node_s3.query("CREATE DATABASE test_fs_db_s3 ENGINE = Filesystem('fsdb_s3')")
    assert node_s3.query("SELECT * FROM test_fs_db_s3.`t.csv`").strip() == "42"
    node_s3.query("DROP DATABASE test_fs_db_s3")


def test_s3_filesystem_database_survives_restart():
    """`getCreateDatabaseQueryImpl` serializes the normalized (disk-root-prefixed) path
    into metadata. For an object-storage disk the root is itself relative, so on reload
    the path is relative again; prefixing it a second time would produce
    `<disk_root>/<disk_root>/...` and the database would fail to load. The constructor
    must keep normalization idempotent so the database still loads after a restart."""
    node_s3.query(
        "INSERT INTO FUNCTION file('fsdb_s3_restart/t.csv', 'CSV', 'x UInt64') SELECT 42"
    )
    node_s3.query("DROP DATABASE IF EXISTS test_fs_db_s3_restart")
    node_s3.query(
        "CREATE DATABASE test_fs_db_s3_restart ENGINE = Filesystem('fsdb_s3_restart')"
    )
    assert (
        node_s3.query("SELECT * FROM test_fs_db_s3_restart.`t.csv`").strip() == "42"
    )

    node_s3.restart_clickhouse()

    assert (
        node_s3.query("SELECT * FROM test_fs_db_s3_restart.`t.csv`").strip() == "42"
    )
    node_s3.query("DROP DATABASE test_fs_db_s3_restart")


def test_s3_relative_path_starting_with_disk_root_prefix():
    """A relative `file()` path that begins with the s3_plain disk's own key prefix
    (as reported by `system.disks`) must be treated as a genuine relative path under
    the disk root - not mistaken for an already-disk-qualified path and silently
    stripped down to a different object at the disk root.

    Regression for the path-resolution ambiguity in `getPathsListOnDisk`: for an
    object-storage disk `disk->getPath()` is itself a relative object-key prefix, so a
    user path like `<prefix>/nested.csv` is a valid relative object name. Previously
    the leading prefix was stripped, so a write to `<prefix>/nested.csv` and a read of
    the short `nested.csv` would hit the *same* object, which could silently read the
    wrong object or overwrite unrelated data."""
    disk_path = node_s3.query(
        "SELECT path FROM system.disks WHERE name = 'disk_s3_plain'"
    ).strip()
    # The bug premise only holds when the disk root is a relative key prefix.
    assert disk_path, "disk_s3_plain has no path"
    assert not disk_path.startswith("/"), f"unexpected absolute disk path: {disk_path}"

    long_rel = disk_path.rstrip("/") + "/prefix_collision.csv"

    # Write a row through the long relative path (which begins with the disk prefix).
    node_s3.query(
        f"INSERT INTO FUNCTION file('{long_rel}', 'CSV', 'x UInt64') SELECT 777"
    )

    # The long relative path must round-trip to the same row.
    assert (
        node_s3.query(f"SELECT * FROM file('{long_rel}', 'CSV', 'x UInt64')").strip()
        == "777"
    )

    # The short path is a *different* object at the disk root and must not exist.
    # With the bug, the long path was stripped to the short one, so this read would
    # have returned 777 instead of failing.
    short_read = node_s3.query_and_get_error(
        "SELECT * FROM file('prefix_collision.csv', 'CSV', 'x UInt64')"
    )
    assert (
        "FILE_DOESNT_EXIST" in short_read
        or "Cannot stat" in short_read
        or "doesn't exist" in short_read
    ), short_read


def test_local_disk_glob_recursion_depth_guarded():
    """A deeply nested directory tree under a `user_files_policy` disk must surface
    `TOO_DEEP_RECURSION` from the disk-backed `**` glob walker instead of exhausting the
    stack. The disk walker must enforce the same `MAX_LIST_FILES_RECURSION_DEPTH` bound
    as the local-filesystem walker `listFilesWithRegexpMatchingImpl`."""
    # Build a directory chain deeper than MAX_LIST_FILES_RECURSION_DEPTH (1000). The
    # physical (single-slash) path stays well under PATH_MAX; the walker throws at depth
    # 1001 before the path it constructs internally can grow that large.
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            'd=/test_user_files_disk1/deep_glob; '
            'for i in $(seq 1 1100); do d="$d/d"; done; '
            'mkdir -p "$d"',
        ]
    )
    err = node_local.query_and_get_error(
        "SELECT count() FROM file('deep_glob/**', 'CSV', 'x UInt64')"
    )
    assert "TOO_DEEP_RECURSION" in err, err


def test_s3_truncate_then_insert():
    """`INSERT` after `TRUNCATE TABLE` must work on an S3-backed `File` table.

    `TRUNCATE` leaves a zero-byte object behind (it rewrites an empty file to mirror
    local `::truncate` semantics). A subsequent `INSERT` must rewrite that zero-byte
    object rather than switch to `WriteMode::Append` (a zero-byte object has no format
    prefix to preserve), because `s3_plain` rejects append and the table would otherwise
    become un-insertable after truncation."""
    node_s3.query("DROP TABLE IF EXISTS s3_truncate_insert")
    node_s3.query(
        "CREATE TABLE s3_truncate_insert (x UInt64) ENGINE = File(CSV, 's3_truncate_insert.csv')"
    )
    node_s3.query("INSERT INTO s3_truncate_insert VALUES (1), (2)")
    assert node_s3.query("SELECT count() FROM s3_truncate_insert").strip() == "2"

    node_s3.query("TRUNCATE TABLE s3_truncate_insert")
    assert node_s3.query("SELECT count() FROM s3_truncate_insert").strip() == "0"

    # This INSERT previously failed on `s3_plain`: the zero-byte object left by TRUNCATE
    # made the sink choose `WriteMode::Append`, which `s3_plain` does not support.
    node_s3.query("INSERT INTO s3_truncate_insert VALUES (99)")
    assert node_s3.query("SELECT * FROM s3_truncate_insert").strip() == "99"
    node_s3.query("DROP TABLE s3_truncate_insert")


def test_s3_glob_wildcard_directory_component():
    """A glob with a wildcard *directory* component (e.g. `dir*/data.csv`) must match
    nested objects on an S3-backed `user_files_policy` disk.

    Regression for the disk-glob walker join: after a directory-component glob matches
    (say `wglob_a`), the walker recursed with the parent passed as `wglob_a/` and the
    remaining suffix `/data.csv`, joining them as `wglob_a//data.csv`. POSIX local disks
    collapse the `//`, but object-storage disks treat `wglob_a//data.csv` as a key
    distinct from `wglob_a/data.csv`, so the matching objects were silently missed and
    the query returned no rows."""
    node_s3.query(
        "INSERT INTO FUNCTION file('wglob_a/data.csv', 'CSV', 'x UInt64') SELECT 1"
    )
    node_s3.query(
        "INSERT INTO FUNCTION file('wglob_b/data.csv', 'CSV', 'x UInt64') SELECT 2"
    )

    result = node_s3.query(
        "SELECT * FROM file('wglob_*/data.csv', 'CSV', 'x UInt64') ORDER BY x"
    )
    assert result.strip() == "1\n2", result


def test_s3_recursive_glob_nested_object():
    """A `**` recursive glob must descend into nested directories on an S3-backed disk.

    This covers the recursive branch of the disk-glob walker, which used the same
    parent-directory join that produced a `dir//suffix` double slash. With the bug, the
    file directly under the glob root (`gstar_root/top.csv`) was still found, but the
    object one level deeper (`gstar_root/sub/deep.csv`) was reached only via a recursive
    descent whose prefix became `gstar_root/sub//`, so it was silently missed on object
    storage."""
    node_s3.query(
        "INSERT INTO FUNCTION file('gstar_root/top.csv', 'CSV', 'x UInt64') SELECT 10"
    )
    node_s3.query(
        "INSERT INTO FUNCTION file('gstar_root/sub/deep.csv', 'CSV', 'x UInt64') SELECT 20"
    )

    # Both the top-level and the nested object must be matched; the nested one only
    # surfaces if the recursive descent joins paths with a single separator.
    result = node_s3.query(
        "SELECT sum(x) FROM file('gstar_root/**', 'CSV', 'x UInt64')"
    )
    assert result.strip() == "30", result


def test_s3_recursive_glob_middle_literal_suffix():
    """A `**` between literal components (`dir/**/data.csv`) must match the same set of
    objects on an S3-backed `user_files_policy` disk as it does on the local
    `user_files_path` walker: `**/` stands for *zero or more* directory levels.

    Regression for two divergences of `listFilesWithRegexpMatchingOnDisk` from
    `listFilesWithRegexpMatchingImpl`: it lacked the zero-directory branch (so the
    globstar-adjacent file `gmid_root/data.csv` was skipped) and it dropped the `**`
    when descending (so only the *single* level `gmid_root/one/data.csv` matched, while
    deeper `gmid_root/one/two/data.csv` was missed). With the fix all three depths match
    and the non-matching sibling `gmid_root/one/other.csv` is excluded."""
    node_s3.query(
        "INSERT INTO FUNCTION file('gmid_root/data.csv', 'CSV', 'x UInt64') SELECT 1"
    )
    node_s3.query(
        "INSERT INTO FUNCTION file('gmid_root/one/data.csv', 'CSV', 'x UInt64') SELECT 2"
    )
    node_s3.query(
        "INSERT INTO FUNCTION file('gmid_root/one/two/data.csv', 'CSV', 'x UInt64') SELECT 4"
    )
    # A file whose name does not match the literal suffix must be excluded.
    node_s3.query(
        "INSERT INTO FUNCTION file('gmid_root/one/other.csv', 'CSV', 'x UInt64') SELECT 8"
    )

    result = node_s3.query(
        "SELECT x FROM file('gmid_root/**/data.csv', 'CSV', 'x UInt64') ORDER BY x"
    )
    assert result.strip() == "1\n2\n4", result


def test_s3_recursive_glob_adjacent_globstars_no_duplicates():
    """Adjacent globstars (`**/**/data.csv`) can reach the same object through both the
    zero-level branch and the recursive descent, so the disk walker must deduplicate its
    matches - otherwise a single object is returned multiple times and its bytes are
    counted more than once in the read progress.

    `gadj_root/data.csv` is reachable with both globstars empty, and
    `gadj_root/one/data.csv` is reachable two ways (`one`+empty and empty+`one`); each
    must appear exactly once."""
    node_s3.query(
        "INSERT INTO FUNCTION file('gadj_root/data.csv', 'CSV', 'x UInt64') SELECT 100"
    )
    node_s3.query(
        "INSERT INTO FUNCTION file('gadj_root/one/data.csv', 'CSV', 'x UInt64') SELECT 200"
    )

    # Each matching object must be counted once: two rows, summing to 300 (not 500+,
    # which is what the un-deduplicated walker would return for the twice-reachable file).
    result = node_s3.query(
        "SELECT count(), sum(x) FROM file('gadj_root/**/**/data.csv', 'CSV', 'x UInt64')"
    )
    assert result.strip() == "2\t300", result


def test_local_recursive_glob_middle_literal_suffix():
    """Parity check on a local `user_files_policy` disk: `dir/**/data.csv` must match the
    globstar-adjacent file and every deeper level, exactly like the plain
    `user_files_path` walker. Locks `listFilesWithRegexpMatchingOnDisk` to the semantics
    of `listFilesWithRegexpMatchingImpl` for the disk-backed local case too."""
    node_local.query(
        "INSERT INTO FUNCTION file('lmid_root/data.csv', 'CSV', 'x UInt64') SELECT 1"
    )
    node_local.query(
        "INSERT INTO FUNCTION file('lmid_root/one/data.csv', 'CSV', 'x UInt64') SELECT 2"
    )
    node_local.query(
        "INSERT INTO FUNCTION file('lmid_root/one/two/data.csv', 'CSV', 'x UInt64') SELECT 4"
    )

    result = node_local.query(
        "SELECT x FROM file('lmid_root/**/data.csv', 'CSV', 'x UInt64') ORDER BY x"
    )
    assert result.strip() == "1\n2\n4", result


def test_user_files_policy_precedence_over_bad_local_path():
    """`user_files_policy` must take precedence over `user_files_path` during startup.

    `node_precedence` sets `user_files_path` to `/dev/null/user_files/` - a path under a
    character device, so creating the legacy local directory would fail. The server must
    still start (the policy's disk provides the user files location) and `file()` must
    resolve against the policy disk, not the bogus local path. Without skipping the local
    `user_files_path` setup when a policy is configured, `fs::create_directories` throws
    during startup and the server never comes up."""
    # The instance being up at all already proves startup did not fail on the unusable
    # local path; this confirms the server is actually serving.
    assert node_precedence.query("SELECT 1").strip() == "1"

    node_precedence.query(
        "INSERT INTO FUNCTION file('precedence_test.csv', 'CSV', 'x UInt64') SELECT 555"
    )
    assert (
        node_precedence.query(
            "SELECT * FROM file('precedence_test.csv', 'CSV', 'x UInt64')"
        ).strip()
        == "555"
    )

    # The file must land on the policy disk, not under the bogus `/dev/null/...` path.
    output = node_precedence.exec_in_container(
        ["bash", "-c", "cat /test_user_files_disk_precedence/precedence_test.csv"]
    )
    assert "555" in output


def test_encrypted_disk_read_through_idisk_and_reject_local_consumers():
    """A local `DiskEncrypted` `user_files_policy` is not remote, but its backing path holds
    ciphertext. The disk-aware `file` path must read/write the logical (decrypted) contents
    through `IDisk`, while local-only consumers that bypass `IDisk` (here the `filesystem`
    table function) must fail closed instead of exposing the ciphertext / backing files.

    Regression for the fail-open guard that tested only `disk->isRemote()`: a local encrypted
    disk passed that test, so `filesystem()` would list and read the encrypted backing
    directory via `std::filesystem` / `ReadBufferFromFile`."""
    secret = "secret_plaintext_value"

    # `file()` is disk-aware: write and read round-trip to the logical (decrypted) value.
    node_encrypted.query(
        f"INSERT INTO FUNCTION file('enc_test.csv', 'CSV', 'x String') SELECT '{secret}'"
    )
    assert (
        node_encrypted.query(
            "SELECT * FROM file('enc_test.csv', 'CSV', 'x String')"
        ).strip()
        == secret
    )

    # The bytes physically stored on the backing disk must be ciphertext: the plaintext must
    # not appear anywhere under the backing directory. This is exactly why local-POSIX
    # consumers cannot be allowed to read the disk root directly.
    backing = node_encrypted.exec_in_container(
        [
            "bash",
            "-c",
            "grep -rl '" + secret + "' /test_user_files_disk_encrypted_backing/ || true",
        ]
    )
    assert backing.strip() == "", f"plaintext leaked to backing store: {backing}"

    # A local-only consumer must reject the encrypted disk (fail closed), not read ciphertext.
    err = node_encrypted.query_and_get_error("SELECT count() FROM filesystem()")
    assert "not a plain local filesystem disk" in err, err


def test_local_literal_partition_id_filename():
    """A literal file name that merely contains the `{_partition_id}` substring, read without
    `INSERT ... PARTITION BY`, must resolve to a real disk-qualified path.

    Regression for the `{_partition_id}` fast path in `getPathsListOnDisk`: it returned the raw
    user input instead of the internal `<disk_path>/<relative>` form, so on the read path
    `splitUserFilesAbsolutePath` could not recover a disk and the file was reported missing
    (`FILE_DOESNT_EXIST`), unlike the local `user_files_path` implementation which pushes the
    resolved absolute path."""
    # The single-quotes keep the literal `{_partition_id}` in the file name (bash does not
    # brace-expand a single-element `{...}`, but quoting makes the intent explicit).
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            "echo 'literal_part' > '/test_user_files_disk1/lit_{_partition_id}.csv'",
        ]
    )

    result = node_local.query(
        "SELECT * FROM file('lit_{_partition_id}.csv', 'CSV', 'x String')"
    )
    assert result.strip() == "literal_part", result


def test_s3_non_glob_directory_trailing_slash():
    """A non-glob directory path with a trailing slash (`file('dir/', ...)`) must read the same
    objects as without the slash (`file('dir', ...)`) on an S3-backed disk.

    Regression for the directory expansion in `getPathsListOnDisk`: `normalizeDiskRelativePath`
    preserved a trailing slash, so `relative_pattern` was `dir/` and the per-entry join produced
    `dir//name`. POSIX local disks collapse the double slash, but `s3_plain` object keys are exact,
    so `file('dir/', ...)` silently missed objects that `file('dir', ...)` finds."""
    node_s3.query(
        "INSERT INTO FUNCTION file('trail_dir/a.csv', 'CSV', 'x UInt64') SELECT 1"
    )
    node_s3.query(
        "INSERT INTO FUNCTION file('trail_dir/b.csv', 'CSV', 'x UInt64') SELECT 2"
    )

    without_slash = node_s3.query(
        "SELECT * FROM file('trail_dir', 'CSV', 'x UInt64') ORDER BY x"
    ).strip()
    with_slash = node_s3.query(
        "SELECT * FROM file('trail_dir/', 'CSV', 'x UInt64') ORDER BY x"
    ).strip()

    assert without_slash == "1\n2", without_slash
    assert with_slash == without_slash, with_slash


def test_encrypted_disk_rejects_embedded_rocksdb():
    """`EmbeddedRocksDB` with an explicit `rocksdb_dir` opens RocksDB via local POSIX APIs
    (`fs::create_directories` / `rocksdb::DB::Open`) under `user_files_path`, so it must reject a
    `user_files_policy` disk that is not plain local. A local `DiskEncrypted` is not remote, so a
    guard testing only `disk->isRemote()` would accept it and operate on the ciphertext backing
    files; the guard must use `isPlainLocalDisk` instead.

    A plain local `user_files_policy` disk (here `node_local`) must still be accepted."""
    node_encrypted.query("DROP TABLE IF EXISTS rocksdb_enc")
    err = node_encrypted.query_and_get_error(
        "CREATE TABLE rocksdb_enc (k UInt64, v String) "
        "ENGINE = EmbeddedRocksDB(0, 'rocksdb_enc_dir') PRIMARY KEY(k)"
    )
    assert "not a plain local filesystem disk" in err, err

    # The plain local policy disk must not be over-rejected: the same statement succeeds there.
    node_local.query("DROP TABLE IF EXISTS rocksdb_plain_local")
    node_local.query(
        "CREATE TABLE rocksdb_plain_local (k UInt64, v String) "
        "ENGINE = EmbeddedRocksDB(0, 'rocksdb_plain_local_dir') PRIMARY KEY(k)"
    )
    node_local.query("INSERT INTO rocksdb_plain_local VALUES (1, 'a')")
    assert node_local.query("SELECT v FROM rocksdb_plain_local WHERE k = 1").strip() == "a"
    node_local.query("DROP TABLE rocksdb_plain_local")
