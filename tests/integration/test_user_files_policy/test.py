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
