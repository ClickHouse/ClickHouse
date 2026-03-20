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
        "/test_user_files_disk2:size=100M",
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


def test_local_read_file_from_first_disk():
    """Test that file() function can read files from the first disk in the volume."""
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            "echo 'hello from disk1' > /test_user_files_disk1/test1.csv",
        ]
    )

    result = node_local.query("SELECT * FROM file('test1.csv', 'CSV', 'x String')")
    assert result.strip() == "hello from disk1"


def test_local_read_file_from_second_disk():
    """Test that file() function can read files from the second disk in the volume."""
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            "echo 'hello from disk2' > /test_user_files_disk2/test2.csv",
        ]
    )

    result = node_local.query("SELECT * FROM file('test2.csv', 'CSV', 'x String')")
    assert result.strip() == "hello from disk2"


def test_local_read_files_from_both_disks():
    """Test that files from both disks are accessible using globs."""
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
            "echo '2' > /test_user_files_disk2/glob_test_b.csv",
        ]
    )

    result = node_local.query(
        "SELECT * FROM file('glob_test_*.csv', 'CSV', 'x UInt64') ORDER BY x"
    )
    assert result.strip() == "1\n2"


def test_local_absolute_path_on_first_disk():
    """Test that absolute paths on the first disk are accepted."""
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


def test_local_absolute_path_on_second_disk():
    """Test that absolute paths on the second disk are accepted."""
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            "echo 'abs2' > /test_user_files_disk2/abs_test.csv",
        ]
    )

    result = node_local.query(
        "SELECT * FROM file('/test_user_files_disk2/abs_test.csv', 'CSV', 'x String')"
    )
    assert result.strip() == "abs2"


def test_local_absolute_path_outside_disks_rejected():
    """Test that absolute paths outside the policy's disks are rejected."""
    with pytest.raises(Exception, match="DATABASE_ACCESS_DENIED|not inside"):
        node_local.query(
            "SELECT * FROM file('/etc/passwd', 'CSV', 'x String')"
        )


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
