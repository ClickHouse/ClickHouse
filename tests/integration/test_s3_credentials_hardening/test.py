import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key

# The server is given S3 credentials by server-managed mechanisms that user SQL must not be able to reuse
# when `s3_allow_server_credentials_in_user_queries` is disabled (the default). Here we exercise the AWS
# shared-credentials file (the ProfileConfigFileAWSCredentialsProvider): it points at the working minio
# credentials, and AWS_EC2_METADATA_DISABLED plus the absence of AWS_ACCESS_KEY_ID make it the only
# credential source, so the test isolates that provider specifically.
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_minio=True,
    env_variables={
        "AWS_SHARED_CREDENTIALS_FILE": "/tmp/aws_credentials",
        "AWS_EC2_METADATA_DISABLED": "true",
    },
)

ALLOW = "SETTINGS s3_allow_server_credentials_in_user_queries = 1"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        # Stand in for the server's own AWS profile credentials.
        node.exec_in_container(
            [
                "bash",
                "-c",
                "printf '[default]\\naws_access_key_id = %s\\naws_secret_access_key = %s\\n' > /tmp/aws_credentials"
                % (minio_access_key, minio_secret_key),
            ]
        )
        yield
    finally:
        cluster.shutdown()


def test_profile_file_not_used_for_user_queries():
    # No explicit keys, no env/IMDS credentials: the only possible source is the AWS shared-credentials
    # file (ProfileConfigFileAWSCredentialsProvider). It must not be used by default.
    url = "http://minio1:9001/root/profilecreds/data.tsv"
    insert = (
        f"INSERT INTO FUNCTION s3('{url}', format = 'TSV', structure = 'x UInt8') "
        f"SELECT 1 SETTINGS s3_truncate_on_insert = 1"
    )

    error = node.query_and_get_error(insert)
    assert "ACCESS_DENIED" in error, error

    # With the setting enabled, the profile file is used and the request succeeds (proving the profile
    # file really was the only available credential source).
    node.query(insert + ", s3_allow_server_credentials_in_user_queries = 1")
    assert (
        node.query(
            f"SELECT * FROM s3('{url}', format = 'TSV', structure = 'x UInt8') {ALLOW}"
        ).strip()
        == "1"
    )


def test_server_data_disk_unaffected():
    # The server's own S3-backed operations are not user queries and keep working regardless.
    node.query("DROP TABLE IF EXISTS t_local SYNC")
    node.query("CREATE TABLE t_local (x UInt8) ENGINE = MergeTree ORDER BY tuple()")
    node.query("INSERT INTO t_local SELECT 4")
    assert node.query("SELECT sum(x) FROM t_local").strip() == "4"
    node.query("DROP TABLE t_local SYNC")
