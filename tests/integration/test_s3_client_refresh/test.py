import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_minio=True,
    main_configs=["configs/s3.xml"],
    env_variables={
        "AWS_ACCESS_KEY_ID": minio_access_key,
        "AWS_SECRET_ACCESS_KEY": minio_secret_key,
        "AWS_EC2_METADATA_DISABLED": "true",
    },
)

TRUSTED = {"s3_allow_server_credentials_in_user_queries": 1}
RESTRICTED = {"s3_allow_server_credentials_in_user_queries": 0}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("scope", ["global", "endpoint"])
@pytest.mark.parametrize("partitioned", [False, True])
def test_named_collection_credentials_survive_restricted_read(scope, partitioned):
    # Refresh must preserve the collection's ambient-credential opt-in over server defaults,
    # while still refusing restricted reads. Partitioned tables exercise the system-log write path.
    table = f"refresh_{scope}_{int(partitioned)}"
    filename = f"{scope}/{table}/data"
    if partitioned:
        filename += "_{_partition_id}"
    filename += ".tsv"
    partition_by = "PARTITION BY x" if partitioned else ""
    node.query(
        f"CREATE TABLE {table} (x UInt8) "
        f"ENGINE = S3(refresh_env, filename = '{filename}') {partition_by}",
        settings=TRUSTED,
    )
    try:
        for value in (1, 2):
            node.query(
                f"INSERT INTO {table} VALUES ({value})",
                settings={**TRUSTED, "s3_truncate_on_insert": 1},
            )
            # Read back with explicit keys so verification cannot refresh the table's client.
            object_name = filename.replace("{_partition_id}", str(value))
            assert (
                node.query(
                    f"SELECT * FROM s3('http://minio1:9001/root/refresh/{object_name}', "
                    f"'{minio_access_key}', '{minio_secret_key}', 'TSV', 'x UInt8')",
                    settings=RESTRICTED,
                )
                == f"{value}\n"
            )

            error = node.query_and_get_error(
                f"SELECT * FROM {table}", settings=RESTRICTED
            )
            if partitioned:
                assert "ACCESS_DENIED" in error or "NOT_IMPLEMENTED" in error, error
            else:
                assert "ACCESS_DENIED" in error, error
    finally:
        node.query(f"DROP TABLE {table} SYNC")


@pytest.mark.parametrize("named_collection", [False, True])
def test_explicit_keys_survive_client_refresh(named_collection):
    # Static keys must also survive a session change, even with conflicting endpoint credentials.
    table = f"refresh_keys_{int(named_collection)}"
    url = f"http://minio1:9001/root/refresh/keys/{table}.tsv"
    if named_collection:
        arguments = (
            f"refresh_env, url = '{url}', access_key_id = '{minio_access_key}', "
            f"secret_access_key = '{minio_secret_key}'"
        )
    else:
        arguments = f"'{url}', '{minio_access_key}', '{minio_secret_key}', 'TSV'"
    node.query(
        f"CREATE TABLE {table} (x UInt8) ENGINE = S3({arguments})", settings=TRUSTED
    )
    try:
        node.query(f"INSERT INTO {table} VALUES (7)", settings=TRUSTED)
        for settings in (RESTRICTED, TRUSTED, RESTRICTED):
            assert node.query(f"SELECT * FROM {table}", settings=settings) == "7\n"
    finally:
        node.query(f"DROP TABLE {table} SYNC")
