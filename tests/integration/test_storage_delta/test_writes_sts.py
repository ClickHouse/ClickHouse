"""
Delta Lake writes with STS assume-role credentials: writes through `extra_credentials(role_arn)`,
a wrong role fails closed, credentials rotated between two INSERTs rebuild the kernel engine,
and a stale token during an INSERT never leaves a half-written table.
"""

import logging

import pyarrow as pa
import pytest
from deltalake.writer import write_deltalake

from helpers.cluster import ClickHouseCluster
from test_storage_delta.test import randomize_table_name
from test_storage_s3.test_sts import run_s3_mocks

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node",
            with_minio=True,
            env_variables={"AWS_ACCESS_KEY_ID": "aws", "AWS_SECRET_ACCESS_KEY": "aws123"},
            main_configs=["configs/config.d/use_environment_credentials.xml"],
            user_configs=["configs/users.d/users.xml", "configs/users.d/enable_writes.xml"],
            stay_alive=True,
        )
        sts = cluster.add_instance(
            name="sts.amazonaws.com",
            hostname="sts.amazonaws.com",
            image="clickhouse/python-bottle",
            tag="latest",
            stay_alive=True,
        )
        sts.stop_clickhouse(kill=True)
        logging.info("Starting cluster...")
        cluster.start()
        if int(cluster.instances["node"].query("SELECT count() FROM system.table_engines WHERE name = 'DeltaLake'").strip()) == 0:
            pytest.skip("DeltaLake engine is not available")
        run_s3_mocks(cluster)
        yield cluster
    finally:
        cluster.shutdown()


def storage_options(started_cluster):
    return {
        "AWS_ENDPOINT_URL": f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}",
        "AWS_ACCESS_KEY_ID": "minio",
        "AWS_SECRET_ACCESS_KEY": "ClickHouse_Minio_P@ssw0rd",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def new_table(started_cluster, name):
    path = randomize_table_name(name)
    schema = pa.schema([("id", pa.int32(), False)])
    write_deltalake(
        f"s3://{started_cluster.minio_bucket}/{path}",
        pa.Table.from_arrays([pa.array([], type=pa.int32())], schema=schema),
        storage_options=storage_options(started_cluster),
        mode="overwrite",
    )
    return path


def table_function(started_cluster, path, role_session_name="miniorole"):
    url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{path}/"
    return f"deltaLake('{url}', extra_credentials(role_arn = 'arn::role', role_session_name = '{role_session_name}'))"


def log_versions(started_cluster, path):
    return sorted(
        int(obj.object_name.rsplit("/", 1)[1][:-5])
        for obj in started_cluster.minio_client.list_objects(started_cluster.minio_bucket, f"{path}/_delta_log/", recursive=True)
        if obj.object_name.endswith(".json")
    )


def data_files(started_cluster, path):
    return [obj.object_name for obj in started_cluster.minio_client.list_objects(started_cluster.minio_bucket, path, recursive=True) if obj.object_name.endswith(".parquet")]


def test_write_with_assumed_role(started_cluster):
    node = started_cluster.instances["node"]
    path = new_table(started_cluster, "test_sts_write")
    tf = table_function(started_cluster, path)
    node.query(f"INSERT INTO TABLE FUNCTION {tf} SELECT number AS id FROM numbers(10)")
    assert node.query(f"SELECT count(), sum(id) FROM {tf}").strip() == "10\t45"
    assert log_versions(started_cluster, path) == [0, 1]

    # A role the STS mock does not know yields wrong credentials: nothing may be written.
    _, error = node.query_and_get_answer_with_error(f"INSERT INTO TABLE FUNCTION {table_function(started_cluster, path, 'unknownrole')} VALUES (100)")
    assert error, "write with invalid assumed-role credentials succeeded"
    assert log_versions(started_cluster, path) == [0, 1]
    assert len(data_files(started_cluster, path)) == 1
    assert node.query(f"SELECT count() FROM {tf}").strip() == "10"


def test_write_after_credentials_rotation(started_cluster):
    node = started_cluster.instances["node"]
    path = new_table(started_cluster, "test_sts_rotation")
    node.query(f"CREATE TABLE {path} (id Int32) ENGINE = DeltaLake({table_function(started_cluster, path)[len('deltaLake(') : -1]})")
    node.query(f"INSERT INTO {path} SELECT number FROM numbers(5)")

    # Simulate an STS rotation between the two INSERTs. The write path opens a fresh kernel
    # transaction per INSERT (no cached snapshot state to rebuild), so the contract is that the
    # second commit lands with the rotated credentials.
    query_id = f"{path}_rotated"
    node.query("SYSTEM ENABLE FAILPOINT delta_kernel_force_credentials_fingerprint_drift")
    try:
        node.query(f"INSERT INTO {path} SELECT number + 5 FROM numbers(5)", query_id=query_id)
    finally:
        node.query("SYSTEM DISABLE FAILPOINT delta_kernel_force_credentials_fingerprint_drift")
    assert log_versions(started_cluster, path) == [0, 1, 2]
    assert node.query(f"SELECT count(), uniqExact(id) FROM {path}").strip() == "10\t10"
    node.query(f"DROP TABLE {path}")


def test_stale_token_during_write_fails_closed(started_cluster):
    node = started_cluster.instances["node"]
    path = new_table(started_cluster, "test_sts_stale")
    node.query(f"CREATE TABLE {path} (id Int32) ENGINE = DeltaLake({table_function(started_cluster, path)[len('deltaLake(') : -1]})")
    # The stale-token error fires when the kernel state is rebuilt, so force a rebuild too.
    node.query("SYSTEM ENABLE FAILPOINT delta_kernel_force_credentials_fingerprint_drift")
    node.query("SYSTEM ENABLE FAILPOINT delta_kernel_force_stale_token_error")
    try:
        _, error = node.query_and_get_answer_with_error(f"INSERT INTO {path} SELECT number FROM numbers(5)")
    finally:
        node.query("SYSTEM DISABLE FAILPOINT delta_kernel_force_stale_token_error")
        node.query("SYSTEM DISABLE FAILPOINT delta_kernel_force_credentials_fingerprint_drift")
    logging.info("stale token during write: %s", (error or "refreshed and committed").splitlines()[0][:200])
    # Fails closed: the stale token is not refreshed inside the INSERT, nothing is committed.
    assert error, "the INSERT succeeded with a stale token"
    assert log_versions(started_cluster, path) == [0]
    assert data_files(started_cluster, path) == []
    # Recovered: the next write works.
    node.query(f"INSERT INTO {path} SELECT number + 100 FROM numbers(5)")
    assert node.query(f"SELECT count() FROM {path}").strip() == "5"
    node.query(f"DROP TABLE {path}")
