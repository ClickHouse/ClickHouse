import os
import uuid

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key
from helpers.mock_servers import start_mock_servers

ROLE = "extra_credentials(role_arn = 'arn::role', role_session_name = 'expired-token-test')"
BACKUP_SETTINGS = {
    "backup_restore_s3_retry_attempts": 2,
    "backup_restore_s3_retry_initial_backoff_ms": 10,
    "backup_restore_s3_retry_max_backoff_ms": 10,
    "backup_restore_s3_retry_jitter_factor": 0,
    "s3_max_single_operation_copy_size": 5 * 1024 * 1024,
}


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    cluster.add_instance(
        "node",
        with_minio=True,
        main_configs=["configs/s3.xml"],
        env_variables={"AWS_ACCESS_KEY_ID": "aws", "AWS_SECRET_ACCESS_KEY": "aws123"},
    )
    sts = cluster.add_instance(
        name="sts.amazonaws.com",
        hostname="sts.amazonaws.com",
        image="clickhouse/python-bottle",
        tag="latest",
        stay_alive=True,
    )
    sts.stop_clickhouse(kill=True)
    proxy = cluster.add_instance(
        name="s3-proxy",
        hostname="s3-proxy",
        image="clickhouse/python-bottle",
        tag="latest",
        stay_alive=True,
    )
    proxy.stop_clickhouse(kill=True)

    try:
        cluster.start()
        cluster.exec_in_container(
            cluster.minio_docker_id,
            [
                "mc",
                "alias",
                "set",
                "root",
                "http://minio1:9001",
                "minio",
                minio_secret_key,
            ],
        )
        cluster.exec_in_container(
            cluster.minio_docker_id,
            [
                "mc",
                "admin",
                "user",
                "add",
                "root",
                "minio_rotated",
                "Rotated_Minio_Test_Secret_123",
            ],
        )
        cluster.exec_in_container(
            cluster.minio_docker_id,
            [
                "mc",
                "admin",
                "policy",
                "attach",
                "root",
                "readwrite",
                "--user",
                "minio_rotated",
            ],
        )

        script_dir = os.path.join(os.path.dirname(__file__), "s3_mocks")
        start_mock_servers(
            cluster,
            script_dir,
            [
                ("mock_sts.py", "sts.amazonaws.com", "80", []),
                ("s3_proxy.py", "s3-proxy", "8080", []),
            ],
        )
        yield cluster
    finally:
        cluster.shutdown()


def proxy_url(cluster):
    return f"http://{cluster.get_instance_ip('s3-proxy')}:8080"


def arm(cluster, operation, prefix, count=1, rotate=False, delay_ms=0):
    response = requests.post(
        f"{proxy_url(cluster)}/control/arm",
        params={
            "operation": operation,
            "prefix": prefix,
            "count": count,
            "rotate": int(rotate),
            "delay_ms": delay_ms,
        },
        timeout=10,
    )
    response.raise_for_status()


def events(cluster):
    response = requests.get(f"{proxy_url(cluster)}/control/events", timeout=10)
    response.raise_for_status()
    return response.json()


def backup_destination(path):
    return f"S3('http://s3-proxy:8080/root/{path}', {ROLE})"


def backup(node, path, settings):
    return node.query(
        f"BACKUP TABLE expired_token_data TO {backup_destination(path)}",
        settings=settings,
    )


def matching_retry(events_, access_key):
    injected_events = [event for event in events_ if event["injected"]]
    assert injected_events, events_
    injected = injected_events[0]
    assert injected["access_key"] != access_key, events_
    assert any(
        event["path"] == injected["path"]
        and not event["injected"]
        and event["access_key"] == access_key
        for event in events_
    ), events_


def test_backup_restore_refreshes_expired_s3_credentials(started_cluster):
    cluster = started_cluster
    node = cluster.instances["node"]
    node.query(
        "CREATE TABLE expired_token_data (value String) ENGINE = MergeTree ORDER BY tuple() SETTINGS storage_policy = 's3_source'"
    )
    node.query("INSERT INTO expired_token_data SELECT randomString(8 * 1024 * 1024)")

    path = f"backups/expired_token_{uuid.uuid4().hex}"
    arm(cluster, "UploadPartCopy", f"/root/{path}/", rotate=True)
    assert "BACKUP_CREATED" in backup(node, path, BACKUP_SETTINGS)
    matching_retry(events(cluster), "minio_rotated")

    arm(cluster, "GetObject", f"/root/{path}/", rotate=True)
    result = node.query(
        f"RESTORE TABLE expired_token_data AS restored_data FROM {backup_destination(path)}",
        settings=BACKUP_SETTINGS,
    )
    assert "RESTORED" in result
    matching_retry(events(cluster), "minio")
    assert node.query("SELECT cityHash64(value) FROM restored_data") == node.query(
        "SELECT cityHash64(value) FROM expired_token_data"
    )

    node.query(
        "CREATE TABLE local_data (value String) ENGINE = MergeTree ORDER BY tuple()"
    )
    node.query("INSERT INTO local_data SELECT randomString(8 * 1024 * 1024)")
    upload_path = f"backups/expired_token_upload_{uuid.uuid4().hex}"
    arm(cluster, "UploadPart", f"/root/{upload_path}/", rotate=True)
    result = node.query(
        f"BACKUP TABLE local_data TO {backup_destination(upload_path)}",
        settings={**BACKUP_SETTINGS, "s3_max_single_part_upload_size": 0},
    )
    assert "BACKUP_CREATED" in result
    upload_events = events(cluster)
    matching_retry(upload_events, "minio_rotated")
    assert next(event for event in upload_events if event["injected"])["body_size"] > 0
    assert "RESTORED" in node.query(
        f"RESTORE TABLE local_data AS restored_local FROM {backup_destination(upload_path)}",
        settings=BACKUP_SETTINGS,
    )
    assert node.query("SELECT cityHash64(value) FROM restored_local") == node.query(
        "SELECT cityHash64(value) FROM local_data"
    )

    for timeout_ms, attempts, delay_ms, expected_requests in [
        (5000, 2, 0, 3),
        (200, 2, 500, 2),
        (1, 2, 0, 1),
        (0, 2, 0, 1),
    ]:
        failing_path = f"backups/expired_token_limit_{uuid.uuid4().hex}"
        arm(
            cluster,
            "UploadPartCopy",
            f"/root/{failing_path}/",
            count=100,
            delay_ms=delay_ms,
        )
        error = node.query_and_get_error(
            f"BACKUP TABLE expired_token_data TO {backup_destination(failing_path)}",
            settings={
                **BACKUP_SETTINGS,
                "backup_s3_expired_token_retry_timeout_ms": timeout_ms,
                "backup_restore_s3_retry_attempts": attempts,
            },
        )
        assert "ExpiredToken" in error, error
        observed = events(cluster)
        injected = [event for event in observed if event["injected"]]
        assert injected, observed
        first_path = injected[0]["path"]
        assert (
            len([event for event in observed if event["path"] == first_path])
            == expected_requests
        ), observed
