import json
import logging
import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers
from helpers.s3_tools import prepare_s3_bucket

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


def run_s3_mock(cluster):
    script_dir = os.path.join(os.path.dirname(__file__), "s3_mocks")
    start_mock_servers(
        cluster,
        script_dir,
        [
            ("mocker_s3.py", "resolver", "8081"),
        ],
    )


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    try:
        cluster.add_instance(
            "node",
            main_configs=["configs/storage_conf.xml"],
            with_minio=True,
        )
        cluster.start()

        prepare_s3_bucket(cluster)
        run_s3_mock(cluster)

        yield cluster
    finally:
        cluster.shutdown()


def get_recorded_storage_classes(cluster):
    response = cluster.exec_in_container(
        cluster.get_container_id("resolver"),
        ["curl", "-s", "http://localhost:8081/recorded_storage_classes"],
    )
    return json.loads(response)


def reset_recorded_storage_classes(cluster):
    cluster.exec_in_container(
        cluster.get_container_id("resolver"),
        ["curl", "-s", "http://localhost:8081/reset_recorded_storage_classes"],
    )


def test_multipart_upload_uses_storage_class(started_cluster):
    node = started_cluster.instances["node"]

    node.query(
        """
        CREATE TABLE test_s3_storage_class_multipart
        (
            id UInt64,
            value String
        )
        ENGINE = MergeTree
        ORDER BY id
        SETTINGS storage_policy = 's3_intelligent_tiering';
        """
    )

    # Enough distinct data so that the data files of the part are larger than
    # the (tiny) single-part upload threshold and are uploaded via multipart upload.
    node.query(
        """
        INSERT INTO test_s3_storage_class_multipart
        SELECT number, toString(number) FROM numbers(100000);
        """
    )

    assert node.query("SELECT count() FROM test_s3_storage_class_multipart") == "100000\n"

    recorded = get_recorded_storage_classes(started_cluster)

    # The bug (https://github.com/ClickHouse/ClickHouse/issues/68551) is that the storage
    # class is set for single-part PutObject requests but not for multipart uploads, so
    # large objects silently end up with the default storage class. Without the fix, the
    # CreateMultipartUpload requests carry no storage class header at all (recorded as None).
    assert (
        len(recorded["CreateMultipartUpload"]) > 0
    ), "Expected at least one multipart upload to be created"

    assert all(
        storage_class == "STANDARD"
        for storage_class in recorded["CreateMultipartUpload"]
    ), f"Some multipart uploads did not carry the configured storage class: {recorded['CreateMultipartUpload']}"

    # Single-part uploads (small files of the part) must keep using the storage class too.
    assert all(
        storage_class == "STANDARD"
        for storage_class in recorded["PutObject"]
    ), f"Some single-part uploads did not carry the configured storage class: {recorded['PutObject']}"


def test_object_storage_storage_class_alias(started_cluster):
    node = started_cluster.instances["node"]

    # The `s3` table function (object storage) must also accept the legacy `storage_class`
    # spelling as an alias for `storage_class_name`, passed here as a key-value argument.
    # See https://github.com/ClickHouse/ClickHouse/issues/68551
    reset_recorded_storage_classes(started_cluster)

    node.query(
        """
        INSERT INTO FUNCTION s3(
            'http://resolver:8081/root/data/test_object_storage_alias.csv',
            'minio', 'ClickHouse_Minio_P@ssw0rd', 'CSV', 'id UInt64, value String',
            storage_class = 'STANDARD')
        SELECT number, toString(number) FROM numbers(10)
        SETTINGS s3_truncate_on_insert = 1;
        """
    )

    recorded = get_recorded_storage_classes(started_cluster)

    # Without the alias wired through the object-storage key-value path, no storage class
    # header would be sent (recorded as None) and this assertion would fail.
    uploads = recorded["PutObject"] + recorded["CreateMultipartUpload"]
    assert len(uploads) > 0, "Expected at least one object-creating request"
    assert all(
        storage_class == "STANDARD" for storage_class in uploads
    ), f"The `storage_class` alias did not reach the object-storage write: {recorded}"
