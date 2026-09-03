import io
import logging
import os
import re
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

# `role_arn` and `role_session_name` are not secrets -- assuming the role still needs the server's
# own identity and a matching trust policy -- so they survive into the `<base_backup>` locator and
# let every hop of a chain reopen its base. The mock STS accepts the role and session name below.
#
# Metadata written by a version that stripped the identifiers names no credentials and carries no
# marker to reconstruct them from, so its base backup opens unauthenticated. Such a chain must still
# be recoverable with `use_same_s3_credentials_for_base_backup = 1`, which propagates the credentials
# of the locator the RESTORE was given -- here a role -- into every base backup along the way. That
# shape is written into the metadata directly, so it keeps being exercised whatever a current backup
# would store.

ROLE_CREDENTIALS = (
    "extra_credentials(role_arn = 'arn::role', role_session_name = 'miniorole')"
)

BASE_BACKUP_RE = re.compile(r"<base_backup>.*?</base_backup>", re.DOTALL)
MARKER_RE = re.compile(
    r"<base_backup_copy_s3_credentials_from_backup>.*?</base_backup_copy_s3_credentials_from_backup>",
    re.DOTALL,
)


def run_s3_mocks(started_cluster):
    script_dir = os.path.join(os.path.dirname(__file__), "s3_mocks")
    start_mock_servers(
        started_cluster,
        script_dir,
        [("mock_sts.py", "sts.amazonaws.com", "80", [])],
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            with_minio=True,
            # The `AssumeRole` call itself is signed with the server's ambient credentials.
            env_variables={
                "AWS_ACCESS_KEY_ID": "aws",
                "AWS_SECRET_ACCESS_KEY": "aws123",
            },
            main_configs=["configs/use_environment_credentials.xml"],
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
        logging.info("Cluster started")

        run_s3_mocks(cluster)

        yield cluster
    finally:
        cluster.shutdown()


def object_url(started_cluster, path):
    return (
        f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
        f"/{started_cluster.minio_bucket}/{path}"
    )


def locator(started_cluster, path):
    return f"S3('{object_url(started_cluster, path)}', {ROLE_CREDENTIALS})"


def status_of(response):
    return response.split("\t")[1].strip()


def backup(node, table, destination, base=None):
    query = f"BACKUP TABLE {table} TO {destination}"
    if base is not None:
        query += f" SETTINGS base_backup = {base}"
    return status_of(node.query(query))


def restore(node, source, destination_table, settings=None):
    query = f"RESTORE TABLE data AS {destination_table} FROM {source}"
    if settings is not None:
        query += f" SETTINGS {settings}"
    return status_of(node.query(query))


def read_metadata(started_cluster, path):
    response = started_cluster.minio_client.get_object(
        started_cluster.minio_bucket, f"{path}/.backup"
    )
    try:
        return response.read().decode()
    finally:
        response.close()
        response.release_conn()


def rewrite_base_backup_as_legacy(started_cluster, path, base_path):
    """Leave a base backup locator naming no credentials and no marker to rebuild them from."""
    content = read_metadata(started_cluster, path)
    assert "<base_backup>" in content, content

    # The locator is XML-escaped in the metadata, so the quotes around the url are `&apos;`.
    bare = f"<base_backup>S3(&apos;{object_url(started_cluster, base_path)}&apos;)</base_backup>"
    legacy = BASE_BACKUP_RE.sub(lambda _: bare, content, count=1)
    legacy = MARKER_RE.sub("", legacy)

    # With anything left to authenticate the base backup, the test would pass for the wrong reason.
    # The clause is what carries the role; `role_arn` alone also occurs in the backup path.
    assert "extra_credentials" not in legacy, legacy
    assert "<base_backup_copy_s3_credentials_from_backup>" not in legacy, legacy

    encoded = legacy.encode()
    started_cluster.minio_client.put_object(
        started_cluster.minio_bucket,
        f"{path}/.backup",
        io.BytesIO(encoded),
        len(encoded),
    )


def create_data(node):
    node.query("DROP TABLE IF EXISTS data SYNC")
    node.query("CREATE TABLE data (key Int) ENGINE = MergeTree() ORDER BY tuple()")


# Two hops exercise one reopened base; five make every restore walk one more locator.
CHAIN_LENGTH = 5
ROWS_PER_HOP = 10


def test_restore_role_authenticated_incremental_chain(started_cluster):
    node = started_cluster.instances["node"]
    prefix = f"backups/role_arn_chain_{uuid.uuid4().hex}"

    create_data(node)
    node.query(f"INSERT INTO data SELECT number FROM numbers({ROWS_PER_HOP})")

    chain = [
        locator(started_cluster, f"{prefix}/backup_{hop}.bkp")
        for hop in range(CHAIN_LENGTH)
    ]

    assert backup(node, "data", chain[0]) == "BACKUP_CREATED"

    # Each hop's own rows make the count say how far down the chain a restore reached.
    for hop in range(1, CHAIN_LENGTH):
        node.query(
            f"INSERT INTO data SELECT number FROM numbers({hop * ROWS_PER_HOP}, {ROWS_PER_HOP})"
        )
        assert backup(node, "data", chain[hop], base=chain[hop - 1]) == "BACKUP_CREATED"

    # No hop re-supplies `base_backup`: each reopens its base from the role in its own metadata.
    for hop, source in enumerate(chain):
        table = f"data_hop_{hop}"
        assert restore(node, source, table) == "RESTORED"
        assert node.query(f"SELECT count() FROM {table}").strip() == str(
            (hop + 1) * ROWS_PER_HOP
        )


def test_role_identifiers_kept_in_backup_metadata(started_cluster):
    node = started_cluster.instances["node"]
    prefix = f"backups/role_arn_metadata_{uuid.uuid4().hex}"

    create_data(node)
    node.query("INSERT INTO data SELECT number FROM numbers(10)")

    base = locator(started_cluster, f"{prefix}/base.bkp")
    inc = locator(started_cluster, f"{prefix}/inc.bkp")

    assert backup(node, "data", base) == "BACKUP_CREATED"
    node.query("INSERT INTO data SELECT number FROM numbers(10, 10)")
    assert backup(node, "data", inc, base=base) == "BACKUP_CREATED"

    url = f"{object_url(started_cluster, f'{prefix}/inc.bkp')}/.backup"
    metadata = node.query(
        f"SELECT line FROM s3('{url}', 'LineAsString', 'line String', {ROLE_CREDENTIALS}) FORMAT TSVRaw"
    )

    base_backup_lines = [
        line for line in metadata.splitlines() if "<base_backup>" in line
    ]
    assert len(base_backup_lines) == 1, metadata
    # The role identifiers stay, so the base stays openable; `external_id` and key pairs never do.
    assert "arn::role" in base_backup_lines[0]
    assert "miniorole" in base_backup_lines[0]


def test_restore_legacy_role_chain_reusing_credentials(started_cluster):
    node = started_cluster.instances["node"]
    prefix = f"backups/role_arn_legacy_{uuid.uuid4().hex}"

    create_data(node)

    base_path = f"{prefix}/base.bkp"
    inc_1_path = f"{prefix}/inc_1.bkp"
    inc_2_path = f"{prefix}/inc_2.bkp"

    base = locator(started_cluster, base_path)
    inc_1 = locator(started_cluster, inc_1_path)
    inc_2 = locator(started_cluster, inc_2_path)

    node.query("INSERT INTO data SELECT number FROM numbers(10)")
    assert backup(node, "data", base) == "BACKUP_CREATED"

    node.query("INSERT INTO data SELECT number FROM numbers(10, 10)")
    assert backup(node, "data", inc_1, base=base) == "BACKUP_CREATED"

    node.query("INSERT INTO data SELECT number FROM numbers(20, 10)")
    assert backup(node, "data", inc_2, base=inc_1) == "BACKUP_CREATED"

    rewrite_base_backup_as_legacy(started_cluster, inc_1_path, base_path)
    rewrite_base_backup_as_legacy(started_cluster, inc_2_path, inc_1_path)

    # Nothing is left to authenticate the base backup with, so restoring the chain fails.
    error = node.query_and_get_error(f"RESTORE TABLE data AS data_legacy FROM {inc_2}")
    assert "ACCESS_DENIED" in error or "Access Denied" in error, error

    # Reusing the role named on the RESTORE locator reaches every hop of the chain.
    assert (
        restore(
            node,
            inc_2,
            "data_recovered",
            settings="use_same_s3_credentials_for_base_backup = 1",
        )
        == "RESTORED"
    )
    assert node.query("SELECT count() FROM data_recovered").strip() == "30"
    assert node.query("SELECT sum(key) FROM data_recovered").strip() == "435"
