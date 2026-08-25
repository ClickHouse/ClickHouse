import logging
import os
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

# `role_arn` and `role_session_name` are not secrets -- assuming the role still needs the server's
# own identity and a matching trust policy -- so they survive into the `<base_backup>` locator and
# let every hop of a chain reopen its base. The mock STS accepts the session name below.

ROLE_CREDENTIALS = (
    "extra_credentials(role_arn = 'arn::role', role_session_name = 'miniorole')"
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


def locator(started_cluster, path):
    url = (
        f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
        f"/{started_cluster.minio_bucket}/{path}"
    )
    return f"S3('{url}', {ROLE_CREDENTIALS})"


def backup(node, table, destination, base=None):
    query = f"BACKUP TABLE {table} TO {destination}"
    if base is not None:
        query += f" SETTINGS base_backup = {base}"
    return node.query(query).split("\t")[1].strip()


def restore(node, source, destination_table):
    query = f"RESTORE TABLE data AS {destination_table} FROM {source}"
    return node.query(query).split("\t")[1].strip()


# Two hops exercise one reopened base; five make every restore walk one more locator.
CHAIN_LENGTH = 5
ROWS_PER_HOP = 10


def test_restore_role_authenticated_incremental_chain(started_cluster):
    node = started_cluster.instances["node"]
    prefix = f"backups/role_arn_chain_{uuid.uuid4().hex}"

    node.query("DROP TABLE IF EXISTS data SYNC")
    node.query("CREATE TABLE data (key Int) ENGINE = MergeTree() ORDER BY tuple()")
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

    node.query("DROP TABLE IF EXISTS data SYNC")
    node.query("CREATE TABLE data (key Int) ENGINE = MergeTree() ORDER BY tuple()")
    node.query("INSERT INTO data SELECT number FROM numbers(10)")

    base = locator(started_cluster, f"{prefix}/base.bkp")
    inc = locator(started_cluster, f"{prefix}/inc.bkp")

    assert backup(node, "data", base) == "BACKUP_CREATED"
    node.query("INSERT INTO data SELECT number FROM numbers(10, 10)")
    assert backup(node, "data", inc, base=base) == "BACKUP_CREATED"

    url = (
        f"http://{started_cluster.minio_host}:{started_cluster.minio_port}"
        f"/{started_cluster.minio_bucket}/{prefix}/inc.bkp/.backup"
    )
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
