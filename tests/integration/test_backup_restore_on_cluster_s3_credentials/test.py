import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.test_tools import TSV

# `BACKUP ... ON CLUSTER` initiated by a trusted user whose profile enables
# `s3_allow_server_credentials_in_user_queries` must work even when the default profile pins the setting
# to 0 with a readonly constraint (the hardened-deployment setup). The initiator's `=1` travels to the
# other hosts inside the distributed DDL entry, but those hosts clamp the entry settings against the
# default profile (no user is set for distributed DDL by default), which silently drops the change. The
# internal BACKUP/RESTORE queries there must still be allowed to use the server credentials, because the
# initiator has already opened the same backup destination under its own, properly constrained, settings.
#
# The server's own credentials come from the AWS shared-credentials file (with AWS_EC2_METADATA_DISABLED
# and no AWS_ACCESS_KEY_ID it is the only credential source), so a bare `S3('url')` backup engine relies
# on server-managed credentials, which is exactly what the restriction guards.

cluster = ClickHouseCluster(__file__)

main_configs = ["configs/cluster.xml"]
user_configs = ["configs/users.xml"]
env_variables = {
    "AWS_SHARED_CREDENTIALS_FILE": "/tmp/aws_credentials",
    "AWS_EC2_METADATA_DISABLED": "true",
}

node1 = cluster.add_instance(
    "node1",
    main_configs=main_configs,
    user_configs=user_configs,
    env_variables=env_variables,
    macros={"replica": "node1", "shard": "shard1"},
    with_zookeeper=True,
    with_minio=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=main_configs,
    user_configs=user_configs,
    env_variables=env_variables,
    macros={"replica": "node2", "shard": "shard1"},
    with_zookeeper=True,
    with_minio=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        # Stand in for the server's own AWS profile credentials.
        for node in [node1, node2]:
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "printf '[default]\\naws_access_key_id = %s\\naws_secret_access_key = %s\\n' > /tmp/aws_credentials"
                    % (minio_access_key, minio_secret_key),
                ]
            )
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_after_test():
    try:
        yield
    finally:
        node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster' SYNC")


backup_id_counter = 0


def new_backup_destination():
    global backup_id_counter
    backup_id_counter += 1
    return f"S3('http://minio1:9001/root/backup_on_cluster_{backup_id_counter}')"


def new_backup_destination_with_keys():
    global backup_id_counter
    backup_id_counter += 1
    return (
        f"S3('http://minio1:9001/root/backup_on_cluster_{backup_id_counter}', "
        f"'{minio_access_key}', '{minio_secret_key}')"
    )


def create_and_fill_table():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' (x UInt64) "
        "ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}') ORDER BY x"
    )
    node1.query("INSERT INTO tbl SELECT number FROM numbers(10)")
    node2.query("SYSTEM SYNC REPLICA tbl")


def test_backup_restore_on_cluster_with_server_credentials():
    create_and_fill_table()
    backup_destination = new_backup_destination()

    # The profile of `backup_user` enables the setting, so the whole ON CLUSTER operation must succeed,
    # including the internal queries on the host that did not initiate the backup.
    node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_destination}",
        user="backup_user",
    )

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node1.query(
        f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_destination}",
        user="backup_user",
    )

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")
    assert node1.query("SELECT count(), sum(x) FROM tbl") == TSV([[10, 45]])
    assert node2.query("SELECT count(), sum(x) FROM tbl") == TSV([[10, 45]])


def test_restriction_still_applies_to_untrusted_users():
    create_and_fill_table()
    backup_destination = new_backup_destination()

    # The default profile pins the setting to 0, so the initiator itself must reject the backup before
    # any other host is involved.
    error = node1.query_and_get_error(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_destination}"
    )
    assert "ACCESS_DENIED" in error, error

    # The readonly constraint forbids opting in.
    error = node1.query_and_get_error(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_destination} "
        "SETTINGS s3_allow_server_credentials_in_user_queries = 1"
    )
    assert "SETTING_CONSTRAINT_VIOLATION" in error, error

    # The `internal` backup setting marks the trusted on-cluster continuation queries and would bypass
    # the initiator-side check above, so it must not be accepted from a user.
    error = node1.query_and_get_error(
        f"BACKUP TABLE tbl TO {backup_destination} SETTINGS internal = true"
    )
    assert "Setting 'internal' cannot be set explicitly" in error, error


def test_untrusted_initiator_cannot_reach_server_credentials_on_other_hosts():
    # The fix propagates the initiator's `s3_allow_server_credentials_in_user_queries` to the other hosts
    # so a trusted on-cluster backup works. This must not let an untrusted initiator reach the server
    # credentials on those hosts. The base backup is opened lazily during entry collection, which for an
    # ON CLUSTER backup happens on the per-host (internal) operations, not on the initiator. So a backup
    # with explicit credentials for the destination (accepted, they are explicit) but a bare-URL base backup
    # (which resolves the server's own credentials) exercises the restriction specifically on the other host.
    create_and_fill_table()
    destination = new_backup_destination_with_keys()
    base_backup = new_backup_destination()  # bare URL: relies on server-managed credentials

    error = node1.query_and_get_error(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {destination} SETTINGS base_backup = {base_backup}"
    )
    assert "server-managed credentials" in error or "ACCESS_DENIED" in error, error
