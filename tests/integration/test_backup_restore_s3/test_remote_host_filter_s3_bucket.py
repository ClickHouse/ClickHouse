import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/remote_url_allow_hosts_s3_bucket.xml"],
    with_minio=True,
)
# The same bucket, allowed without a port: this is the form of a real entry for a shared cloud
# endpoint on the default port, for example `s3.eu-west-1.amazonaws.com/<bucket>`.
node_no_port = cluster.add_instance(
    "node_no_port",
    main_configs=["configs/remote_url_allow_hosts_s3_bucket_no_port.xml"],
    with_minio=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        for instance in (node, node_no_port):
            instance.query(
                "CREATE TABLE t (id UInt64, s String) ENGINE = MergeTree ORDER BY id"
            )
            instance.query("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        yield cluster
    finally:
        cluster.shutdown()


def test_backup_to_allowed_bucket():
    name = uuid.uuid4().hex
    destination = f"S3('http://minio1:9001/root/data/backups/{name}', 'minio', '{minio_secret_key}')"
    node.query(f"BACKUP TABLE t TO {destination}")
    node.query(f"RESTORE TABLE t AS t_{name} FROM {destination}")
    assert node.query(f"SELECT count() FROM t_{name}") == "2\n"
    node.query(f"DROP TABLE t_{name} SYNC")


@pytest.mark.parametrize(
    "url",
    [
        "http://minio1:9001/other/data/backups/x",
        "http://minio1:9001/root-other/data/backups/x",
        "http://minio1:9002/root/data/backups/x",
        "http://resolver:8080/root/data/backups/x",
    ],
)
def test_backup_to_disallowed_bucket(url):
    destination = f"S3('{url}', 'minio', '{minio_secret_key}')"
    settings = "SETTINGS backup_restore_s3_retry_attempts = 0"

    error = node.query_and_get_error(f"BACKUP TABLE t TO {destination} {settings}")
    assert "UNACCEPTABLE_URL" in error, error

    error = node.query_and_get_error(
        f"RESTORE TABLE t AS t_restored FROM {destination} {settings}"
    )
    assert "UNACCEPTABLE_URL" in error, error


def test_s3_table_function_respects_bucket():
    name = uuid.uuid4().hex
    node.query(
        f"INSERT INTO FUNCTION s3('http://minio1:9001/root/data/{name}.csv', 'minio', '{minio_secret_key}', 'CSV') "
        "SELECT * FROM t"
    )
    assert (
        node.query(
            f"SELECT count() FROM s3('http://minio1:9001/root/data/{name}.csv', 'minio', '{minio_secret_key}', 'CSV', 'id UInt64, s String')"
        )
        == "2\n"
    )

    error = node.query_and_get_error(
        f"SELECT * FROM s3('http://minio1:9001/other/data/{name}.csv', 'minio', '{minio_secret_key}', 'CSV', 'id UInt64, s String')"
    )
    assert "UNACCEPTABLE_URL" in error, error

    error = node.query_and_get_error(
        f"SELECT * FROM url('http://minio1:9001/root/data/{name}.csv', 'CSV', 'id UInt64, s String')"
    )
    assert "UNACCEPTABLE_URL" in error, error


@pytest.mark.parametrize("disk_type", ["s3", "s3_plain_rewritable"])
def test_custom_s3_disk_respects_bucket(disk_type):
    name = uuid.uuid4().hex

    def create_query(table, endpoint):
        return f"""
            CREATE TABLE {table} (id UInt64, s String) ENGINE = MergeTree ORDER BY id
            SETTINGS disk = disk(
                type = '{disk_type}',
                endpoint = '{endpoint}',
                access_key_id = 'minio',
                secret_access_key = '{minio_secret_key}')
            """

    node.query(create_query(f"t_disk_{name}", f"http://minio1:9001/root/data/disks/{name}/"))
    node.query(f"INSERT INTO t_disk_{name} SELECT * FROM t")
    assert node.query(f"SELECT count() FROM t_disk_{name}") == "2\n"
    node.query(f"DROP TABLE t_disk_{name} SYNC")

    # Other buckets on the allowed host, another port and another host are rejected before the disk
    # (and its S3 client) is created.
    for endpoint in [
        f"http://minio1:9001/other/data/disks/{name}/",
        f"http://minio1:9001/root-other/data/disks/{name}/",
        f"http://minio1:9002/root/data/disks/{name}/",
        f"http://resolver:8080/root/data/disks/{name}/",
    ]:
        error = node.query_and_get_error(create_query(f"t_disk_bad_{name}", endpoint))
        assert "UNACCEPTABLE_URL" in error, error


def test_database_s3_respects_bucket():
    name = uuid.uuid4().hex
    node.query(
        f"INSERT INTO FUNCTION s3('http://minio1:9001/root/data/{name}.csv', 'minio', '{minio_secret_key}', 'CSV') "
        "SELECT * FROM t"
    )
    node.query(
        f"CREATE DATABASE db_{name} ENGINE = S3('http://minio1:9001/root', 'minio', '{minio_secret_key}')"
    )
    assert node.query(f"EXISTS TABLE db_{name}.`data/{name}.csv`") == "1\n"
    assert node.query(f"SELECT count() FROM db_{name}.`data/{name}.csv`") == "2\n"

    node.query(
        f"CREATE DATABASE db_other_{name} ENGINE = S3('http://minio1:9001/other', 'minio', '{minio_secret_key}')"
    )
    # `DatabaseS3` checks the URL with `throw_on_error = false` when it looks a table up, so a rejected
    # bucket surfaces as a missing table and not as `UNACCEPTABLE_URL`. `CREATE DATABASE` itself does not
    # check the URL.
    assert node.query(f"EXISTS TABLE db_other_{name}.`data/{name}.csv`") == "0\n"
    error = node.query_and_get_error(
        f"SELECT * FROM db_other_{name}.`data/{name}.csv`"
    )
    assert "UNKNOWN_TABLE" in error, error

    node.query(f"DROP DATABASE db_{name}")
    node.query(f"DROP DATABASE db_other_{name}")


def test_bucket_entry_without_port():
    # `<s3_bucket>minio1/root</s3_bucket>` names no port, so it matches the bucket on every port, like a
    # `<host>` entry without a port.
    name = uuid.uuid4().hex
    destination = f"S3('http://minio1:9001/root/data/backups/{name}', 'minio', '{minio_secret_key}')"
    node_no_port.query(f"BACKUP TABLE t TO {destination}")
    node_no_port.query(f"RESTORE TABLE t AS t_{name} FROM {destination}")
    assert node_no_port.query(f"SELECT count() FROM t_{name}") == "2\n"
    node_no_port.query(f"DROP TABLE t_{name} SYNC")

    settings = "SETTINGS backup_restore_s3_retry_attempts = 0"

    # Another bucket on the same host is still rejected.
    error = node_no_port.query_and_get_error(
        f"BACKUP TABLE t TO S3('http://minio1:9001/other/data/backups/{name}', 'minio', '{minio_secret_key}') {settings}"
    )
    assert "UNACCEPTABLE_URL" in error, error

    # The same bucket on another port passes the filter. Nothing listens on 9002, so the failure comes
    # from the connection and not from `remote_url_allow_hosts`.
    error = node_no_port.query_and_get_error(
        f"BACKUP TABLE t TO S3('http://minio1:9002/root/data/backups/{name}', 'minio', '{minio_secret_key}') {settings}"
    )
    assert "S3_ERROR" in error and "UNACCEPTABLE_URL" not in error, error


def test_base_backup_respects_bucket():
    # The `base_backup` of an incremental backup is a second S3 reference in the same statement and goes
    # through the same check as the destination.
    name = uuid.uuid4().hex
    node.query(
        f"CREATE TABLE t_{name} (id UInt64, s String) ENGINE = MergeTree ORDER BY id"
    )
    node.query(f"INSERT INTO t_{name} VALUES (1, 'a'), (2, 'b')")

    base = f"S3('http://minio1:9001/root/data/backups/{name}_base', 'minio', '{minio_secret_key}')"
    incremental = f"S3('http://minio1:9001/root/data/backups/{name}_inc', 'minio', '{minio_secret_key}')"
    node.query(f"BACKUP TABLE t_{name} TO {base}")
    node.query(f"INSERT INTO t_{name} VALUES (3, 'c')")
    node.query(f"BACKUP TABLE t_{name} TO {incremental} SETTINGS base_backup = {base}")
    node.query(f"RESTORE TABLE t_{name} AS t_{name}_restored FROM {incremental}")
    assert node.query(f"SELECT count() FROM t_{name}_restored") == "3\n"

    rejected_base = f"S3('http://minio1:9001/other/data/backups/{name}_base', 'minio', '{minio_secret_key}')"
    error = node.query_and_get_error(
        f"BACKUP TABLE t_{name} TO S3('http://minio1:9001/root/data/backups/{name}_inc2', 'minio', '{minio_secret_key}') "
        f"SETTINGS base_backup = {rejected_base}, backup_restore_s3_retry_attempts = 0"
    )
    assert "UNACCEPTABLE_URL" in error, error

    node.query(f"DROP TABLE t_{name} SYNC")
    node.query(f"DROP TABLE t_{name}_restored SYNC")


def test_reload_config_updates_buckets_and_keeps_previous_on_error():
    # `remote_url_allow_hosts` is applied on `SYSTEM RELOAD CONFIG` without a restart. A malformed
    # `<s3_bucket>` makes the reload fail and keeps the list from the previous successful load.
    # `root2` is the second bucket that the test MinIO creates.
    config_path = "/etc/clickhouse-server/config.d/remote_url_allow_hosts_s3_bucket.xml"
    name = uuid.uuid4().hex
    settings = "SETTINGS backup_restore_s3_retry_attempts = 0"

    def backup_query(bucket, step):
        return f"BACKUP TABLE t TO S3('http://minio1:9001/{bucket}/data/backups/{name}_{step}', 'minio', '{minio_secret_key}') {settings}"

    def backup_error(bucket, step):
        return node.query_and_get_error(backup_query(bucket, step))

    def set_config(buckets):
        entries = "".join(f"<s3_bucket>{bucket}</s3_bucket>" for bucket in buckets)
        node.replace_config(
            config_path,
            f"<clickhouse><remote_url_allow_hosts>{entries}</remote_url_allow_hosts></clickhouse>",
        )

    try:
        assert "UNACCEPTABLE_URL" in backup_error("root2", 1)

        set_config(["minio1:9001/root", "minio1:9001/root2"])
        node.query("SYSTEM RELOAD CONFIG")
        node.query(backup_query("root2", 2))
        assert "UNACCEPTABLE_URL" in backup_error("root-other", 3)

        # The malformed entry comes first: a parser that clears the lists before it reads the entries
        # would leave nothing allowed here, so the checks below would reject `root` and `root2`.
        set_config(["minio1:9001", "minio1:9001/root", "minio1:9001/root2"])
        error = node.query_and_get_error("SYSTEM RELOAD CONFIG")
        assert "must have the form host/bucket or host:port/bucket" in error, error
        node.query(backup_query("root", 4))
        node.query(backup_query("root2", 5))
        assert "UNACCEPTABLE_URL" in backup_error("root-other", 6)
    finally:
        set_config(["minio1:9001/root"])
        node.query("SYSTEM RELOAD CONFIG")

    assert "UNACCEPTABLE_URL" in backup_error("root2", 7)
    node.query(backup_query("root", 8))
