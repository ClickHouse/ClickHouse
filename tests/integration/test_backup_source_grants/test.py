"""BACKUP/RESTORE destinations must be authorized against the user's SOURCES grants.

`SOURCES ON *.*` revoked, a regex-scoped `WRITE ON S3` for one prefix, and MinIO. Without the
check, the user can write a backup to (and read one from) any bucket the server can reach.
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/enable_grants.xml",
        "configs/remote_servers.xml",
        "configs/backups.xml",
    ],
    with_minio=True,
    with_azurite=True,
    with_zookeeper=True,
)

USER = "u67785"
ALLOWED = "http://minio1:9001/root/data/allowed"
DENIED = "http://minio1:9001/root/data/denied"


def s3(url):
    return f"S3('{url}', 'minio', '{minio_secret_key}')"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node.query(
            """
            CREATE DATABASE IF NOT EXISTS d67785;
            CREATE TABLE d67785.secrets (x UInt64) ENGINE = MergeTree ORDER BY x;
            INSERT INTO d67785.secrets VALUES (42);
            """
        )
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def restricted_user(started_cluster):
    node.query(f"DROP USER IF EXISTS {USER}")
    node.query(
        f"""
        CREATE USER {USER};
        GRANT BACKUP ON d67785.* TO {USER};
        GRANT CREATE DATABASE, DROP DATABASE ON *.* TO {USER};
        GRANT CREATE TABLE, INSERT, SELECT ON d67785.* TO {USER};
        GRANT CLUSTER ON *.* TO {USER};
        REVOKE SOURCES ON *.* FROM {USER};
        """
    )
    yield
    node.query(f"DROP USER IF EXISTS {USER}")


def test_backup_requires_write_on_the_destination(started_cluster):
    # The reporter's exact case: only the `allowed` prefix is granted, so a backup to `denied`
    # must be refused with ACCESS_DENIED rather than reaching S3 (which used to give S3_ERROR).
    node.query(f"GRANT WRITE ON S3('{ALLOWED}/.*') TO {USER}")

    error = node.query_and_get_error(
        f"BACKUP TABLE d67785.secrets TO {s3(DENIED + '/b1')}", user=USER
    )
    assert "ACCESS_DENIED" in error, error
    assert "WRITE ON S3" in error, error

    node.query(
        f"BACKUP TABLE d67785.secrets TO {s3(ALLOWED + '/b1')} FORMAT Null", user=USER
    )


def test_restore_requires_read_on_the_source(started_cluster):
    node.query(f"BACKUP TABLE d67785.secrets TO {s3(DENIED + '/b2')} FORMAT Null")

    # WRITE alone must not authorize reading a backup back.
    node.query(f"GRANT WRITE ON S3 TO {USER}")
    error = node.query_and_get_error(
        f"RESTORE TABLE d67785.secrets AS d67785.r2 FROM {s3(DENIED + '/b2')}", user=USER
    )
    assert "ACCESS_DENIED" in error, error
    assert (
        node.query("SELECT count() FROM system.tables WHERE database = 'd67785' AND name = 'r2'")
        == "0\n"
    )

    node.query(f"GRANT READ ON S3('{DENIED}/.*') TO {USER}")
    node.query(
        f"RESTORE TABLE d67785.secrets AS d67785.r2 FROM {s3(DENIED + '/b2')} FORMAT Null",
        user=USER,
    )
    assert node.query("SELECT x FROM d67785.r2") == "42\n"
    node.query("DROP TABLE d67785.r2 SYNC")


def test_create_database_backup_engine_requires_read(started_cluster):
    node.query(f"BACKUP DATABASE d67785 TO {s3(DENIED + '/b3')} FORMAT Null")

    error = node.query_and_get_error(
        f"CREATE DATABASE db67785 ENGINE = Backup('d67785', {s3(DENIED + '/b3')})", user=USER
    )
    assert "ACCESS_DENIED" in error, error
    assert node.query("SELECT count() FROM system.databases WHERE name = 'db67785'") == "0\n"

    node.query(f"GRANT READ ON S3('{DENIED}/.*') TO {USER}")
    node.query(
        f"CREATE DATABASE db67785 ENGINE = Backup('d67785', {s3(DENIED + '/b3')})", user=USER
    )
    assert node.query("SELECT x FROM db67785.secrets") == "42\n"
    node.query("DROP DATABASE db67785 SYNC")


def test_named_collection_destination_requires_the_whole_source_grant(started_cluster):
    # A collection can be re-pointed after the check, so it is authorized whole-source: a grant
    # filtered on the URL it currently resolves to is deliberately not enough.
    collection = "nc67785"
    node.query(
        f"""
        CREATE NAMED COLLECTION IF NOT EXISTS {collection} AS
            url = '{DENIED}/b8',
            access_key_id = 'minio',
            secret_access_key = '{minio_secret_key}'
        """
    )
    try:
        # Not what this case tests: the collection itself must be usable by the user.
        node.query(f"GRANT NAMED COLLECTION ON {collection} TO {USER}")
        node.query(f"GRANT WRITE ON S3('{DENIED}/.*') TO {USER}")

        error = node.query_and_get_error(
            f"BACKUP TABLE d67785.secrets TO S3({collection})", user=USER
        )
        assert "ACCESS_DENIED" in error, error
        # Not a missing NAMED COLLECTION privilege, which would also deny.
        assert "WRITE ON S3" in error, error

        node.query(f"GRANT WRITE ON S3 TO {USER}")
        node.query(f"BACKUP TABLE d67785.secrets TO S3({collection}) FORMAT Null", user=USER)
    finally:
        node.query(f"DROP NAMED COLLECTION IF EXISTS {collection}")


def azure(path):
    return (
        f"AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', "
        f"'{cluster.azurite_container}', '{path}')"
    )


def test_azure_destination_requires_the_matching_direction(started_cluster):
    # The Azure hook is not a copy of the S3 one: its own pre-split blob path and its own
    # reader-UNLOCK direction, so S3 coverage does not transfer.
    error = node.query_and_get_error(
        f"BACKUP TABLE d67785.secrets TO {azure('b9')}", user=USER
    )
    assert "ACCESS_DENIED" in error, error
    assert "WRITE ON AZURE" in error, error

    node.query(f"GRANT WRITE ON AZURE TO {USER}")
    node.query(f"BACKUP TABLE d67785.secrets TO {azure('b9')} FORMAT Null", user=USER)

    # WRITE alone must not authorize reading it back.
    error = node.query_and_get_error(
        f"RESTORE TABLE d67785.secrets AS d67785.r9 FROM {azure('b9')}", user=USER
    )
    assert "ACCESS_DENIED" in error, error
    assert "READ ON AZURE" in error, error
    assert (
        node.query("SELECT count() FROM system.tables WHERE database = 'd67785' AND name = 'r9'")
        == "0\n"
    )

    node.query(f"GRANT READ ON AZURE TO {USER}")
    node.query(
        f"RESTORE TABLE d67785.secrets AS d67785.r9 FROM {azure('b9')} FORMAT Null", user=USER
    )
    assert node.query("SELECT x FROM d67785.r9") == "42\n"
    node.query("DROP TABLE d67785.r9 SYNC")


def test_create_database_on_cluster_is_authorized_on_the_initiator(started_cluster):
    # `is_create_database` returns from executeQueryOnCluster before DatabaseFactory runs, so
    # only the initiator preflight can gate this; worker legs have no user by default.
    node.query(f"BACKUP DATABASE d67785 TO {s3(DENIED + '/b7')} FORMAT Null")

    error = node.query_and_get_error(
        f"CREATE DATABASE dbcluster ON CLUSTER one_shard "
        f"ENGINE = Backup('d67785', {s3(DENIED + '/b7')})",
        user=USER,
    )
    assert "ACCESS_DENIED" in error, error
    assert "READ ON S3" in error, error
    assert (
        node.query("SELECT count() FROM system.databases WHERE name = 'dbcluster'")
        == "0\n"
    )

    node.query(f"GRANT READ ON S3('{DENIED}/.*') TO {USER}")
    node.query(
        f"CREATE DATABASE dbcluster ON CLUSTER one_shard "
        f"ENGINE = Backup('d67785', {s3(DENIED + '/b7')})",
        user=USER,
    )
    assert node.query("SELECT x FROM dbcluster.secrets") == "42\n"
    node.query("DROP DATABASE dbcluster ON CLUSTER one_shard SYNC")


def test_restore_on_cluster_authorizes_an_embedded_definition_over_an_existing_database(
    started_cluster,
):
    # `CHECK_ACCESS_ONLY` runs in the initiator process for every host, so a local existence answer
    # cannot stand in for the host that will actually create the database. Without the mode conjunct
    # the check is skipped here and the restricted user reaches the embedded locator unchecked.
    #
    # The outer locator is File and IS granted, so a denial naming READ ON S3 can only come from the
    # embedded S3 locator. `BACKUP DATABASE` serializes the locator as a string that
    # `BackupInfo::fromAST` rejects before any check, so the function form is written into the
    # manifest directly - the attacker-controlled-manifest shape this authorization exists for.
    # Every locator here is credential-free (File locally, a 1-argument S3 URL in the manifest): the
    # later definition-mismatch error logs both definitions, and a masked credential in that line
    # trips the tests-only `throw_on_match` masking rule and aborts the server.
    node.query("DROP DATABASE IF EXISTS dbembedded SYNC")
    node.query("BACKUP DATABASE d67785 TO File('inner10') FORMAT Null")
    node.query("CREATE DATABASE dbembedded ENGINE = Backup('d67785', File('inner10'))")
    node.query("BACKUP DATABASE dbembedded TO File('outer10') FORMAT Null")
    node.exec_in_container(
        [
            "bash",
            "-c",
            "printf \"CREATE DATABASE dbembedded ENGINE = Backup('d67785', "
            "S3('http://minio1:9001/root/data/denied/b10'))\\n\" "
            "> /var/lib/clickhouse/backups/outer10/metadata/dbembedded.sql",
        ],
        user="root",
    )

    # The target database exists on the initiator, which is what used to skip the check.
    assert node.query("SELECT count() FROM system.databases WHERE name = 'dbembedded'") == "1\n"
    node.query(f"GRANT READ ON FILE TO {USER}")

    error = node.query_and_get_error(
        "RESTORE DATABASE dbembedded ON CLUSTER one_shard FROM File('outer10')", user=USER
    )
    assert "ACCESS_DENIED" in error, error
    # Not the outer File locator, and not some other missing privilege.
    assert "READ ON S3" in error, error

    # With the grant, authorization passes: the restore proceeds past CHECKING_ACCESS_RIGHTS and
    # fails later on the pre-existing string-vs-function definition mismatch instead.
    node.query(f"GRANT READ ON S3 TO {USER}")
    error = node.query_and_get_error(
        "RESTORE DATABASE dbembedded ON CLUSTER one_shard FROM File('outer10')", user=USER
    )
    assert "ACCESS_DENIED" not in error, error
    assert "CANNOT_RESTORE_DATABASE" in error, error

    node.query("DROP DATABASE dbembedded SYNC")


def test_restore_on_cluster_of_a_real_backup_engine_manifest_is_unchanged(started_cluster):
    # The manifest is NOT crafted here, which is the point: `BACKUP DATABASE` serializes the inner
    # locator as a string, and authorizing it would parse it and reject it with BAD_ARGUMENTS before
    # any access decision. Only this shape can catch that, so it is a separate case from the crafted
    # one - which asserts the security property but cannot see this class.
    node.query("DROP DATABASE IF EXISTS dbreal SYNC")
    node.query("BACKUP DATABASE d67785 TO File('inner11') FORMAT Null")
    node.query("CREATE DATABASE dbreal ENGINE = Backup('d67785', File('inner11'))")
    node.query("BACKUP DATABASE dbreal TO File('outer11') FORMAT Null")
    manifest = node.exec_in_container(
        ["bash", "-c", "cat /var/lib/clickhouse/backups/outer11/metadata/dbreal.sql"],
        user="root",
    )
    # The locator really is the string form: an ASTLiteral, not an ASTFunction.
    assert "Backup('d67785', 'File(\\'inner11\\')')" in manifest, manifest

    # Only the outer locator's own grant; the string-form inner one must not be authorized at all.
    node.query(f"GRANT READ ON FILE TO {USER}")

    # Target exists, so nothing is created and the restore fully succeeds. Authorizing the string
    # form turns this into `Code: 36` out of CHECKING_ACCESS_RIGHTS.
    node.query(
        "RESTORE DATABASE dbreal ON CLUSTER one_shard FROM File('outer11') FORMAT Null", user=USER
    )

    # Target absent, so creation runs and rejects the string form - as it does without this feature.
    # `While creating database` is what pins the failure to the creation stage rather than the
    # access-check one, which is where the same code would report it.
    node.query("DROP DATABASE dbreal SYNC")
    error = node.query_and_get_error(
        "RESTORE DATABASE dbreal ON CLUSTER one_shard FROM File('outer11')", user=USER
    )
    assert "ACCESS_DENIED" not in error, error
    assert "BAD_ARGUMENTS" in error, error
    assert "While creating database" in error, error


def test_explicit_base_backup_locator_is_authorized_on_the_initiator(started_cluster):
    # A base backup opens lazily and a worker leg carries no user, so on RESTORE ON CLUSTER the
    # workers read the base while the initiator never opens it at all: only a preflight before
    # dispatch can authorize it. The outer locator is granted throughout, so a denial naming
    # READ ON S3 can only come from the base.
    #
    # The table comment makes the two manifests differ, which is load-bearing: with identical
    # metadata the initiator's own manifest write opens the base and denies for that reason
    # instead, so the case would pass without the preflight. Only the data file dedups, and that
    # read happens exclusively on the workers. Measured on a binary without the preflight: this
    # RESTORE succeeds and hands the user the base's rows.
    node.query("DROP TABLE IF EXISTS d67785.based SYNC")
    node.query(
        "CREATE TABLE d67785.based (x UInt64) ENGINE = MergeTree ORDER BY x "
        "COMMENT 'a comment long enough that dropping it shrinks the .sql file'"
    )
    node.query("INSERT INTO d67785.based VALUES (7)")
    node.query(f"BACKUP TABLE d67785.based TO {s3(DENIED + '/base12')} FORMAT Null")
    node.query("ALTER TABLE d67785.based MODIFY COMMENT ''")
    node.query(
        f"BACKUP TABLE d67785.based TO {s3(ALLOWED + '/outer12')} "
        f"SETTINGS base_backup={s3(DENIED + '/base12')} FORMAT Null"
    )

    node.query(f"GRANT SELECT, INSERT ON d67785.based TO {USER}")
    node.query(f"GRANT WRITE ON S3('{ALLOWED}/.*') TO {USER}")
    node.query(f"GRANT READ ON S3('{ALLOWED}/.*') TO {USER}")

    error = node.query_and_get_error(
        f"RESTORE TABLE d67785.based AS d67785.r12 ON CLUSTER one_shard "
        f"FROM {s3(ALLOWED + '/outer12')} SETTINGS base_backup={s3(DENIED + '/base12')}",
        user=USER,
    )
    assert "ACCESS_DENIED" in error, error
    assert "READ ON S3" in error, error
    assert (
        node.query("SELECT count() FROM system.tables WHERE database = 'd67785' AND name = 'r12'")
        == "0\n"
    )

    # An incremental BACKUP reads its base, so the base is READ even though the outer locator is
    # WRITE. A denial naming WRITE ON S3 would mean the base inherited the outer's direction.
    error = node.query_and_get_error(
        f"BACKUP TABLE d67785.based ON CLUSTER one_shard TO {s3(ALLOWED + '/inc12')} "
        f"SETTINGS base_backup={s3(DENIED + '/base12')}",
        user=USER,
    )
    assert "ACCESS_DENIED" in error, error
    assert "READ ON S3" in error, error

    node.query(f"GRANT READ ON S3('{DENIED}/.*') TO {USER}")
    node.query(
        f"BACKUP TABLE d67785.based ON CLUSTER one_shard TO {s3(ALLOWED + '/inc12')} "
        f"SETTINGS base_backup={s3(DENIED + '/base12')} FORMAT Null",
        user=USER,
    )
    node.query(
        f"RESTORE TABLE d67785.based AS d67785.r12 ON CLUSTER one_shard "
        f"FROM {s3(ALLOWED + '/outer12')} SETTINGS base_backup={s3(DENIED + '/base12')} FORMAT Null",
        user=USER,
    )
    # The row lives in the base: `outer12`'s data file deduplicated fully against it, so a correct
    # value here also proves the base really is read rather than skipped.
    assert node.query("SELECT x FROM d67785.r12") == "7\n"
    node.query("DROP TABLE d67785.r12 SYNC")
    node.query("DROP TABLE d67785.based SYNC")


def test_on_cluster_is_authorized_on_the_initiator(started_cluster):
    # A worker leg runs with no user by default, so the initiator must reject the query before
    # distributing it; and a granted user's ON CLUSTER backup must keep working.
    error = node.query_and_get_error(
        f"BACKUP TABLE d67785.secrets ON CLUSTER one_shard TO {s3(DENIED + '/b4')}", user=USER
    )
    assert "ACCESS_DENIED" in error, error
    # The denial must be the source check, not some other missing privilege.
    assert "WRITE ON S3" in error, error

    node.query(f"GRANT WRITE ON S3 TO {USER}")
    node.query(
        f"BACKUP TABLE d67785.secrets ON CLUSTER one_shard TO {s3(ALLOWED + '/b4')} FORMAT Null",
        user=USER,
    )
