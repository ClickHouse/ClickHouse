import uuid

import pymysql.cursors
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import mysql_pass, pg_pass
from helpers.postgres_utility import get_postgres_conn

# The `MySQL` and `PostgreSQL` database engines record "this table is detached/dropped" as the
# existence of an empty marker file in the database metadata directory. The marker mutation
# must fsync its parent directory (under `fsync_metadata`), otherwise an acknowledged
# `DETACH`/`DROP`/`ATTACH` can be lost on power loss and the table silently changes visibility
# on restart. We cannot cut power in a test, so assert the `DirectorySync` ProfileEvent fires
# for the DDL query (`>= 1` with `fsync_metadata = 1`, `0` with `fsync_metadata = 0`), the same
# technique as `02361_fsync_profile_events.sh`.
#
# The counter is incremented before the syscall and `LocalDirectorySyncGuard` logs and swallows
# a failure, so it proves the sync is issued on the path, not that it returned success.
# `DirectorySyncElapsedMicroseconds` would discriminate that, but it is incremented by a
# truncated microsecond count that is `0` for a fast sync, which is why
# `02361_fsync_profile_events.sh` retries up to 100 times; a DDL statement cannot be retried
# the same way.

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_mysql8=True,
    with_postgres=True,
    # An object-storage database disk does not fsync directories (`DiskObjectStorage` does not
    # override `getDirectorySyncGuard`), so the `DirectorySync` oracle is meaningless there.
    with_remote_database_disk=False,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def directory_sync(query_id, instance=node):
    instance.query("SYSTEM FLUSH LOGS query_log")
    return int(instance.query(f"""
            SELECT ProfileEvents['DirectorySync']
            FROM system.query_log
            WHERE query_id = '{query_id}' AND type = 'QueryFinish'
            ORDER BY event_time_microseconds DESC
            LIMIT 1""").strip())


def run(query, fsync_metadata, instance=node):
    query_id = str(uuid.uuid4())
    instance.query(
        query, query_id=query_id, settings={"fsync_metadata": fsync_metadata}
    )
    return directory_sync(query_id, instance)


def metadata_dir(ch_db, instance=node):
    metadata_path = instance.query(
        f"SELECT metadata_path FROM system.databases WHERE name = '{ch_db}'"
    ).strip()
    return f"/var/lib/clickhouse/{metadata_path}"


def marker_files(ch_db, instance=node):
    """Names of the engine's `.removed` / `.remove_flag` markers on disk."""
    listing = instance.exec_in_container(
        ["ls", "-A", metadata_dir(ch_db, instance)],
        privileged=True,
        user="root",
    )
    return sorted(name for name in listing.split() if name)


def set_immutable(ch_db, immutable, instance=node):
    """Make the metadata directory reject `unlink` and `create`, so a marker mutation throws
    `EPERM`.

    This is the only fault this harness can inject into the marker path, and it hits exactly
    the step whose failure the statement order has to survive.
    """
    flag = "+i" if immutable else "-i"
    instance.exec_in_container(
        ["chattr", flag, metadata_dir(ch_db, instance)],
        privileged=True,
        user="root",
    )


def make_postgres_database(started_cluster, pg_db, tables):
    """Create `pg_db` in PostgreSQL with the given tables and return an open cursor to it."""
    conn = get_postgres_conn(started_cluster.postgres_ip, started_cluster.postgres_port)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE {pg_db}")
    conn.close()

    conn = get_postgres_conn(
        started_cluster.postgres_ip,
        started_cluster.postgres_port,
        database=True,
        database_name=pg_db,
    )
    cursor = conn.cursor()
    for table in tables:
        cursor.execute(f"CREATE TABLE {table} (id Integer NOT NULL, PRIMARY KEY (id))")
    return conn, cursor


def mysql_query(started_cluster, query):
    connection = pymysql.connect(
        user="root",
        password=mysql_pass,
        host=started_cluster.mysql8_ip,
        port=started_cluster.mysql8_port,
        autocommit=True,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
    finally:
        connection.close()


def test_mysql_database_marker_fsync(started_cluster):
    suffix = uuid.uuid4().hex[:8]
    mysql_db = f"mysql_fsync_{suffix}"
    ch_db = f"ch_mysql_fsync_{suffix}"

    mysql_query(started_cluster, f"DROP DATABASE IF EXISTS {mysql_db}")
    mysql_query(started_cluster, f"CREATE DATABASE {mysql_db}")
    mysql_query(
        started_cluster,
        f"CREATE TABLE `{mysql_db}`.`t` (`id` int(11) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB",
    )

    node.query(
        f"CREATE DATABASE {ch_db} ENGINE = MySQL('mysql80:3306', '{mysql_db}', 'root', '{mysql_pass}')"
    )
    try:
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")

        # The marker is created by `DETACH TABLE ... PERMANENTLY` and removed by `ATTACH TABLE`.
        assert run(f"DETACH TABLE {ch_db}.t PERMANENTLY", 1) >= 1
        assert "t" not in node.query(f"SHOW TABLES FROM {ch_db}")
        assert run(f"ATTACH TABLE {ch_db}.t", 1) >= 1
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")

        assert run(f"DETACH TABLE {ch_db}.t PERMANENTLY", 0) == 0
        assert "t" not in node.query(f"SHOW TABLES FROM {ch_db}")
        assert run(f"ATTACH TABLE {ch_db}.t", 0) == 0
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")

        # A plain (non-permanent) `DETACH` writes no marker, so the `ATTACH` unlink is a
        # no-op and the guard is deliberately skipped even with `fsync_metadata = 1`.
        node.query(f"DETACH TABLE {ch_db}.t")
        assert "t" not in node.query(f"SHOW TABLES FROM {ch_db}")
        assert run(f"ATTACH TABLE {ch_db}.t", 1) == 0
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")
    finally:
        node.query(f"DROP DATABASE IF EXISTS {ch_db}")
        mysql_query(started_cluster, f"DROP DATABASE IF EXISTS {mysql_db}")


def test_postgresql_database_marker_fsync(started_cluster):
    suffix = uuid.uuid4().hex[:8]
    pg_db = f"pg_fsync_{suffix}"
    ch_db = f"ch_pg_fsync_{suffix}"

    conn = get_postgres_conn(started_cluster.postgres_ip, started_cluster.postgres_port)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE {pg_db}")
    conn.close()

    conn = get_postgres_conn(
        started_cluster.postgres_ip,
        started_cluster.postgres_port,
        database=True,
        database_name=pg_db,
    )
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE t (id Integer NOT NULL, PRIMARY KEY (id))")

    node.query(
        f"CREATE DATABASE {ch_db} ENGINE = PostgreSQL('postgres1:5432', '{pg_db}', 'postgres', '{pg_pass}')"
    )
    try:
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")

        # The marker is created by `DROP TABLE` and removed by `ATTACH TABLE`.
        assert run(f"DROP TABLE {ch_db}.t", 1) >= 1
        assert "t" not in node.query(f"SHOW TABLES FROM {ch_db}")
        assert run(f"ATTACH TABLE {ch_db}.t", 1) >= 1
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")

        assert run(f"DROP TABLE {ch_db}.t", 0) == 0
        assert "t" not in node.query(f"SHOW TABLES FROM {ch_db}")
        assert run(f"ATTACH TABLE {ch_db}.t", 0) == 0
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")

        # `DETACH TABLE ... PERMANENTLY` writes the same marker from its own call site.
        assert run(f"DETACH TABLE {ch_db}.t PERMANENTLY", 1) >= 1
        node.query(f"ATTACH TABLE {ch_db}.t")
        assert run(f"DETACH TABLE {ch_db}.t PERMANENTLY", 0) == 0
        node.query(f"ATTACH TABLE {ch_db}.t")

        # A plain (non-permanent) `DETACH` writes no marker, so the `ATTACH` unlink is a
        # no-op and the guard is deliberately skipped even with `fsync_metadata = 1`.
        node.query(f"DETACH TABLE {ch_db}.t")
        assert "t" not in node.query(f"SHOW TABLES FROM {ch_db}")
        assert run(f"ATTACH TABLE {ch_db}.t", 1) == 0
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")
    finally:
        node.query(f"DROP DATABASE IF EXISTS {ch_db}")
        cursor.execute("DROP TABLE IF EXISTS t")
        conn.close()


def test_mysql_attach_failure_keeps_table_detached(started_cluster):
    suffix = uuid.uuid4().hex[:8]
    mysql_db = f"mysql_atomicity_{suffix}"
    ch_db = f"ch_mysql_atomicity_{suffix}"

    mysql_query(started_cluster, f"DROP DATABASE IF EXISTS {mysql_db}")
    mysql_query(started_cluster, f"CREATE DATABASE {mysql_db}")
    mysql_query(
        started_cluster,
        f"CREATE TABLE `{mysql_db}`.`t` (`id` int(11) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB",
    )

    node.query(
        f"CREATE DATABASE {ch_db} ENGINE = MySQL('mysql80:3306', '{mysql_db}', 'root', '{mysql_pass}')"
    )
    try:
        # A failed marker write leaves the table attached: the detached state is rolled back,
        # so the statement is retryable.
        set_immutable(ch_db, True)
        try:
            with pytest.raises(Exception):
                node.query(f"DETACH TABLE {ch_db}.t PERMANENTLY")
            assert marker_files(ch_db) == []
            assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")
        finally:
            set_immutable(ch_db, False)

        node.query(f"DETACH TABLE {ch_db}.t PERMANENTLY")
        assert marker_files(ch_db) == ["t.remove_flag"]

        set_immutable(ch_db, True)
        try:
            with pytest.raises(Exception):
                node.query(f"ATTACH TABLE {ch_db}.t")

            # The failed `ATTACH` changed nothing: the marker is still there and the table is
            # still detached, so the operation is retryable.
            assert marker_files(ch_db) == ["t.remove_flag"]
            assert "t" not in node.query(f"SHOW TABLES FROM {ch_db}")
        finally:
            set_immutable(ch_db, False)

        # Retrying after the fault is cleared succeeds.
        node.query(f"ATTACH TABLE {ch_db}.t")
        assert marker_files(ch_db) == []
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")
    finally:
        set_immutable(ch_db, False)
        node.query(f"DROP DATABASE IF EXISTS {ch_db}")
        mysql_query(started_cluster, f"DROP DATABASE IF EXISTS {mysql_db}")


def test_postgresql_attach_failure_keeps_table_detached(started_cluster):
    suffix = uuid.uuid4().hex[:8]
    pg_db = f"pg_atomicity_{suffix}"
    ch_db = f"ch_pg_atomicity_{suffix}"

    conn, cursor = make_postgres_database(started_cluster, pg_db, ["t"])

    node.query(
        f"CREATE DATABASE {ch_db} ENGINE = PostgreSQL('postgres1:5432', '{pg_db}', 'postgres', '{pg_pass}')"
    )
    try:
        # A failed marker write leaves the table attached: the detached state is rolled back,
        # so the statement is retryable. Both call sites that write the marker are covered.
        set_immutable(ch_db, True)
        try:
            for write in ("DROP TABLE", "DETACH TABLE"):
                stmt = f"{write} {ch_db}.t"
                if write == "DETACH TABLE":
                    stmt += " PERMANENTLY"
                with pytest.raises(Exception):
                    node.query(stmt)
                assert marker_files(ch_db) == []
                assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")
        finally:
            set_immutable(ch_db, False)

        node.query(f"DROP TABLE {ch_db}.t")
        assert marker_files(ch_db) == ["t.removed"]

        set_immutable(ch_db, True)
        try:
            with pytest.raises(Exception):
                node.query(f"ATTACH TABLE {ch_db}.t")

            assert marker_files(ch_db) == ["t.removed"]
            assert "t" not in node.query(f"SHOW TABLES FROM {ch_db}")
        finally:
            set_immutable(ch_db, False)

        node.query(f"ATTACH TABLE {ch_db}.t")
        assert marker_files(ch_db) == []
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")
    finally:
        set_immutable(ch_db, False)
        node.query(f"DROP DATABASE IF EXISTS {ch_db}")
        cursor.execute("DROP TABLE IF EXISTS t")
        conn.close()


def test_postgresql_attach_failure_rolls_back_table_cache(started_cluster):
    """A failed `ATTACH TABLE` must not leave its storage in the table cache.

    `getCreateTableQueryImpl` reads the cache without consulting the detached set, unlike
    `tryGetTable` and `getTablesIterator`, so a storage left behind by a failed attach would be
    served to later callers. With `use_table_cache = 1` a full-definition `ATTACH` supplies its
    own columns, which is what makes a poisoned entry observable: the column would survive.
    """
    suffix = uuid.uuid4().hex[:8]
    pg_db = f"pg_cache_rollback_{suffix}"
    ch_db = f"ch_pg_cache_rollback_{suffix}"

    conn, cursor = make_postgres_database(started_cluster, pg_db, ["t"])

    node.query(
        f"CREATE DATABASE {ch_db} ENGINE = PostgreSQL('postgres1:5432', '{pg_db}', 'postgres', "
        f"'{pg_pass}', '', 1)"
    )
    try:
        # The remote schema is the single column `id`; cache it, then drop the table so its
        # marker exists and the cache entry is erased again.
        assert "id" in node.query(f"SHOW CREATE TABLE {ch_db}.t")
        node.query(f"DROP TABLE {ch_db}.t")
        assert marker_files(ch_db) == ["t.removed"]

        set_immutable(ch_db, True)
        try:
            # A full-definition `ATTACH`: its `wrong_column` reaches the cache before the unlink.
            with pytest.raises(Exception):
                node.query(
                    f"ATTACH TABLE {ch_db}.t (wrong_column Int64) "
                    f"ENGINE = PostgreSQL('postgres1:5432', '{pg_db}', 't', 'postgres', '{pg_pass}')"
                )
            assert marker_files(ch_db) == ["t.removed"]
        finally:
            set_immutable(ch_db, False)

        # The failed definition must not have been cached: a short retry has to go back to
        # PostgreSQL for the schema.
        node.query(f"ATTACH TABLE {ch_db}.t")
        create = node.query(f"SHOW CREATE TABLE {ch_db}.t")
        assert "wrong_column" not in create
        assert "id" in create
    finally:
        set_immutable(ch_db, False)
        node.query(f"DROP DATABASE IF EXISTS {ch_db}")
        cursor.execute("DROP TABLE IF EXISTS t")
        conn.close()
