import time
import uuid

import pymysql.cursors
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import mysql_pass, pg_pass
from helpers.postgres_utility import get_postgres_conn

# The MySQL and PostgreSQL database engines record "this table is detached/dropped" as the
# existence of an empty marker file in the database metadata directory. The marker mutation
# must fsync its parent directory (under fsync_metadata), otherwise an acknowledged
# DETACH/DROP/ATTACH can be lost on power loss and the table silently changes visibility on
# restart. We cannot cut power in a test, so assert the DirectorySync ProfileEvent fires for
# the DDL query (>= 1 with fsync_metadata=1, 0 with fsync_metadata=0), the same technique as
# 02361_fsync_profile_events.sh.

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_mysql8=True,
    with_postgres=True,
    # An object-storage database disk does not fsync directories (DiskObjectStorage does not
    # override getDirectorySyncGuard), so the DirectorySync oracle is meaningless there.
    with_remote_database_disk=False,
)

# `DatabasePostgreSQL::removeOutdatedTables` is a background task with no query context, so it
# reads `fsync_metadata` from the global context. That value comes from the `default` profile
# once at startup (`Context::setDefaultProfiles`), which a per-query setting cannot reach, so
# the disabled case needs a second instance. It shares the PostgreSQL container with `node`.
node_no_fsync = cluster.add_instance(
    "node_no_fsync",
    with_postgres=True,
    with_remote_database_disk=False,
    user_configs=["configs/no_fsync_metadata.xml"],
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


def global_directory_sync(instance=node):
    """Process-wide `DirectorySync` counter.

    The background cleaner task runs with no `query_id`, so `system.query_log` has no row for
    it and the per-query oracle above cannot observe it. `system.events` is the only counter
    that covers it; it is process-global, so only use it for a delta around a step that is
    known to be the sole source of directory syncs at that moment.
    """
    return int(
        instance.query(
            "SELECT value FROM system.events WHERE event = 'DirectorySync'"
        ).strip()
        or 0
    )


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


def wait_directory_sync(before, expected, instance=node):
    """Wait for the process-global `DirectorySync` delta to reach `expected`, then return it.

    The marker disappearing only observes the unlink; the guard is destroyed, and so increments
    the counter, after the loop the unlink runs in. Reading the counter as soon as the marker is
    gone would therefore race with the end of the pass.
    """
    for _ in range(120):
        delta = global_directory_sync(instance) - before
        if delta >= expected:
            break
        time.sleep(0.5)
    return global_directory_sync(instance) - before


def set_immutable(ch_db, immutable, instance=node):
    """Make the metadata directory reject `unlink`, so the marker removal throws `EPERM`.

    This is the only fault this harness can inject into the marker-removal path, and it hits
    exactly the step whose failure the statement order has to survive.
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

        # The marker is created by DETACH TABLE ... PERMANENTLY and removed by ATTACH TABLE.
        assert run(f"DETACH TABLE {ch_db}.t PERMANENTLY", 1) >= 1
        assert "t" not in node.query(f"SHOW TABLES FROM {ch_db}")
        assert run(f"ATTACH TABLE {ch_db}.t", 1) >= 1
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")

        assert run(f"DETACH TABLE {ch_db}.t PERMANENTLY", 0) == 0
        assert "t" not in node.query(f"SHOW TABLES FROM {ch_db}")
        assert run(f"ATTACH TABLE {ch_db}.t", 0) == 0
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")

        # A plain (non-permanent) DETACH writes no marker, so the ATTACH unlink is a no-op
        # and the guard is deliberately skipped even with fsync_metadata = 1.
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

        # DatabasePostgreSQL does not override detachTablePermanently (IDatabase throws
        # NOT_IMPLEMENTED), so its marker is created by DROP TABLE and removed by ATTACH TABLE.
        assert run(f"DROP TABLE {ch_db}.t", 1) >= 1
        assert "t" not in node.query(f"SHOW TABLES FROM {ch_db}")
        assert run(f"ATTACH TABLE {ch_db}.t", 1) >= 1
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")

        assert run(f"DROP TABLE {ch_db}.t", 0) == 0
        assert "t" not in node.query(f"SHOW TABLES FROM {ch_db}")
        assert run(f"ATTACH TABLE {ch_db}.t", 0) == 0
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")

        # A plain (non-permanent) DETACH writes no marker, so the ATTACH unlink is a no-op
        # and the guard is deliberately skipped even with fsync_metadata = 1.
        node.query(f"DETACH TABLE {ch_db}.t")
        assert "t" not in node.query(f"SHOW TABLES FROM {ch_db}")
        assert run(f"ATTACH TABLE {ch_db}.t", 1) == 0
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")
    finally:
        node.query(f"DROP DATABASE IF EXISTS {ch_db}")
        cursor.execute("DROP TABLE IF EXISTS t")
        conn.close()


def test_postgresql_cleaner_marker_removal(started_cluster):
    """The background cleaner also unlinks the marker, and that unlink must be fsynced.

    `DatabasePostgreSQL::removeOutdatedTables` drops the marker for any detached/dropped name
    that no longer exists in PostgreSQL, and erases the name from its in-memory set in the
    same pass, so a marker restored by a power loss is never removed again: the table is back
    in `actual_tables` and every later pass takes the `else` branch. The stale marker then
    hides a table that really exists.

    The cleaner has no `query_id`, so this test drives the state transition and takes the
    process-global `DirectorySync` delta across the pass instead of the per-query oracle.
    """
    suffix = uuid.uuid4().hex[:8]
    pg_db = f"pg_cleaner_{suffix}"
    ch_db = f"ch_pg_cleaner_{suffix}"

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
        assert marker_files(ch_db) == []

        node.query(f"DROP TABLE {ch_db}.t")
        assert marker_files(ch_db) == ["t.removed"]

        # `DETACH DATABASE` deactivates the cleaner, so detach before dropping the table in
        # PostgreSQL: otherwise the pass scheduled at `CREATE DATABASE` is still active and could
        # remove the marker before the measured one. `ATTACH DATABASE` then re-runs
        # `loadStoredObjects`, whose `activateAndSchedule` schedules the cleaner with no delay, so
        # the pass runs at once, not after the 60s period.
        node.query(f"DETACH DATABASE {ch_db}")

        # Drop it in PostgreSQL too, so the name is absent from the cleaner's `actual_tables`
        # and its removal branch becomes reachable for it.
        cursor.execute("DROP TABLE t")

        before = global_directory_sync()
        node.query(f"ATTACH DATABASE {ch_db}")

        for _ in range(120):
            if marker_files(ch_db) == []:
                break
            time.sleep(0.5)
        assert marker_files(ch_db) == []

        # The unlink was fsynced. Nothing else touches a metadata directory here, so the
        # counter can only have moved because the cleaner took the guard.
        assert wait_directory_sync(before, 1) >= 1

        # A pass with nothing to remove must not fsync at all: the guard is created lazily,
        # only once a marker is actually about to be unlinked.
        node.query(f"DETACH DATABASE {ch_db}")
        before_empty = global_directory_sync()
        node.query(f"ATTACH DATABASE {ch_db}")
        time.sleep(3)
        assert global_directory_sync() == before_empty

        # The cleaner really cleared the detached state: the table is visible again once it
        # comes back in PostgreSQL, which a lost unlink would have prevented for good.
        cursor.execute("CREATE TABLE t (id Integer NOT NULL, PRIMARY KEY (id))")
        assert "t" in node.query(f"SHOW TABLES FROM {ch_db}")
    finally:
        node.query(f"DROP DATABASE IF EXISTS {ch_db}")
        cursor.execute("DROP TABLE IF EXISTS t")
        conn.close()


def test_postgresql_cleaner_many_markers_sync_once(started_cluster):
    """One directory sync per cleaner pass, however many markers it removes.

    The guard is created on the first removal and reused (`!dir_sync_guard`) for the rest of the
    pass, so a pass that unlinks three markers must still fsync exactly once.
    """
    suffix = uuid.uuid4().hex[:8]
    pg_db = f"pg_many_{suffix}"
    ch_db = f"ch_pg_many_{suffix}"
    tables = ["t1", "t2", "t3"]

    conn, cursor = make_postgres_database(started_cluster, pg_db, tables)

    node.query(
        f"CREATE DATABASE {ch_db} ENGINE = PostgreSQL('postgres1:5432', '{pg_db}', 'postgres', '{pg_pass}')"
    )
    try:
        for table in tables:
            node.query(f"DROP TABLE {ch_db}.{table}")
        assert marker_files(ch_db) == ["t1.removed", "t2.removed", "t3.removed"]

        # Detach first, so the still-active cleaner cannot remove a marker before the measured
        # pass; only then make the names absent from PostgreSQL.
        node.query(f"DETACH DATABASE {ch_db}")

        for table in tables:
            cursor.execute(f"DROP TABLE {table}")

        before = global_directory_sync()
        node.query(f"ATTACH DATABASE {ch_db}")

        for _ in range(120):
            if marker_files(ch_db) == []:
                break
            time.sleep(0.5)
        assert marker_files(ch_db) == []

        # Exactly one, not one per marker: the guard is taken once and destroyed after the loop.
        assert wait_directory_sync(before, 1) == 1
    finally:
        node.query(f"DROP DATABASE IF EXISTS {ch_db}")
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def test_postgresql_cleaner_markerless_entry_does_not_sync(started_cluster):
    """A detached name with no marker on disk must not make the cleaner fsync.

    A plain (non-permanent) `DETACH TABLE` records the name in `detached_or_dropped` and writes
    no marker (`DatabasePostgreSQL::detachTable` touches no disk). If that table then disappears
    from PostgreSQL, the cleaner reaches its removal branch for a name whose marker does not
    exist, and the `existsFile` term is what keeps it from taking a pointless guard.

    `DETACH DATABASE` cannot be the fast trigger here: it would discard the in-memory
    plain-detach state this case is about. The cleaner's own 60s period is the only trigger, so
    this is the one long wait in the module. It cannot be turned into a plain poll either: a
    pass is observable only through the name leaving the detached set, and that is observable
    only once the table is back in PostgreSQL, which a pass that has not run yet would read as
    "still there" and skip. So wait out a period first, then probe, and if the probe lost the
    race put the table back in the dropped state and wait for the next period.
    """
    suffix = uuid.uuid4().hex[:8]
    pg_db = f"pg_markerless_{suffix}"
    ch_db = f"ch_pg_markerless_{suffix}"

    conn, cursor = make_postgres_database(started_cluster, pg_db, ["t"])

    # `CREATE DATABASE` runs `loadStoredObjects`, which schedules the cleaner immediately; that
    # first (empty) pass then reschedules itself 60s later.
    node.query(
        f"CREATE DATABASE {ch_db} ENGINE = PostgreSQL('postgres1:5432', '{pg_db}', 'postgres', '{pg_pass}')"
    )
    deadline = time.monotonic() + 75
    try:
        node.query(f"DETACH TABLE {ch_db}.t")
        assert marker_files(ch_db) == []

        # The baseline precedes the remote drop, so no pass can become eligible before it.
        before = global_directory_sync()
        cursor.execute("DROP TABLE t")

        cleaned = False
        for _ in range(4):
            while time.monotonic() < deadline:
                time.sleep(1)
            cursor.execute("CREATE TABLE t (id Integer NOT NULL, PRIMARY KEY (id))")
            if "t" in node.query(f"SHOW TABLES FROM {ch_db}"):
                cleaned = True
                break
            cursor.execute("DROP TABLE t")
            deadline = time.monotonic() + 65

        # The table is back in PostgreSQL and visible again, which only a pass that reached the
        # removal branch for the markerless name can produce.
        assert cleaned

        # There was no marker to unlink, so nothing had to be made durable.
        assert global_directory_sync() == before
    finally:
        node.query(f"DROP DATABASE IF EXISTS {ch_db}")
        cursor.execute("DROP TABLE IF EXISTS t")
        conn.close()


def test_postgresql_cleaner_marker_removal_no_fsync(started_cluster):
    """With `fsync_metadata` disabled the cleaner unlinks the marker without any directory sync.

    The setting is read from the global context, so it comes from this instance's `default`
    profile; a per-query setting provably cannot reach a background task.
    """
    suffix = uuid.uuid4().hex[:8]
    pg_db = f"pg_cleaner_nofsync_{suffix}"
    ch_db = f"ch_pg_cleaner_nofsync_{suffix}"

    conn, cursor = make_postgres_database(started_cluster, pg_db, ["t"])

    node_no_fsync.query(
        f"CREATE DATABASE {ch_db} ENGINE = PostgreSQL('postgres1:5432', '{pg_db}', 'postgres', '{pg_pass}')"
    )
    try:
        node_no_fsync.query(f"DROP TABLE {ch_db}.t")
        assert marker_files(ch_db, node_no_fsync) == ["t.removed"]

        # Detach first, so the still-active cleaner cannot remove the marker before the measured
        # pass; only then make the name absent from PostgreSQL.
        node_no_fsync.query(f"DETACH DATABASE {ch_db}")

        cursor.execute("DROP TABLE t")

        before = global_directory_sync(node_no_fsync)
        node_no_fsync.query(f"ATTACH DATABASE {ch_db}")

        # The unlink itself is not gated on the setting, only its durability is.
        for _ in range(120):
            if marker_files(ch_db, node_no_fsync) == []:
                break
            time.sleep(0.5)
        assert marker_files(ch_db, node_no_fsync) == []

        assert global_directory_sync(node_no_fsync) == before
    finally:
        node_no_fsync.query(f"DROP DATABASE IF EXISTS {ch_db}")
        cursor.execute("DROP TABLE IF EXISTS t")
        conn.close()


# The marker removal can fail (the guard opens the metadata directory, the `unlink` itself can
# return `EPERM`, `EACCES` or `EIO`), so the in-memory "no longer detached" state must be
# published only after the marker is gone. Otherwise a failed `ATTACH TABLE` leaves the table
# visible in memory with the marker still on disk: the retry reports `TABLE_ALREADY_EXISTS` and
# a restart hides the table again. `chattr +i` on the metadata directory makes the `unlink` fail
# deterministically.


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
