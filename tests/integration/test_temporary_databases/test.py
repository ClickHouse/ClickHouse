import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
    with_remote_database_disk=False, # for test_restart(requires access to local disk)
)


def escape_test_name(s: str) -> str:
    return s.replace("[", "_").replace("]", "_").replace("=", "_").replace(",", "_").replace("-", "_")


def get_session_id(r, session_num: int) -> str:
    return f"{escape_test_name(r.node.name)}_session_{session_num}"


def get_db_name(r, db_num: int) -> str:
    return f"{escape_test_name(r.node.name)}_db_{db_num}"


def get_session_id_with_db_name(r, num: int) -> tuple[str, str]:
    return get_session_id(r, num), get_db_name(r, num)


def get_user(r, user_num: int) -> str:
    return f"{escape_test_name(r.node.name)}_user_{user_num}"


def query(session_id: str, sql: str, modifying: bool = True, params: dict = None, **kwargs):
    # we can't use node1.query because it uses the cli client that does not have session_id argument yet
    return node1.http_query(
        sql=sql,
        method="POST" if modifying else None,
        params={"session_id": session_id, "session_timeout": 3600} | (params or {}),
        **kwargs
    )


def assert_table_filled(session_id: str, db_name: str, table_name: str = "table1", expected_count: int = 10000,
                        **kwargs):
    assert (query(session_id, f"SELECT count() FROM `{db_name}`.`{table_name}`", modifying=False, **kwargs)
            == TSV([[expected_count]]))


def create_and_fill_table(session_id: str, db_name: str, table_name: str, **kwargs):
    query(session_id, f"CREATE TABLE `{db_name}`.`{table_name}` (x UInt64, x2 UInt64) ENGINE=MergeTree() ORDER BY x", **kwargs)
    query(session_id, f"INSERT INTO `{db_name}`.`{table_name}` SELECT rand(), rand() FROM numbers(10000)", **kwargs)
    assert_table_filled(session_id, db_name, table_name, **kwargs)


def create_db_with_table(session_id: str, db_name: str, table_name: str = "table1", **kwargs):
    query(session_id, f"CREATE TEMPORARY DATABASE `{db_name}`", **kwargs)
    create_and_fill_table(session_id, db_name, table_name, **kwargs)


def assert_db_does_not_exist(session_id: str, db_name: str, sql: str = "SELECT count() FROM `{}`.`table1`",
                             modifying: bool = False, **kwargs):
    with pytest.raises(Exception, match=f"Database {db_name} does not exist"):
        query(session_id, sql.replace("{}", db_name), modifying=modifying, **kwargs)


def get_dbs_from_system_table(session_id: str, names: list[str], show_others: bool, table: str = "databases", **kwargs):
    names = ", ".join(f"'{name}'" for name in names)
    return query(session_id,
                 f"SELECT database FROM system.`{table}` WHERE database in ({names})",
                 False,
                 {"show_temporary_databases_from_other_sessions_in_system_tables": 1 if show_others else 0},
                 **kwargs)


def drop_db(session_id: str, db_name: str, **kwargs):
    query(session_id, f"DROP DATABASE `{db_name}`", **kwargs)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        node1.query("CREATE USER default2")
        node1.query("GRANT CURRENT GRANTS ON *.* TO default2 WITH GRANT OPTION")

        yield cluster
    finally:
        cluster.shutdown()


def test_allow_experimental_temporary_databases(request):
    with pytest.raises(Exception, match="Temporary databases are experimental"):
        query(get_session_id(request, 1), f"CREATE TEMPORARY DATABASE anydb",
              params={"allow_experimental_temporary_databases": 0})


def test_attach_unsupported(request):
    session1, db1 = get_session_id_with_db_name(request, 1)
    with pytest.raises(Exception, match="Temporary databases or tables inside cannot be attached. Use CREATE instead"):
        query(session1, f"ATTACH TEMPORARY DATABASE `{db1}`")


def test_detach_unsupported(request):
    session1, db1 = get_session_id_with_db_name(request, 1)
    create_db_with_table(session1, db1)

    with pytest.raises(Exception, match="Temporary databases or tables inside cannot be detached. Use DROP instead"):
        query(session1, f"DETACH DATABASE `{db1}`")
    with pytest.raises(Exception, match="Temporary databases or tables inside cannot be detached. Use DROP instead"):
        query(session1, f"DETACH TABLE `{db1}`.`table1`")

    drop_db(session1, db1)


def test_unsupported_engines(request):
    session1, db1 = get_session_id_with_db_name(request, 1)

    with pytest.raises(Exception, match="Replicated databases cannot be temporary"):
        query(session1, f"CREATE TEMPORARY DATABASE `{db1}` ENGINE = Replicated('some/path/r', 'shard1', 'replica1')")
    with pytest.raises(Exception, match="Backup databases cannot be temporary"):
        query(session1, f"CREATE TEMPORARY DATABASE `{db1}` ENGINE = Backup('database_name_inside_backup', 'backup_destination')")


def test_on_cluster_unsupported(request):
    session1, db1 = get_session_id_with_db_name(request, 1)
    create_db_with_table(session1, db1)

    def assert_(sql: str):
        with pytest.raises(Exception, match=f"ON CLUSTER cannot be used with temporary databases or tables inside"):
            query(session1, sql)

    db2 = get_db_name(request, 2)
    assert_(f"CREATE TEMPORARY DATABASE `{db2}` ON CLUSTER default")
    assert_(f"ALTER DATABASE `{db1}` ON CLUSTER default MODIFY COMMENT 'test comment'")
    assert_(f"RENAME DATABASE `{db1}` TO `{db2}` ON CLUSTER default")
    assert_(f"TRUNCATE DATABASE `{db1}` ON CLUSTER default")
    assert_(f"DETACH DATABASE `{db1}` ON CLUSTER default")
    assert_(f"DROP DATABASE `{db1}` ON CLUSTER default")
    assert_(f"CREATE TABLE `{db1}`.`table2` ON CLUSTER default (x UInt64) ENGINE=MergeTree() ORDER BY x")
    assert_(f"ALTER TABLE `{db1}`.`table1` ON CLUSTER default ADD COLUMN y UInt64")
    assert_(f"ALTER TABLE `{db1}`.`table1` ON CLUSTER default MODIFY COLUMN y UInt64 COMMENT 'test comment'")
    assert_(f"ALTER TABLE `{db1}`.`table1` ON CLUSTER default RENAME COLUMN y TO y2")
    assert_(f"ALTER TABLE `{db1}`.`table1` ON CLUSTER default DROP COLUMN y2")
    assert_(f"ALTER TABLE `{db1}`.`table1` ON CLUSTER default UPDATE x2 = 1 WHERE x2 < 100")
    assert_(f"RENAME TABLE `{db1}`.`table1` TO `{db1}`.`table2` ON CLUSTER default")
    assert_(f"EXCHANGE TABLES `{db1}`.`table2` AND `{db1}`.`table3` ON CLUSTER default")
    assert_(f"DETACH TABLE `{db1}`.`table1` ON CLUSTER default")
    assert_(f"DROP TABLE `{db1}`.`table1` ON CLUSTER default")
    assert_(f"UNDROP TABLE `{db1}`.`table1` ON CLUSTER default")
    assert_(f"TRUNCATE TABLE `{db1}`.`table1` ON CLUSTER default")
    assert_(f"OPTIMIZE TABLE `{db1}`.`table1` ON CLUSTER default")

    assert_(f"UPDATE `{db1}`.`table1` ON CLUSTER default SET x2 = 1 WHERE x2 < 100")
    assert_(f"DELETE FROM `{db1}`.`table1` ON CLUSTER default WHERE x2 < 100")

    drop_db(session1, db1)


@pytest.mark.parametrize("user2", ["default", "default2"])
def test_inaccessible_from_other_sessions(request, user2: str):
    session1, db1 = get_session_id_with_db_name(request, 1)
    session2, db2 = get_session_id_with_db_name(request, 2)

    create_db_with_table(session1, db1)
    create_db_with_table(session2, db2, user=user2)

    assert_db_does_not_exist(session1, db2)
    assert_db_does_not_exist(session2, db1, user=user2)

    def assert_(sql: str, ddl: bool = True):
        assert_db_does_not_exist(session2, db1, sql, ddl, user=user2)

    assert_("ALTER DATABASE `{}` MODIFY COMMENT 'test comment'")
    assert_(f"RENAME DATABASE `{{}}` TO `{db2}`")
    assert_("TRUNCATE DATABASE `{}`")
    assert_("DETACH DATABASE `{}`")
    assert_("DROP DATABASE `{}`")
    assert_("CREATE TABLE `{}`.`table2` (x UInt64) ENGINE=MergeTree() ORDER BY x")
    assert_("ALTER TABLE `{}`.`table1` ADD COLUMN y UInt64")
    assert_("ALTER TABLE `{}`.`table1` MODIFY COLUMN y UInt64 COMMENT 'test comment'")
    assert_("ALTER TABLE `{}`.`table1` RENAME COLUMN y TO y2")
    assert_("ALTER TABLE `{}`.`table1` DROP COLUMN y2")
    assert_("ALTER TABLE `{}`.`table1` UPDATE x2 = 1 WHERE x2 < 100")
    assert_("RENAME TABLE `{}`.`table1` TO `{}`.`table2`")
    assert_("EXCHANGE TABLES `{}`.`table2` AND `{}`.`table3`")
    assert_("DETACH TABLE `{}`.`table1`")
    assert_("DROP TABLE `{}`.`table1`")
    assert_("UNDROP TABLE `{}`.`table1`")
    assert_("TRUNCATE TABLE `{}`.`table1`")
    assert_("OPTIMIZE TABLE `{}`.`table1`")

    assert_("SELECT count() FROM `{}`.`table1`", False)
    assert_("INSERT INTO `{}`.`table1` SELECT number FROM numbers(10000)")
    assert_("UPDATE `{}`.`table1` SET x2 = 1 WHERE x2 < 100")
    assert_("DELETE FROM `{}`.`table1` WHERE x2 < 100")

    drop_db(session1, db1)
    drop_db(session2, db2, user=user2)


def test_database_already_exists(request):
    session1, db1 = get_session_id_with_db_name(request, 1)
    create_db_with_table(session1, db1)

    with pytest.raises(Exception, match=f"Temporary database {db1} already exists"):
        create_db_with_table(get_session_id(request, 2), db1)

    drop_db(session1, db1)


@pytest.mark.parametrize("table", ["databases", "tables"])
def test_system_tables(request, table: str):
    session1, db1 = get_session_id_with_db_name(request, 1)
    create_db_with_table(session1, db1)

    assert get_dbs_from_system_table(session1, [db1], False, table=table) == TSV([[db1]])
    assert get_dbs_from_system_table(session1, [db1], True, table=table) == TSV([[db1]])

    session2 = get_session_id(request, 2)
    assert get_dbs_from_system_table(session2, [db1], False, table=table) == TSV([])
    assert get_dbs_from_system_table(session2, [db1], True, table=table) == TSV([[db1]])

    drop_db(session1, db1)


def test_access_rights(request):
    session1 = get_session_id(request, 1)

    session2, db2 = get_session_id_with_db_name(request, 2)
    user2 = get_user(request, 2)

    session3, db3 = get_session_id_with_db_name(request, 3)
    user3 = get_user(request, 3)

    query(session1, f"CREATE USER {user2}")
    query(session1, f"GRANT CREATE DATABASE ON `{db2}`.* TO {user2}")
    query(session1, f"GRANT DROP DATABASE ON `{db2}`.* TO {user2}")
    query(session1, f"CREATE USER {user3}")
    query(session1, f"GRANT CREATE TEMPORARY DATABASE ON `{db3}`.* TO {user3}")
    query(session1, f"GRANT DROP DATABASE ON `{db3}`.* TO {user3}")

    query(session2, f"CREATE TEMPORARY DATABASE `{db2}`", user=user2)
    drop_db(session2, db2, user=user2)
    query(session2, f"CREATE DATABASE `{db2}`", user=user2)
    drop_db(session2, db2, user=user2)

    with pytest.raises(Exception, match=f"To execute this query, it's necessary to have the grant CREATE TEMPORARY DATABASE ON {db3}.*"):
        query(session2, f"CREATE TEMPORARY DATABASE `{db3}`", user=user2)
    with pytest.raises(Exception, match=f"To execute this query, it's necessary to have the grant CREATE DATABASE ON {db3}.*"):
        query(session2, f"CREATE DATABASE `{db3}`", user=user2)


    query(session3, f"CREATE TEMPORARY DATABASE `{db3}`", user=user3)
    drop_db(session3, db3, user=user3)

    with pytest.raises(Exception, match=f"To execute this query, it's necessary to have the grant CREATE DATABASE ON {db3}.*"):
        query(session3, f"CREATE DATABASE `{db3}`", user=user3)
    with pytest.raises(Exception, match=f"To execute this query, it's necessary to have the grant CREATE TEMPORARY DATABASE ON {db2}.*"):
        query(session3, f"CREATE TEMPORARY DATABASE `{db2}`", user=user3)
    with pytest.raises(Exception, match=f"To execute this query, it's necessary to have the grant CREATE DATABASE ON {db2}.*"):
        query(session3, f"CREATE DATABASE `{db2}`", user=user3)


    query(session1, f"DROP USER {user2}")
    query(session1, f"DROP USER {user3}")


def test_rename(request):
    session1, db1 = get_session_id_with_db_name(request, 1)
    session2, db2 = get_session_id_with_db_name(request, 2)
    create_db_with_table(session1, db1)

    query(session1, f"RENAME DATABASE `{db1}` TO `{db2}`")

    assert_db_does_not_exist(session1, db1)
    assert_db_does_not_exist(session2, db1)
    assert get_dbs_from_system_table(session1, [db1, db2], True) == TSV([[db2]])

    assert_table_filled(session1, db2)

    drop_db(session1, db2)


def test_set_comment(request):
    session1, db1 = get_session_id_with_db_name(request, 1)
    create_db_with_table(session1, db1)

    query(session1, f"ALTER DATABASE `{db1}` MODIFY COMMENT 'test comment'")
    assert query(session1, f"SELECT comment FROM system.databases WHERE name = '{db1}'", False) == TSV(
        [["test comment"]])

    drop_db(session1, db1)

def test_create_if_not_exists(request):
    session1, db1 = get_session_id_with_db_name(request, 1)
    create_db_with_table(session1, db1)

    query(session1, f"CREATE DATABASE IF NOT EXISTS `{db1}`")
    with pytest.raises(Exception, match=f"Temporary database {db1} already exists in other session"):
        query(get_session_id(request, 2), f"CREATE DATABASE IF NOT EXISTS `{db1}`")

    drop_db(session1, db1)


def test_drop_if_exists(request):
    session1, db1 = get_session_id_with_db_name(request, 1)
    session2, db2 = get_session_id_with_db_name(request, 2)
    create_db_with_table(session1, db1)

    query(session1, f"DROP DATABASE IF EXISTS `{db1}`")
    assert_db_does_not_exist(session1, db1)
    assert_db_does_not_exist(session2, db1)

    query(session1, f"DROP DATABASE IF EXISTS `{db2}`")
    query(session2, f"DROP DATABASE IF EXISTS `{db2}`")


def test_database_hints(request):
    session1, db1 = get_session_id_with_db_name(request, 1)
    create_db_with_table(session1, db1)

    with pytest.raises(Exception, match=f"Database {db1}_no does not exist. Maybe you meant {db1}?"):
        query(session1, f"SELECT count() FROM `{db1}_no`.`table1`", False)
    with pytest.raises(Exception, match=rf"Database {db1}_no does not exist\. \(UNKNOWN_DATABASE\)"):
        query(get_session_id(request, 2), f"SELECT count() FROM `{db1}_no`.`table1`", False)

    drop_db(session1, db1)


def test_table_hints(request):
    session1, db1 = get_session_id_with_db_name(request, 1)
    create_db_with_table(session1, db1)

    with pytest.raises(Exception, match=f"Table {db1}.table1_no does not exist. Maybe you meant {db1}.table1?"):
        query(session1, f"DROP TABLE `{db1}`.`table1_no`")
    with pytest.raises(Exception, match=rf"Database {db1} does not exist\. \(UNKNOWN_DATABASE\)"):
        query(get_session_id(request, 2), f"DROP TABLE `{db1}`.`table1_no`")

    drop_db(session1, db1)


def test_undrop_table(request):
    session1, db1 = get_session_id_with_db_name(request, 1)
    create_db_with_table(session1, db1)

    query(session1, f"DROP TABLE `{db1}`.`table1`")
    with pytest.raises(Exception, match=f"Unknown table expression identifier '{db1}.table1'"):
        assert_table_filled(session1, db1)

    query(session1, f"UNDROP TABLE `{db1}`.`table1`")
    assert_table_filled(session1, db1)

    drop_db(session1, db1)


def test_mergetree(request):
    session1, db1 = get_session_id_with_db_name(request, 1)
    query(session1, f"CREATE TEMPORARY DATABASE `{db1}`")
    query(session1, f"CREATE TABLE `{db1}`.`table1` (x UInt64, v String) ENGINE=MergeTree() ORDER BY x "
                    f"SETTINGS enable_block_number_column=1, enable_block_offset_column=1")

    query(session1, f"INSERT INTO `{db1}`.`table1` VALUES (1, 'a'), (2, 'b')")
    query(session1, f"UPDATE `{db1}`.`table1` SET v = 'c' WHERE x = 1")
    assert query(session1, f"SELECT * FROM `{db1}`.`table1` ORDER BY x", False) == TSV([[1, 'c'], [2, 'b']])

    query(session1, f"DELETE FROM `{db1}`.`table1` WHERE x = 1")
    assert query(session1, f"SELECT * FROM `{db1}`.`table1` ORDER BY x", False) == TSV([[2, 'b']])

    drop_db(session1, db1)


@pytest.mark.parametrize("user2", ["default_same_session", "default", "default2"])
def test_excluded_from_backups(request, user2: str):
    session1, db1 = get_session_id_with_db_name(request, 1)
    session2, db2 = get_session_id_with_db_name(request, 2)

    create_db_with_table(session1, db1)
    query(session1, f"CREATE DATABASE `{db2}`")

    file = f"backups/{escape_test_name(request.node.name)}.zip"
    if user2 == "default_same_session":
        user2 = "default"
        session2 = session1


    def assert_(sql: str):
        msg = f"Temporary database '{db1}' cannot be backed up." if session1 == session2 else f"Database {db1} does not exist"
        with pytest.raises(Exception, match=msg):
            query(session2, sql, user=user2)

    assert_(f"BACKUP DATABASE `{db1}` TO File('{file}')")
    assert_(f"BACKUP DATABASE `{db1}`, DATABASE `{db2}` TO File('{file}')")
    assert_(f"BACKUP TABLE `{db1}`.table1 TO File('{file}')")
    assert_(f"BACKUP TABLE `{db1}`.table1, TABLE `{db2}`.table1 TO File('{file}')")
    assert_(f"BACKUP TABLE `{db2}`.table1, DATABASE `{db1}` TO File('{file}')")


    query(session2, f"BACKUP ALL EXCEPT DATABASE system TO File('{file}')", user=user2)
    drop_db(session1, db1)
    drop_db(session2, db2, user=user2)
    query(session2, f"RESTORE ALL FROM File('{file}')", user=user2)
    assert get_dbs_from_system_table(session2, [db1, db2], True, user=user2) == TSV([[db2]])
    drop_db(session2, db2, user=user2)


def test_session_closed_cleanup(request):
    session1, db1 = get_session_id_with_db_name(request, 1)
    create_db_with_table(session1, db1, params={"session_timeout": 1})

    session2 = get_session_id(request, 2)
    timeout = time.monotonic() + 60  # it supposed to be done within a few seconds, but CI runners are slow sometimes
    while time.monotonic() < timeout:
        time.sleep(1)
        if get_dbs_from_system_table(session2, [db1], True) == TSV([]):
            break
    else:
        pytest.fail("Database was not dropped after session timeout")


@pytest.mark.parametrize("kill", [False, True], ids=["kill=False", "kill=True"])
def test_restart(request, kill: bool):
    session1, db1 = get_session_id_with_db_name(request, 1)
    create_db_with_table(session1, db1)

    db2 = get_db_name(request, 2)
    query(session1, f"CREATE DATABASE `{db2}`")

    metadata_file = f"/var/lib/clickhouse/temporary/metadata/{db1}.sql"

    assert node1.file_exists_in_container(metadata_file)
    node1.stop_clickhouse(kill=kill)
    assert node1.file_exists_in_container(metadata_file)

    node1.start_clickhouse()
    assert not node1.file_exists_in_container(metadata_file)
    assert_db_does_not_exist(session1, db1)

    assert get_dbs_from_system_table(session1, [db1, db2], True) == TSV([[db2]])
    drop_db(session1, db2)
