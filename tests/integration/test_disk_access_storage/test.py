import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance', config_dir='configs', main_configs=['configs/access_control_path.xml'], stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def create_entities():
    instance.query("CREATE USER u1")
    instance.query("CREATE ROLE rx")
    instance.query("CREATE USER u2 IDENTIFIED BY 'qwerty' HOST LOCAL DEFAULT ROLE rx")
    instance.query("CREATE ROW POLICY p ON mydb.mytable FOR SELECT USING a<1000 TO u1, u2")
    instance.query("CREATE QUOTA q FOR INTERVAL 1 HOUR SET MAX QUERIES = 100 TO ALL EXCEPT rx")


@pytest.fixture(autouse=True)
def drop_entities():
    instance.query("DROP USER IF EXISTS u1, u2")    
    instance.query("DROP ROLE IF EXISTS rx, ry")    
    instance.query("DROP ROW POLICY IF EXISTS p ON mydb.mytable")
    instance.query("DROP QUOTA IF EXISTS q")
    

def test_create():
    create_entities()

    def check():
        assert instance.query("SHOW CREATE USER u1") == "CREATE USER u1\n"
        assert instance.query("SHOW CREATE USER u2") == "CREATE USER u2 HOST LOCAL DEFAULT ROLE rx\n"
        assert instance.query("SHOW CREATE ROW POLICY p ON mydb.mytable") == "CREATE POLICY p ON mydb.mytable FOR SELECT USING a < 1000 TO u1, u2\n"
        assert instance.query("SHOW CREATE QUOTA q") == "CREATE QUOTA q KEYED BY \\'none\\' FOR INTERVAL 1 HOUR MAX QUERIES = 100 TO ALL EXCEPT rx\n"
        assert instance.query("SHOW GRANTS FOR u1") == ""
        assert instance.query("SHOW GRANTS FOR u2") == "GRANT rx TO u2\n"
        assert instance.query("SHOW GRANTS FOR rx") == ""

    check()
    instance.restart_clickhouse()  # Check persistency
    check()


def test_alter():
    create_entities()
    instance.restart_clickhouse()

    instance.query("CREATE ROLE ry")
    instance.query("GRANT ry TO u2")
    instance.query("ALTER USER u2 DEFAULT ROLE ry")
    instance.query("GRANT rx TO ry WITH ADMIN OPTION")
    instance.query("GRANT SELECT ON mydb.mytable TO u1")
    instance.query("GRANT SELECT ON mydb.* TO rx WITH GRANT OPTION")

    def check():
        assert instance.query("SHOW CREATE USER u1") == "CREATE USER u1\n"
        assert instance.query("SHOW CREATE USER u2") == "CREATE USER u2 HOST LOCAL DEFAULT ROLE ry\n"
        assert instance.query("SHOW GRANTS FOR u1") == "GRANT SELECT ON mydb.mytable TO u1\n"
        assert instance.query("SHOW GRANTS FOR u2") == "GRANT rx, ry TO u2\n"
        assert instance.query("SHOW GRANTS FOR rx") == "GRANT SELECT ON mydb.* TO rx WITH GRANT OPTION\n"
        assert instance.query("SHOW GRANTS FOR ry") == "GRANT rx TO ry WITH ADMIN OPTION\n"

    check()
    instance.restart_clickhouse()  # Check persistency
    check()


def test_drop():
    create_entities()
    instance.restart_clickhouse()

    instance.query("DROP USER u2")
    instance.query("DROP ROLE rx")
    instance.query("DROP ROW POLICY p ON mydb.mytable")
    instance.query("DROP QUOTA q")

    def check():
        assert instance.query("SHOW CREATE USER u1") == "CREATE USER u1\n"
        assert "User `u2` not found" in instance.query_and_get_error("SHOW CREATE USER u2")
        assert "Row policy `p ON mydb.mytable` not found" in instance.query_and_get_error("SHOW CREATE ROW POLICY p ON mydb.mytable")
        assert "Quota `q` not found" in instance.query_and_get_error("SHOW CREATE QUOTA q")

    check()
    instance.restart_clickhouse()  # Check persistency
    check()
