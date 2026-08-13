import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def setup_nodes():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

def test_parameterized_view_restart():
    instance.query("CREATE TABLE table_Catalog (Name String, Price UInt64, Quantity UInt64) ENGINE =MergeTree()")
    instance.query("INSERT INTO table_Catalog VALUES ('Pen', 10, 3)")
    instance.query("INSERT INTO table_Catalog VALUES ('Book', 20, 4)")
    instance.query("INSERT INTO table_Catalog VALUES ('Paper', 30, 5)")
    instance.query("CREATE VIEW `Catalog View` AS SELECT * FROM table_Catalog WHERE Name={name:String}")
    assert (instance.query("SELECT * FROM `Catalog View` (name = 'Pen')") == "Pen	10	3\n")
    instance.query("CREATE VIEW `Catalog View 2` AS SELECT * FROM `Catalog View`(name = {name:String})")
    assert (instance.query("SELECT * FROM `Catalog View 2` (name = 'Pen')") == "Pen	10	3\n")

    instance.restart_clickhouse()

    assert (instance.query("SELECT * FROM `Catalog View 2` (name = 'Pen')") == "Pen	10	3\n")
    instance.query("DROP VIEW `Catalog View 2`\n")
    instance.query("DROP VIEW `Catalog View`\n")
    instance.query("DROP TABLE table_Catalog\n")
