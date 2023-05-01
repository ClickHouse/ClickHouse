import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        instance.query("CREATE USER mira")
        instance.query("CREATE TABLE test_table(x Int32, y Int32) ENGINE=Log")
        instance.query("INSERT INTO test_table VALUES (5,6)")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def clear_after_test():
    try:
        yield
    finally:
        instance.query("CREATE USER OR REPLACE mira")
        instance.query("DROP DICTIONARY IF EXISTS test_dict")


create_query = """
    CREATE DICTIONARY test_dict(x Int32, y Int32) PRIMARY KEY x
    LAYOUT(FLAT())
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'test_table' DB 'default'))
    LIFETIME(0)
    """

drop_query = "DROP DICTIONARY test_dict"


def test_create():
    assert instance.query("SHOW GRANTS FOR mira") == ""
    assert "Not enough privileges" in instance.query_and_get_error(
        create_query, user="mira"
    )

    instance.query("GRANT CREATE DICTIONARY ON *.* TO mira")
    instance.query(create_query, user="mira")
    instance.query(drop_query)

    instance.query("REVOKE CREATE DICTIONARY ON *.* FROM mira")
    assert instance.query("SHOW GRANTS FOR mira") == ""
    assert "Not enough privileges" in instance.query_and_get_error(
        create_query, user="mira"
    )

    instance.query("GRANT CREATE DICTIONARY ON default.* TO mira")
    instance.query(create_query, user="mira")
    instance.query(drop_query)

    instance.query("REVOKE CREATE DICTIONARY ON default.* FROM mira")
    assert instance.query("SHOW GRANTS FOR mira") == ""
    assert "Not enough privileges" in instance.query_and_get_error(
        create_query, user="mira"
    )

    instance.query("GRANT CREATE DICTIONARY ON default.test_dict TO mira")
    instance.query(create_query, user="mira")


def test_drop():
    instance.query(create_query)

    assert instance.query("SHOW GRANTS FOR mira") == ""
    assert "Not enough privileges" in instance.query_and_get_error(
        drop_query, user="mira"
    )

    instance.query("GRANT DROP DICTIONARY ON *.* TO mira")
    instance.query(drop_query, user="mira")
    instance.query(create_query)


def test_dictget():
    instance.query(create_query)

    dictget_query = "SELECT dictGet('default.test_dict', 'y', toUInt64(5))"
    instance.query(dictget_query) == "6\n"
    assert "Not enough privileges" in instance.query_and_get_error(
        dictget_query, user="mira"
    )

    instance.query("GRANT dictGet ON default.test_dict TO mira")
    instance.query(dictget_query, user="mira") == "6\n"

    dictget_query = "SELECT dictGet('default.test_dict', 'y', toUInt64(1))"
    instance.query(dictget_query) == "0\n"
    instance.query(dictget_query, user="mira") == "0\n"

    instance.query("REVOKE dictGet ON *.* FROM mira")
    assert "Not enough privileges" in instance.query_and_get_error(
        dictget_query, user="mira"
    )
