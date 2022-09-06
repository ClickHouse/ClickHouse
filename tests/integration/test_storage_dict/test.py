import pytest

from helpers.cluster import ClickHouseCluster

uuids = []


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1", main_configs=["configs/conf.xml"], with_nginx=True
        )
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_storage_dict(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"insert into table function url(urldb) values ('foo', 'bar')")
    result = node1.query(f"select * from url(urldb)")
    assert result.strip() == "foo\tbar"

    node1.query(
        f"create dictionary dict (k String, v String) primary key k source(http(name urldict)) layout(complex_key_hashed()) lifetime(min 0 max 100)"
    )
    result = node1.query(f"select * from dict")
    assert result.strip() == "foo\tbar"

    node1.query(
        f"create dictionary dict1 (k String, v String) primary key k source(http(name urldict1 format TabSeparated)) layout(complex_key_hashed()) lifetime(min 0 max 100)"
    )
    result = node1.query(f"select * from dict1")
    assert result.strip() == "foo\tbar"

    node1.query(
        f"create dictionary dict2 (k String, v String) primary key k source(http(name urldict2 format TabSeparated)) layout(complex_key_hashed()) lifetime(min 0 max 100)"
    )
    result = node1.query(f"select * from dict2")
    assert result.strip() == "foo\tbar"
