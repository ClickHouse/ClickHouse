import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance')


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

def test_access_rights_for_funtion():
    create_data_type_query = "CREATE TYPE MyType AS int"

    instance.query("CREATE USER A")
    instance.query("CREATE USER B")
    assert "it's necessary to have grant CREATE TYPE ON *.*" in instance.query_and_get_error(create_data_type_query, user = 'A')

    instance.query("GRANT CREATE TYPE on *.* TO A")

    instance.query(create_data_type_query, user = 'A')

    assert "it's necessary to have grant DROP TYPE ON *.*" in instance.query_and_get_error("DROP TYPE MyType", user = 'B')

    instance.query("GRANT DROP TYPE ON *.* TO B")
    instance.query("DROP TYPE MyType", user = 'B')

    instance.query("REVOKE CREATE TYPE ON *.* FROM A")
    assert "it's necessary to have grant CREATE TYPE ON *.*" in instance.query_and_get_error(create_data_type_query, user = 'A')

    instance.query("DROP USER IF EXISTS A")
    instance.query("DROP USER IF EXISTS B")
