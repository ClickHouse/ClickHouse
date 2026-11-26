import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_grants.xml"],
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_system_grants_regex_output(started_cluster):
    node.query("DROP USER IF EXISTS test_regex_grant_user")
    node.query("CREATE USER test_regex_grant_user")

    node.query("GRANT READ ON URL('http://localhost:912.*') TO test_regex_grant_user")
    node.query("GRANT READ ON S3('s3://foo/*') TO test_regex_grant_user")

    # Fetch the access objects and explicitly cast to strings, stripping newlines
    output = node.query(
        "SELECT access_object FROM system.grants WHERE user_name = 'test_regex_grant_user' ORDER BY access_object"
    ).strip().split('\n')

    assert output == ["S3(`s3://foo/*`)", "URL(`http://localhost:912.*`)"]

    node.query("DROP USER test_regex_grant_user")
