import pytest
import uuid
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_access_rights_for_function():
    create_function_query = "CREATE FUNCTION MySum AS (a, b) -> a + b"

    instance.query("CREATE USER A")
    instance.query("CREATE USER B")
    assert (
        "it's necessary to have grant CREATE FUNCTION ON *.*"
        in instance.query_and_get_error(create_function_query, user="A")
    )

    instance.query("GRANT CREATE FUNCTION on *.* TO A")

    instance.query(create_function_query, user="A")
    assert instance.query("SELECT MySum(1, 2)") == "3\n"

    assert (
        "it's necessary to have grant DROP FUNCTION ON *.*"
        in instance.query_and_get_error("DROP FUNCTION MySum", user="B")
    )

    instance.query("GRANT DROP FUNCTION ON *.* TO B")
    instance.query("DROP FUNCTION MySum", user="B")
    assert "Unknown function MySum" in instance.query_and_get_error(
        "SELECT MySum(1, 2)"
    )

    instance.query("REVOKE CREATE FUNCTION ON *.* FROM A")
    assert (
        "it's necessary to have grant CREATE FUNCTION ON *.*"
        in instance.query_and_get_error(create_function_query, user="A")
    )

    instance.query("DROP USER IF EXISTS A")
    instance.query("DROP USER IF EXISTS B")


def test_ignore_obsolete_grant_on_database():
    instance.stop_clickhouse()

    user_id = uuid.uuid4()
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"""
        cat > /var/lib/clickhouse/access/{user_id}.sql << EOF
ATTACH USER X;
ATTACH GRANT CREATE FUNCTION, SELECT ON mydb.* TO X;
EOF""",
        ]
    )

    instance.exec_in_container(
        ["bash", "-c", "touch /var/lib/clickhouse/access/need_rebuild_lists.mark"]
    )
    instance.start_clickhouse()

    assert instance.query("SHOW GRANTS FOR X") == "GRANT SELECT ON mydb.* TO X\n"
