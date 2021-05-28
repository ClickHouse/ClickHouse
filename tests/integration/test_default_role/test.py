import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
import re

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance')


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        
        instance.query("CREATE USER john")
        instance.query("CREATE ROLE rx")
        instance.query("CREATE ROLE ry")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def reset_users_and_roles():
    instance.query("CREATE USER OR REPLACE john")
    yield


def test_set_default_roles():
    assert instance.query("SHOW CURRENT ROLES", user="john") == ""

    instance.query("GRANT rx, ry TO john")
    assert instance.query("SHOW CURRENT ROLES", user="john") == TSV( [['rx', 0, 1], ['ry', 0, 1]] )

    instance.query("SET DEFAULT ROLE NONE TO john")
    assert instance.query("SHOW CURRENT ROLES", user="john") == ""

    instance.query("SET DEFAULT ROLE rx TO john")
    assert instance.query("SHOW CURRENT ROLES", user="john") == TSV( [['rx', 0, 1]] )

    instance.query("SET DEFAULT ROLE ry TO john")
    assert instance.query("SHOW CURRENT ROLES", user="john") == TSV( [['ry', 0, 1]] )

    instance.query("SET DEFAULT ROLE ALL TO john")
    assert instance.query("SHOW CURRENT ROLES", user="john") == TSV( [['rx', 0, 1], ['ry', 0, 1]] )

    instance.query("SET DEFAULT ROLE ALL EXCEPT rx TO john")
    assert instance.query("SHOW CURRENT ROLES", user="john") == TSV( [['ry', 0, 1]] )


def test_alter_user():
    assert instance.query("SHOW CURRENT ROLES", user="john") == ""

    instance.query("GRANT rx, ry TO john")
    assert instance.query("SHOW CURRENT ROLES", user="john") == TSV( [['rx', 0, 1], ['ry', 0, 1]] )

    instance.query("ALTER USER john DEFAULT ROLE NONE")
    assert instance.query("SHOW CURRENT ROLES", user="john") == ""

    instance.query("ALTER USER john DEFAULT ROLE rx")
    assert instance.query("SHOW CURRENT ROLES", user="john") == TSV( [['rx', 0, 1]] )

    instance.query("ALTER USER john DEFAULT ROLE ALL")
    assert instance.query("SHOW CURRENT ROLES", user="john") == TSV( [['rx', 0, 1], ['ry', 0, 1]] )

    instance.query("ALTER USER john DEFAULT ROLE ALL EXCEPT rx")
    assert instance.query("SHOW CURRENT ROLES", user="john") == TSV( [['ry', 0, 1]] )


def test_wrong_set_default_role():
    assert "There is no user `rx`" in instance.query_and_get_error("SET DEFAULT ROLE NONE TO rx")
    assert "There is no user `ry`" in instance.query_and_get_error("SET DEFAULT ROLE rx TO ry")
    assert "There is no role `john`" in instance.query_and_get_error("SET DEFAULT ROLE john TO john")
    assert "There is no role `john`" in instance.query_and_get_error("ALTER USER john DEFAULT ROLE john")
    assert "There is no role `john`" in instance.query_and_get_error("ALTER USER john DEFAULT ROLE ALL EXCEPT john")
