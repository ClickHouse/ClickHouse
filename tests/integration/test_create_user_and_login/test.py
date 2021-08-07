import pytest
import time
import logging
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance')


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        instance.query("DROP USER IF EXISTS A, B")


def test_login():
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")
    assert instance.query("SELECT 1", user='A') == "1\n"
    assert instance.query("SELECT 1", user='B') == "1\n"


def test_grant_create_user():
    instance.query("CREATE USER A")

    expected_error = "Not enough privileges"
    assert expected_error in instance.query_and_get_error("CREATE USER B", user='A')

    instance.query("GRANT CREATE USER ON *.* TO A")
    instance.query("CREATE USER B", user='A')
    assert instance.query("SELECT 1", user='B') == "1\n"


def test_login_as_dropped_user():
    for _ in range(0, 2):
        instance.query("CREATE USER A")
        assert instance.query("SELECT 1", user='A') == "1\n"

        instance.query("DROP USER A")
        expected_error = "no user with such name"
        assert expected_error in instance.query_and_get_error("SELECT 1", user='A')


def test_login_as_dropped_user_xml():
    for _ in range(0, 2):
        instance.exec_in_container(["bash", "-c" , """
            cat > /etc/clickhouse-server/users.d/user_c.xml << EOF
<?xml version="1.0"?>
<yandex>
    <users>
        <C>
            <no_password/>
        </C>
    </users>
</yandex>
EOF"""])

        assert_eq_with_retry(instance, "SELECT name FROM system.users WHERE name='C'", "C")

        instance.exec_in_container(["bash", "-c" , "rm /etc/clickhouse-server/users.d/user_c.xml"])

        expected_error = "no user with such name"
        while True:
            out, err = instance.query_and_get_answer_with_error("SELECT 1", user='C')
            if expected_error in err:
                logging.debug(f"Got error '{expected_error}' just as expected")
                break
            if out == "1\n":
                logging.debug(f"Got output '1', retrying...")
                time.sleep(0.5)
                continue
            raise Exception(f"Expected either output '1' or error '{expected_error}', got output={out} and error={err}")
            
        assert instance.query("SELECT name FROM system.users WHERE name='C'") == ""
