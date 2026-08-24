# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", user_configs=["configs/users_overrides.xml"])


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


# See https://github.com/ClickHouse/ClickHouse/issues/55279
def test_set():
    node.query(
        """
    CREATE TABLE 02581_trips (id UInt32, description String, id2 UInt32) ENGINE = MergeTree PRIMARY KEY id ORDER BY id;
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    INSERT INTO 02581_trips SELECT number, '', number FROM numbers(1);
    """
    )
    with pytest.raises(
        QueryRuntimeException,
        match="Exception happened during execution of mutation",
    ):
        node.query(
            "ALTER TABLE `02581_trips` UPDATE description = 'a' WHERE id IN (SELECT CAST(number * 10, 'UInt32') FROM numbers(10e9)) SETTINGS mutations_sync = 2"
        )
