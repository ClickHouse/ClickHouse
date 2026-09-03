import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_range_hashed_dict(started_cluster):
    script = "echo '4990954156238030839\t2018-12-31 21:00:00\t2020-12-30 20:59:59\t0.1\tRU' > /var/lib/clickhouse/user_files/rates.tsv"
    node1.exec_in_container(["bash", "-c", script])
    node1.query(
        """
    CREATE DICTIONARY rates
    (
        hash_id UInt64,
        start_date DateTime default '0000-00-00 00:00:00',
        end_date DateTime default '0000-00-00 00:00:00',
        price Float64,
        currency String
    )
    PRIMARY KEY hash_id
    SOURCE(file(
        path '/var/lib/clickhouse/user_files/rates.tsv'
        format 'TSV'
    ))
    LAYOUT(RANGE_HASHED())
    RANGE(MIN start_date MAX end_date)
    LIFETIME(60);
    """
    )
    node1.query("SYSTEM RELOAD DICTIONARY default.rates")

    assert (
        node1.query(
            "SELECT dictGetString('default.rates', 'currency', toUInt64(4990954156238030839), toDateTime('2019-10-01 00:00:00'))"
        )
        == "RU\n"
    )
