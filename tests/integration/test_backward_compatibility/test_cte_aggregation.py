import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_zookeeper=True, image='yandex/clickhouse-server', tag='21.8', with_installed_binary=True)
node2 = cluster.add_instance('node2', with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_cte_aggregation(start_cluster):
    queries = [
        """
        with sum(is_aggregate) as d 
        select count() from {}
        """,
        """
        with concat(name, '_foo') as func_foo,
            alias_to as bar 
        select uniqCombined(func_foo, alias_to), count() from {}
        """,
        """
        with (select dummy as number from system.one) as one
        select name, count()
        from {}
        where is_aggregate = one
        group by name
        """,
        """
        select sum(is_aggregate) as is_aggregate,
            count(),
            substring(name, 1, 2) as prefix
        from (
            with max(is_aggregate) as is_aggregate,
                sum(case_insensitive) as case_insensitive
            select
                case_insensitive,
                name,
                is_aggregate
            from {}
            group by name
        )
        group by prefix
        """,
    ]

    sources = [
        "system.functions",
        "remote('node{1,2}', system.functions)"
    ]

    for test_query in queries:
        for source in sources:
            node1.query(test_query.format(source))
            node2.query(test_query.format(source))
