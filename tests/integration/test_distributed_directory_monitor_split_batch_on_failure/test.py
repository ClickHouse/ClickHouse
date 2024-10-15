import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# node1 -- distributed_background_insert_split_batch_on_failure=on
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/overrides_1.xml"],
)
# node2 -- distributed_background_insert_split_batch_on_failure=off
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/overrides_2.xml"],
)


def get_test_settings():
    settings = {"background_insert_batch": [0, 1]}
    return [(k, v) for k, values in settings.items() for v in values]


def drop_tables():
    tables = ["null_", "dist", "data", "mv", "dist_data"]
    query = "\n".join([f"drop table if exists {table};" for table in tables])
    for _, node in cluster.instances.items():
        node.query(query)


def create_tables(**dist_settings):
    drop_tables()
    _settings_values = ",".join([f"{k}={v}" for k, v in dist_settings.items()])
    _settings = f"settings {_settings_values}" if _settings_values else ""
    for _, node in cluster.instances.items():
        node.query(
            f"""
                create table null_ (key Int, value Int) engine=Null();
                create table dist as null_ engine=Distributed(test_cluster, currentDatabase(), null_, key) {_settings};
                create table data (key Int, uniq_values Int) engine=Memory();
                create materialized view mv to data as select key, uniqExact(value) uniq_values from null_ group by key;
                system stop distributed sends dist;

                create table dist_data as data engine=Distributed(test_cluster, currentDatabase(), data);
                """
        )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        drop_tables()
        cluster.shutdown()


def test_distributed_background_insert_split_batch_on_failure_OFF(started_cluster):
    for setting, setting_value in get_test_settings():
        create_tables(**{setting: setting_value})
        for i in range(0, 100):
            limit = 100e3
            node2.query(
                f"insert into dist select number/100, number from system.numbers limit {limit} offset {limit*i}",
                settings={
                    # max_memory_usage is the limit for the batch on the remote node
                    # (local query should not be affected since 20MB is enough for 100K rows)
                    "max_memory_usage": "20Mi",
                    "max_untracked_memory": "0",
                },
            )
        # "Received from" is mandatory, since the exception should be thrown on the remote node.
        if setting == "background_insert_batch" and setting_value == 1:
            with pytest.raises(
                QueryRuntimeException,
                # no DOTALL in pytest.raises, use '(.|\n)'
                match=r"DB::Exception: Received from.*Memory limit \(for query\) exceeded: (.|\n)*While sending a batch",
            ):
                node2.query("system flush distributed dist")
            assert int(node2.query("select count() from dist_data")) == 0
            continue
        node2.query("system flush distributed dist")
        assert int(node2.query("select count() from dist_data")) == 100000


def test_distributed_background_insert_split_batch_on_failure_ON(started_cluster):
    for setting, setting_value in get_test_settings():
        create_tables(**{setting: setting_value})
        for i in range(0, 100):
            limit = 100e3
            node1.query(
                f"insert into dist select number/100, number from system.numbers limit {limit} offset {limit*i}",
                settings={
                    # max_memory_usage is the limit for the batch on the remote node
                    # (local query should not be affected since 20MB is enough for 100K rows)
                    "max_memory_usage": "20Mi",
                    "max_untracked_memory": "0",
                },
            )
        node1.query("system flush distributed dist")
        assert int(node1.query("select count() from dist_data")) == 100000
