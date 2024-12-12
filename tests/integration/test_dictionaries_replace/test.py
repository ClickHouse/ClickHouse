import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node_ll = cluster.add_instance(
    "node_ll",
    main_configs=[
        "configs/remote_servers.xml",
    ],
    user_configs=[
        "configs/allow_database_types.xml",
    ],
    macros={"replica": "node_ll", "shard": "shard"},
    with_zookeeper=True,
)

node_no_ll = cluster.add_instance(
    "node_no_ll",
    main_configs=[
        "configs/no_lazy_load.xml",
        "configs/remote_servers.xml",
    ],
    user_configs=[
        "configs/allow_database_types.xml",
    ],
    macros={"replica": "node_no_ll", "shard": "shard"},
    with_zookeeper=True,
)

instances = [node_ll, node_no_ll]


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        # "sleep(1)" is here to make loading of dictionaries a bit slower for this test.
        instances[0].query(
            "CREATE VIEW src ON CLUSTER 'cluster' AS SELECT number AS key, number * number + 1 + sleep(1) AS value FROM numbers(10)"
        )

        # "dict_get_user" can only call dictGet(), nothing more.
        instances[0].query("CREATE USER dictget_user ON CLUSTER 'cluster'")
        instances[0].query(
            "GRANT dictGet ON atomicdb.dict TO dictget_user ON CLUSTER 'cluster'"
        )
        instances[0].query(
            "GRANT dictGet ON repldb.dict TO dictget_user ON CLUSTER 'cluster'"
        )

        instances[0].query("CREATE DATABASE atomicdb ON CLUSTER 'cluster'")
        instances[0].query(
            "CREATE DATABASE repldb ON CLUSTER 'cluster' ENGINE=Replicated('/clickhouse/path/','{shard}','{replica}')"
        )

        yield cluster

    finally:
        instances[0].query("DROP TABLE IF EXISTS src ON CLUSTER 'cluster'")
        instances[0].query("DROP USER IF EXISTS dictget_user ON CLUSTER 'cluster'")
        instances[0].query("DROP DATABASE IF EXISTS atomicdb ON CLUSTER 'cluster'")
        instances[0].query("DROP DATABASE IF EXISTS repldb ON CLUSTER 'cluster'")

        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        instances[0].query("DROP DICTIONARY IF EXISTS dict ON CLUSTER 'cluster'")


def get_status(instance, dictionary_name):
    return instance.query(
        "SELECT status FROM system.dictionaries WHERE name='" + dictionary_name + "'"
    ).rstrip("\n")


@pytest.mark.parametrize(
    "database, instance_to_create_dictionary, instances_to_check",
    [
        ("atomicdb", node_ll, [node_ll]),
        ("atomicdb", node_no_ll, [node_no_ll]),
        ("repldb", node_ll, [node_ll, node_no_ll]),
        ("repldb", node_no_ll, [node_ll, node_no_ll]),
    ],
)
def test_create_or_replace(database, instance_to_create_dictionary, instances_to_check):
    num_steps = 2
    dict_uuids = {}
    for step in range(0, num_steps):
        create_dictionary_query = f"CREATE OR REPLACE DICTIONARY {database}.dict (key Int64, value Int64) PRIMARY KEY key SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'src' PASSWORD '' DB 'default')) LIFETIME(0) LAYOUT(FLAT())"
        instance_to_create_dictionary.query(create_dictionary_query)

        for instance in instances_to_check:
            if instance != instance_to_create_dictionary:
                instance.query(f"SYSTEM SYNC DATABASE REPLICA {database}")

            dict_uuid = instance.query(
                f"SELECT uuid FROM system.tables WHERE database='{database}' AND table='dict'"
            ).strip("\n")

            dict_db, dict_name, dict_status = (
                instance.query(
                    f"SELECT database, name, status FROM system.dictionaries WHERE uuid='{dict_uuid}'"
                )
                .strip("\n")
                .split("\t")
            )
            assert dict_db == database
            assert dict_name == "dict"

            # "uuid" must be the same for all the dictionaries created at the same "step" and different for the dictionaries created at different steps.
            if step in dict_uuids:
                assert dict_uuids[step] == dict_uuid
            dict_uuids[step] = dict_uuid
            assert dict_uuid not in [
                dict_uuids[prev_step] for prev_step in range(0, step)
            ]

            expected_dict_status = (
                ["NOT_LOADED"] if instance == node_ll else ["LOADING", "LOADED"]
            )
            assert dict_status in expected_dict_status

    for instance in instances_to_check:
        select_query = f"SELECT arrayJoin([0, 5, 7, 11]) as key, dictGet({database}.dict, 'value', key)"
        expected_result = TSV([[0, 1], [5, 26], [7, 50], [11, 0]])
        assert instance.query(select_query) == expected_result
        assert instance.query(select_query, user="dictget_user") == expected_result

    instance_to_create_dictionary.query(f"DROP DICTIONARY IF EXISTS {database}.dict")
