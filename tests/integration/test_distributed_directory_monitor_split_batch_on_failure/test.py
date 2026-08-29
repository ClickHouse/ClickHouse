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
                match=r"DB::Exception: Received from.*Query memory limit exceeded: (.|\n)*While sending a batch",
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


def test_transient_error_during_split_keeps_files(started_cluster):
    drop_tables()
    for _, node in cluster.instances.items():
        node.query(
            """
                create table data (key Int, value Int) engine=MergeTree order by key
                    settings parts_to_throw_insert=1;
                insert into data values (0, 0);
            """
        )

    node1.query(
        """
            create table dist as data engine=Distributed(test_cluster, currentDatabase(), data, key)
                settings background_insert_batch=1, background_insert_split_batch_on_failure=1;
            system stop distributed sends dist;
        """
    )
    for value in (1, 2):
        node1.query(
            f"insert into dist values (1, {value})",
            settings={"distributed_foreground_insert": 0},
        )

    node1.query("system start distributed sends dist")
    with pytest.raises(QueryRuntimeException, match="TOO_MANY_PARTS"):
        node1.query("system flush distributed dist")

    assert int(
        node1.query(
            "select sum(data_files) from system.distribution_queue where table='dist'"
        )
    ) == 2
    assert int(node1.query("select count() from dist")) == 2

    for _, node in cluster.instances.items():
        node.query("alter table data modify setting parts_to_throw_insert=100")

    node1.query("system flush distributed dist")
    assert int(node1.query("select count() from dist")) == 4


def test_transient_error_after_sent_file_keeps_only_unsent_files(started_cluster):
    create_tables(
        background_insert_batch=1, background_insert_split_batch_on_failure=1
    )
    insert_settings = {
        "distributed_foreground_insert": 0,
        "max_memory_usage": "10Mi",
        "max_untracked_memory": "0",
    }
    for offset, limit in ((0, 1), (1, 1_000_000), (1_000_001, 100_000)):
        node1.query(
            f"insert into dist select 1, number from system.numbers limit {limit} offset {offset}",
            settings=insert_settings,
        )

    queue_path = node1.query(
        "select data_path from system.distribution_queue where table='dist' and data_files=3"
    ).strip()
    files = node1.exec_in_container(
        ["bash", "-c", f"ls -1 {queue_path}/*.bin | sort -V"]
    ).strip().splitlines()
    assert len(files) == 3

    node1.query("system start distributed sends dist")
    with pytest.raises(
        QueryRuntimeException,
        match=r"DB::Exception: Received from.*Query memory limit exceeded",
    ):
        node1.query("system flush distributed dist")

    assert int(
        node1.query(
            "select sum(data_files) from system.distribution_queue where table='dist'"
        )
    ) == 2
    assert int(node1.query("select sum(uniq_values) from dist_data")) == 1
    # `current_batch.txt` must preserve failed B followed by unsent C without sent A.
    assert node1.exec_in_container(
        ["cat", f"{queue_path}/current_batch.txt"]
    ).splitlines() == [file.rsplit("/", 1)[-1][:-4] for file in files[1:]]

    node1.query("system flush distributed dist settings max_memory_usage='1Gi'")
    assert int(node1.query("select sum(uniq_values) from dist_data")) == 1_100_001


def test_broken_file_during_split_removes_sent_files(started_cluster):
    drop_tables()
    for _, node in cluster.instances.items():
        node.query("create table data (key Int, value Int) engine=MergeTree order by key")

    node1.query(
        """
            create table dist as data engine=Distributed(test_cluster, currentDatabase(), data, key)
                settings background_insert_batch=1, background_insert_split_batch_on_failure=1;
            system stop distributed sends dist;
        """
    )
    for value in (1, 2):
        node1.query(
            f"insert into dist values (1, {value})",
            settings={"distributed_foreground_insert": 0},
        )

    queue_path = node1.query(
        "select data_path from system.distribution_queue where table='dist' and data_files=2"
    ).strip()
    files = node1.exec_in_container(
        ["bash", "-c", f"ls -1 {queue_path}/*.bin | sort -V"]
    ).strip().splitlines()
    assert len(files) == 2
    file_size = int(node1.exec_in_container(["stat", "-c", "%s", files[-1]]))
    node1.exec_in_container(["truncate", "-s", str(file_size - 10), files[-1]])

    node1.query("system start distributed sends dist")
    node1.query("system flush distributed dist")

    assert int(node1.query("select count() from dist")) == 1
    assert int(
        node1.query(
            "select sum(data_files) from system.distribution_queue where table='dist'"
        )
    ) == 0
