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
    for _, node in cluster.instances.items():
        node.query(
            """
                drop view mv;
                create materialized view mv to data as
                    select key, uniqExact(value) uniq_values
                    from null_ array join range(if(value = 2, 2, 1))
                    group by key;
            """
        )

    for value in (1, 2, 3):
        node1.query(
            f"insert into dist values (1, {value})",
            settings={"distributed_foreground_insert": 0},
        )

    queue_path = node1.query(
        "select data_path from system.distribution_queue where table='dist' and data_files=3"
    ).strip()
    files = node1.exec_in_container(
        ["bash", "-c", f"ls -1 {queue_path}/*.bin | sort -V"]
    ).strip().splitlines()
    assert len(files) == 3
    file_indices = [file.rsplit("/", 1)[-1][:-4] for file in files]

    with pytest.raises(QueryRuntimeException, match="ARGUMENT_OUT_OF_BOUND"):
        node1.query(
            "system flush distributed dist settings function_range_max_elements_in_block=1"
        )

    assert int(
        node1.query(
            "select sum(data_files) from system.distribution_queue where table='dist'"
        )
    ) == 2
    assert int(node1.query("select sum(uniq_values) from dist_data")) == 1
    # `current_batch.txt` must preserve failed B followed by unsent C without sent A.
    assert node1.exec_in_container(
        ["cat", f"{queue_path}/current_batch.txt"]
    ).splitlines() == file_indices[1:]

    # Simulate shutdown after sent A was removed but before the batch metadata was rewritten.
    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"printf '%s\\n' {' '.join(file_indices)} > {queue_path}/current_batch.txt",
        ]
    )
    with pytest.raises(QueryRuntimeException, match="ARGUMENT_OUT_OF_BOUND"):
        node1.query(
            "system flush distributed dist settings function_range_max_elements_in_block=1"
        )
    assert int(
        node1.query(
            "select sum(data_files) from system.distribution_queue where table='dist'"
        )
    ) == 2
    assert int(node1.query("select sum(uniq_values) from dist_data")) == 1

    node1.query(
        "system flush distributed dist settings function_range_max_elements_in_block=3"
    )
    assert int(node1.query("select sum(uniq_values) from dist_data")) == 3


def test_recovery_resends_existing_file_before_broken_file(started_cluster):
    create_tables(
        background_insert_batch=1, background_insert_split_batch_on_failure=1
    )
    for value in (1, 2, 3):
        node1.query(
            f"insert into dist values (1, {value})",
            settings={"distributed_foreground_insert": 0},
        )

    queue_path = node1.query(
        "select data_path from system.distribution_queue where table='dist' and data_files=3"
    ).strip()
    files = node1.exec_in_container(
        ["bash", "-c", f"ls -1 {queue_path}/*.bin | sort -V"]
    ).strip().splitlines()
    assert len(files) == 3
    file_indices = [file.rsplit("/", 1)[-1][:-4] for file in files]

    # A server without ordered split retries skipped `A` after a transient error, went
    # on to quarantine `B`, and died before rewriting the batch metadata. `A` was never
    # acknowledged by the remote shard, so recovery must resend it rather than delete it.
    node1.exec_in_container(["mv", files[1], f"{queue_path}/broken/"])
    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"printf '%s\\n' {' '.join(file_indices)} > {queue_path}/current_batch.txt",
        ]
    )

    node1.query("system flush distributed dist")
    # Every insert block produces its own row in `data` with the number of distinct
    # values it carried, so the sum counts delivered files whether or not `A` and `C`
    # were squashed into one block. Both must arrive; only quarantined `B` is lost.
    assert int(node1.query("select sum(uniq_values) from dist_data")) == 2
    # The queue directory must drain. (`system.distribution_queue` is not consulted
    # here: `B` was quarantined by this test with an out-of-band `mv`, so the in-memory
    # file counter still accounts for it.)
    remaining_files = node1.exec_in_container(
        ["bash", "-c", f"ls -1 {queue_path}/*.bin 2>/dev/null || true"]
    ).strip()
    assert remaining_files == ""


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
    # Exercise a successful file after an earlier file is quarantined.
    node1.exec_in_container(["truncate", "-s", "0", files[0]])

    node1.query("system start distributed sends dist")
    node1.query("system flush distributed dist")

    assert int(node1.query("select count() from dist")) == 1
    assert int(
        node1.query(
            "select sum(data_files) from system.distribution_queue where table='dist'"
        )
    ) == 0


def test_recovery_after_broken_file_followed_by_sent_file(started_cluster):
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
    file_indices = [file.rsplit("/", 1)[-1][:-4] for file in files]
    # A is quarantined locally, B is then sent successfully.
    node1.exec_in_container(["truncate", "-s", "0", files[0]])

    node1.query("system start distributed sends dist")
    node1.query("system flush distributed dist")
    assert int(node1.query("select count() from dist")) == 1

    # Simulate a shutdown right after B was acknowledged and removed, but before the
    # batch metadata was rewritten: `A` sits in `broken/`, `B` is gone, and the stale
    # `current_batch.txt` still lists both of them.
    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"printf '%s\\n' {' '.join(file_indices)} > {queue_path}/current_batch.txt",
        ]
    )

    node1.query("system flush distributed dist")
    # The acknowledged file must not be sent again and the queue must drain.
    assert int(node1.query("select count() from dist")) == 1
    assert int(
        node1.query(
            "select sum(data_files) from system.distribution_queue where table='dist'"
        )
    ) == 0
