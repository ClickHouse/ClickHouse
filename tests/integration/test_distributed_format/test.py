# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
# pylint: disable=line-too-long

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/remote_servers.xml", "configs/another_remote_servers.xml"],
    stay_alive=True,
)

cluster_param = pytest.mark.parametrize(
    "cluster",
    [
        ("test_cluster_internal_replication"),
        ("test_cluster_no_internal_replication"),
    ],
)


def get_dist_path(cluster, node, table):
    data_path = node.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='test' AND name='{table}'"
    ).strip()
    if cluster == "test_cluster_internal_replication":
        return f"{data_path}/shard1_all_replicas"
    return f"{data_path}/shard1_replica1"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node.query("create database test")
        yield cluster

    finally:
        cluster.shutdown()


@cluster_param
def test_single_file(started_cluster, cluster):
    node.query("drop table if exists test.distr_1 sync")

    node.query(
        "create table test.distr_1 (x UInt64, s String) engine = Distributed('{}', database, table)".format(
            cluster
        )
    )
    node.query("insert into test.distr_1 values (1, 'a'), (2, 'bb'), (3, 'ccc')")

    path = get_dist_path(cluster, node, "distr_1")
    query = f"select * from file('{path}/1.bin', 'Distributed')"
    out = node.exec_in_container(
        ["/usr/bin/clickhouse", "local", "--stacktrace", "-q", query]
    )

    assert out == "1\ta\n2\tbb\n3\tccc\n"

    query = f"""
    create table t (x UInt64, s String) engine = File('Distributed', '{path}/1.bin');
    select * from t;
    """
    out = node.exec_in_container(
        ["/usr/bin/clickhouse", "local", "--stacktrace", "-q", query]
    )

    assert out == "1\ta\n2\tbb\n3\tccc\n"

    node.query("drop table test.distr_1 sync")


@cluster_param
def test_two_files(started_cluster, cluster):
    node.query("drop table if exists test.distr_2 sync")
    node.query(
        "create table test.distr_2 (x UInt64, s String) engine = Distributed('{}', database, table)".format(
            cluster
        )
    )
    node.query("insert into test.distr_2 values (0, '_'), (1, 'a')")
    node.query("insert into test.distr_2 values (2, 'bb'), (3, 'ccc')")

    path = get_dist_path(cluster, node, "distr_2")
    query = f"select * from file('{path}/{{1,2,3,4}}.bin', 'Distributed') order by x"
    out = node.exec_in_container(
        ["/usr/bin/clickhouse", "local", "--stacktrace", "-q", query]
    )

    assert out == "0\t_\n1\ta\n2\tbb\n3\tccc\n"

    query = f"""
    create table t (x UInt64, s String) engine = File('Distributed', '{path}/{{1,2,3,4}}.bin');
    select * from t order by x;
    """
    out = node.exec_in_container(
        ["/usr/bin/clickhouse", "local", "--stacktrace", "-q", query]
    )

    assert out == "0\t_\n1\ta\n2\tbb\n3\tccc\n"

    node.query("drop table test.distr_2 sync")


def test_remove_replica(started_cluster):
    node.query("drop table if exists test.local_4 sync")
    node.query("drop table if exists test.distr_4 sync")
    node.query(
        "create table test.local_4 (x UInt64, s String) engine = MergeTree order by x"
    )
    node.query(
        "create table test.distr_4 (x UInt64, s String) engine = Distributed('test_cluster_remove_replica1', test, local_4)"
    )
    node.query(
        "insert into test.distr_4 values (1, 'a'), (2, 'bb'), (3, 'ccc'), (4, 'dddd')"
    )
    node.query("detach table test.distr_4")

    node.exec_in_container(
        [
            "sed",
            "-i",
            "s/test_cluster_remove_replica1/test_cluster_remove_replica_tmp/g",
            "/etc/clickhouse-server/config.d/another_remote_servers.xml",
        ]
    )
    node.exec_in_container(
        [
            "sed",
            "-i",
            "s/test_cluster_remove_replica2/test_cluster_remove_replica1/g",
            "/etc/clickhouse-server/config.d/another_remote_servers.xml",
        ]
    )
    node.query("SYSTEM RELOAD CONFIG")
    node.query("attach table test.distr_4", ignore_error=True)
    node.query("SYSTEM FLUSH DISTRIBUTED test.distr_4", ignore_error=True)
    assert node.query("select 1") == "1\n"

    node.query("drop table test.local_4 sync")
    node.query("drop table test.distr_4 sync")

    # revert back the configs for the subsequent runs
    node.exec_in_container(
        [
            "sed",
            "-i",
            "s/test_cluster_remove_replica1/test_cluster_remove_replica2/g",
            "/etc/clickhouse-server/config.d/another_remote_servers.xml",
        ]
    )
    node.exec_in_container(
        [
            "sed",
            "-i",
            "s/test_cluster_remove_replica_tmp/test_cluster_remove_replica1/g",
            "/etc/clickhouse-server/config.d/another_remote_servers.xml",
        ]
    )


@pytest.mark.parametrize("fsync_directories", [0, 1])
def test_invalid_shard_directory_format(started_cluster, fsync_directories):
    """
    A subdirectory whose name is not one the sink writes names no destination, so its files can
    never be sent. It is renamed to 'unrecognized_<hash>' instead of being taken for a directory
    queue, so it is not reported and a single stray subdirectory cannot break the attach. The old
    name is kept in the file 'original_name' in it. With fsync_directories, the rename and the
    removal on TRUNCATE go through the directory sync guard, like the rest of the spool.
    """
    node.query("drop table if exists test.dist_invalid sync")
    node.query("drop table if exists test.local_invalid sync")
    node.query(
        "create table test.local_invalid (x UInt64, s String) engine = MergeTree order by x"
    )
    node.query(
        "create table test.dist_invalid (x UInt64, s String) "
        "engine = Distributed('test_cluster_internal_replication', test, local_invalid) "
        f"settings fsync_directories = {fsync_directories}"
    )

    node.query("insert into test.dist_invalid values (1, 'a'), (2, 'bb')")

    data_path = node.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables "
        "WHERE database='test' AND name='dist_invalid'"
    ).strip()

    invalid_formats = [
        "shard1_all_replicas_bkp",
        "shard1_all_replicas_backup",
        "shard1_all_replicas_old",
        "shard2_all_replicas_tmp",
        # Well-formed names joined with a comma: the sink writes one directory per destination, so
        # this must not be taken for a queue that sends to both replicas.
        "shard1_replica1,shard1_replica2",
        # Alternate spellings of a well-formed name: the sink writes indices without leading zeros
        # or a sign, so these are not the same directory as shard1_replica1 / shard1_all_replicas.
        "shard01_replica1",
        "shard+1_replica1",
        "shard1_replica01",
        "shard01_all_replicas",
    ]
    # As a server older than 26.9 would have named it with
    # use_compact_format_in_distributed_parts_names=0. The password is unique per run, because the
    # text_log keeps the queries of an earlier run, which mention it.
    password = f"hunter2_{fsync_directories}"
    invalid_formats.append(f"default:{password}@127%2E0%2E0%2E1:9000")
    for invalid_dir in invalid_formats:
        invalid_path = f"{data_path}/{invalid_dir}"
        node.exec_in_container(["mkdir", "-p", invalid_path])
        # just dummy file to have something in the directory
        node.exec_in_container(["touch", f"{invalid_path}/dummy.txt"])

    # Reproduce server restart with detach and attach
    node.query("detach table test.dist_invalid")
    node.query("attach table test.dist_invalid")

    # The queue of the one well-formed directory is still there, so the assertions below are not
    # vacuous.
    assert (
        node.query(
            "SELECT count() FROM system.distribution_queue "
            "WHERE database = 'test' AND table = 'dist_invalid'"
        ).strip()
        == "1"
    )

    listing = node.exec_in_container(["ls", "-1", data_path]).split()

    # Every unrecognized name is renamed and nothing is deleted. The old name, the only record of
    # where the files were meant to be sent, is kept in a file next to them.
    renamed = [name for name in listing if name.startswith("unrecognized_")]
    assert len(renamed) == len(invalid_formats), listing
    assert sorted(listing) == sorted(renamed + ["shard1_all_replicas"]), listing
    original_names = []
    for name in renamed:
        assert sorted(
            node.exec_in_container(["ls", "-1", f"{data_path}/{name}"]).split()
        ) == ["dummy.txt", "original_name"]
        original_names.append(
            node.exec_in_container(["cat", f"{data_path}/{name}/original_name"])
        )
    assert sorted(original_names) == sorted(invalid_formats), original_names

    # The old name is gone from the directory names, from the reported path and from the log.
    node.query("SYSTEM FLUSH LOGS system.text_log")
    assert password not in node.exec_in_container(["ls", "-1R", data_path])
    assert (
        node.query(
            "SELECT count() FROM system.distribution_queue "
            f"WHERE database = 'test' AND table = 'dist_invalid' AND position(data_path, '{password}') > 0"
        ).strip()
        == "0"
    )
    assert (
        node.query(
            f"SELECT count() FROM system.text_log WHERE position(message, '{password}') > 0"
        ).strip()
        == "0"
    )

    # A second start leaves the renamed directories alone.
    node.query("detach table test.dist_invalid")
    node.query("attach table test.dist_invalid")
    assert sorted(node.exec_in_container(["ls", "-1", data_path]).split()) == sorted(
        listing
    ), node.exec_in_container(["ls", "-1", data_path])

    # The renamed directories have no directory queue, but they are still part of the on-disk
    # spool, so `TRUNCATE TABLE` removes them along with the well-formed one. Otherwise they would
    # occupy disk with no way to get rid of them from SQL.
    node.query("truncate table test.dist_invalid")
    assert (
        node.exec_in_container(["ls", "-1", data_path]).split() == []
    ), node.exec_in_container(["ls", "-1R", data_path])

    # Clean up
    node.query("drop table test.dist_invalid sync")
    node.query("drop table test.local_invalid sync")


@cluster_param
def test_selected_rows_not_double_counted(started_cluster, cluster):
    # `Distributed` is read through an inner pipeline whose source accounts the rows on its own,
    # so `SelectedRows` and `SelectedBytes` are twice `read_rows`/`read_bytes` of the same query
    # unless that pipeline has profile event updates disabled. See #116301.
    node.query("drop table if exists test.distr_counters sync")
    node.query(
        "create table test.distr_counters (x UInt64, s String) engine = "
        "Distributed('{}', database, table)".format(cluster)
    )
    node.query("insert into test.distr_counters values (1, 'a'), (2, 'bb'), (3, 'ccc')")
    path = get_dist_path(cluster, node, "distr_counters")

    # The spool file lives under the table's data path, which `file` refuses to read, so the read
    # goes through a copy inside `user_files`. Both names carry the cluster to keep the two
    # parametrized runs independent.
    file_name = f"distr_counters_{cluster}.bin"
    query_id = f"116301_distfmt_{cluster}"
    try:
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"mkdir -p /var/lib/clickhouse/user_files && cp {path}/1.bin /var/lib/clickhouse/user_files/{file_name}",
            ],
            privileged=True,
            user="root",
        )

        node.query(
            f"select * from file('{file_name}', 'Distributed') format Null",
            query_id=query_id,
            settings={
                # The server-side AST fuzzer would re-run this read as extra queries.
                "ast_fuzzer_runs": "0",
            },
        )
        node.query("system flush logs query_log")

        read_rows, read_bytes, selected_rows, selected_bytes = node.query(f"""
            select read_rows, read_bytes,
                   ProfileEvents['SelectedRows'], ProfileEvents['SelectedBytes']
            from system.query_log
            where query_id = '{query_id}' and type = 'QueryFinish'
            order by event_time_microseconds desc limit 1
            """).split()

        # The read amounts are pinned as well, so a query that stops reading the spool file cannot
        # satisfy the equalities with both sides at zero.
        assert read_rows == "3", (read_rows, read_bytes)
        assert read_bytes != "0", (read_rows, read_bytes)
        assert selected_rows == read_rows, (selected_rows, read_rows)
        assert selected_bytes == read_bytes, (selected_bytes, read_bytes)
    finally:
        node.exec_in_container(
            ["bash", "-c", f"rm -f /var/lib/clickhouse/user_files/{file_name}"],
            privileged=True,
            user="root",
        )
        node.query("drop table test.distr_counters sync")
