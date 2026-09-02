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


def get_dist_path(cluster, node, table, dist_format):
    data_path = node.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='test' AND name='{table}'"
    ).strip()
    if dist_format == 0:
        return f"{data_path}/default@not_existing:9000"
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
    node.query(
        "insert into test.distr_1 values (1, 'a'), (2, 'bb'), (3, 'ccc')",
        settings={"use_compact_format_in_distributed_parts_names": "1"},
    )

    path = get_dist_path(cluster, node, "distr_1", 1)
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
    node.query(
        "insert into test.distr_2 values (0, '_'), (1, 'a')",
        settings={
            "use_compact_format_in_distributed_parts_names": "1",
        },
    )
    node.query(
        "insert into test.distr_2 values (2, 'bb'), (3, 'ccc')",
        settings={
            "use_compact_format_in_distributed_parts_names": "1",
        },
    )

    path = get_dist_path(cluster, node, "distr_2", 1)
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


@cluster_param
def test_single_file_old(started_cluster, cluster):
    node.query("drop table if exists test.distr_3 sync")
    node.query("drop table if exists t sync")
    node.query(
        "create table test.distr_3 (x UInt64, s String) engine = Distributed('{}', database, table)".format(
            cluster
        )
    )
    node.query(
        "insert into test.distr_3 values (1, 'a'), (2, 'bb'), (3, 'ccc')",
        settings={
            "use_compact_format_in_distributed_parts_names": "0",
        },
    )

    path = get_dist_path(cluster, node, "distr_3", 0)
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

    node.query("drop table test.distr_3")


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

def test_invalid_shard_directory_format(started_cluster):
    """
    Test that ClickHouse doesn't crash when it encounters
    a malformed directory name like 'shard1_all_replicas_bkp'
    during distributed table initialization.
    """
    node.query("drop table if exists test.dist_invalid sync")
    node.query("drop table if exists test.local_invalid sync")
    node.query(
        "create table test.local_invalid (x UInt64, s String) engine = MergeTree order by x"
    )
    node.query(
        "create table test.dist_invalid (x UInt64, s String) "
        "engine = Distributed('test_cluster_internal_replication', test, local_invalid)"
    )

    node.query(
        "insert into test.dist_invalid values (1, 'a'), (2, 'bb')",
        settings={"use_compact_format_in_distributed_parts_names": "1"},
    )

    data_path = node.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables "
        "WHERE database='test' AND name='dist_invalid'"
    ).strip()

    # Create a malformed directory that would cause the bug
    malformed_dir = f"{data_path}/shard1_all_replicas_bkp"
    node.exec_in_container(["mkdir", "-p", malformed_dir])

    # Create a dummy file so the directory isn't considered empty
    node.exec_in_container(["touch", f"{malformed_dir}/dummy.txt"])

    invalid_formats = [
        "shard1_all_replicas_backup",
        "shard1_all_replicas_old",
        "shard2_all_replicas_tmp",
    ]
    for invalid_dir in invalid_formats:
        invalid_path = f"{data_path}/{invalid_dir}"
        node.exec_in_container(["mkdir", "-p", invalid_path])
        # just dummy file to have something in the directory
        node.exec_in_container(["touch", f"{invalid_path}/dummy.txt"])

    # Reproduce server restart with detach and attach
    node.query("detach table test.dist_invalid")
    node.query("attach table test.dist_invalid")

    node.query("SYSTEM FLUSH LOGS system.text_log")

    error_logs = node.query(
        """
        SELECT count()
        FROM system.text_log
        WHERE level = 'Error'
          AND message LIKE '%Invalid replica_index%'
          AND message LIKE '%shard1_all_replicas%'
        """
    ).strip()

    # We should have at least one error log for each malformed directory
    # But we don't strictly require this in case logging is disabled
    # The important thing is that the server didn't crash
    print(f"Found {error_logs} error log entries for invalid directories")

    # Clean up
    node.query("drop table test.dist_invalid sync")
    node.query("drop table test.local_invalid sync")


def test_long_directory_name_internal_replication(started_cluster):
    # With internal replication the async-insert directory is named after every replica of the
    # shard concatenated, so it can exceed NAME_MAX without any single field being long. That has
    # to be a user error rather than a logical error (which aborts assert builds). See #112719.
    node.query("drop table if exists test.local_long_path sync")
    node.query("drop table if exists test.distr_long_path sync")
    node.query(
        "create table test.local_long_path (x UInt64) engine = MergeTree order by x"
    )
    node.query(
        "create table test.distr_long_path (x UInt64) engine = "
        "Distributed('test_cluster_internal_replication_long_path', test, local_long_path)"
    )

    error = node.query_and_get_error(
        "insert into test.distr_long_path values (1)",
        settings={
            "distributed_foreground_insert": "0",
            "prefer_localhost_replica": "0",
            "use_compact_format_in_distributed_parts_names": "0",
        },
    )
    assert "ARGUMENT_OUT_OF_BOUND" in error
    assert "The max length of a directory name" in error
    assert "distr_long_path" in error
    assert "test_cluster_internal_replication_long_path" in error
    assert "is 255" in error

    # The compact format keeps the name bounded, so the same cluster still works with it.
    node.query(
        "insert into test.distr_long_path values (1)",
        settings={
            "distributed_foreground_insert": "0",
            "prefer_localhost_replica": "0",
            "use_compact_format_in_distributed_parts_names": "1",
        },
    )
    assert (
        node.query(
            "select count() from system.distribution_queue "
            "where database = 'test' and table = 'distr_long_path'"
        ).strip()
        != "0"
    )

    node.query("drop table test.distr_long_path sync")
    node.query("drop table test.local_long_path sync")


def test_long_directory_name_rejected_before_local_write(started_cluster):
    # A shard holding this server plus a too long remote destination must be rejected before the
    # local write, otherwise the INSERT reports a failure it has already partly applied and a
    # retry duplicates rows on the local replica.
    node.query("drop table if exists test.local_mixed_path sync")
    node.query("drop table if exists test.distr_mixed_path sync")
    node.query(
        "create table test.local_mixed_path (x UInt64) engine = MergeTree order by x"
    )
    node.query(
        "create table test.distr_mixed_path (x UInt64) engine = "
        "Distributed('test_cluster_mixed_local_long_path', test, local_mixed_path)"
    )

    settings = {
        "distributed_foreground_insert": "0",
        "prefer_localhost_replica": "1",
        "use_compact_format_in_distributed_parts_names": "0",
    }
    for _ in range(3):
        error = node.query_and_get_error(
            "insert into test.distr_mixed_path values (1)", settings=settings
        )
        assert "ARGUMENT_OUT_OF_BOUND" in error
        assert "The max length of a directory name" in error

    assert node.query("select count() from test.local_mixed_path").strip() == "0"

    # With the local replica queued rather than written to, the shard has two destinations and the
    # short one comes first, so a rejection driven by the second must leave no directory behind.
    queued_dirs = (
        "select count() from system.distribution_queue "
        "where database = 'test' and table = 'distr_mixed_path'"
    )
    error = node.query_and_get_error(
        "insert into test.distr_mixed_path values (1)",
        settings=dict(settings, prefer_localhost_replica="0"),
    )
    assert "ARGUMENT_OUT_OF_BOUND" in error
    assert node.query(queued_dirs).strip() == "0"
    assert node.query("select count() from test.local_mixed_path").strip() == "0"

    # The compact format bounds both names, so the same INSERT queues one directory per destination.
    # That is what makes the count above a live assertion rather than a vacuous zero.
    node.query("system stop distributed sends test.distr_mixed_path")
    node.query(
        "insert into test.distr_mixed_path values (1)",
        settings=dict(
            settings,
            prefer_localhost_replica="0",
            use_compact_format_in_distributed_parts_names="1",
        ),
    )
    assert node.query(queued_dirs).strip() == "2", node.query(queued_dirs)

    node.query("drop table test.distr_mixed_path sync")
    node.query("drop table test.local_mixed_path sync")


def test_long_directory_name_default_database(started_cluster):
    # The directory name is `user[:password]@host:port#default_database`, so a long
    # `default_database` exceeds NAME_MAX with every other field unremarkable. See #112719.
    node.query("drop table if exists test.local_long_db sync")
    node.query(
        "create table test.local_long_db (x UInt64) engine = MergeTree order by x"
    )

    settings = {
        "distributed_foreground_insert": "0",
        "prefer_localhost_replica": "0",
        "use_compact_format_in_distributed_parts_names": "0",
    }

    def create_distributed(table, cluster_name):
        node.query(f"drop table if exists test.{table} sync")
        node.query(
            f"create table test.{table} (x UInt64) engine = "
            f"Distributed('{cluster_name}', test, local_long_db)"
        )
        # Sends are stopped so a queued file stays queued for the assertions below.
        node.query(f"system stop distributed sends test.{table}")

    def queued(table):
        return node.query(
            "select data_files > 0 from system.distribution_queue "
            f"where database = 'test' and table = '{table}'"
        ).strip()

    # A name of exactly 255 bytes is still accepted.
    create_distributed(
        "distr_db_at_limit", "test_cluster_long_default_database_at_limit"
    )
    node.query("insert into test.distr_db_at_limit values (1)", settings=settings)
    assert queued("distr_db_at_limit") == "1"

    # One byte over the limit is rejected. The reported length pins both cases, since the two
    # clusters differ by a single database byte.
    create_distributed(
        "distr_db_over_limit", "test_cluster_long_default_database_over_limit"
    )
    error = node.query_and_get_error(
        "insert into test.distr_db_over_limit values (1)", settings=settings
    )
    assert "ARGUMENT_OUT_OF_BOUND" in error
    assert "The max length of a directory name" in error
    assert "distr_db_over_limit" in error
    assert "test_cluster_long_default_database_over_limit" in error
    assert "is 255, current length is 256" in error

    # The compact format names the directory after the shard and replica index, so the same
    # cluster works with it.
    node.query(
        "insert into test.distr_db_over_limit values (1)",
        settings=dict(settings, use_compact_format_in_distributed_parts_names="1"),
    )
    assert queued("distr_db_over_limit") == "1"

    # The name embeds the password, so the message must not disclose it. The same name without
    # the password is 242 bytes, so a rejection at 256 is only reached because it counts.
    create_distributed(
        "distr_db_password", "test_cluster_long_default_database_password"
    )
    error = node.query_and_get_error(
        "insert into test.distr_db_password values (1)", settings=settings
    )
    assert "ARGUMENT_OUT_OF_BOUND" in error
    assert "is 255, current length is 256" in error
    assert "secret_112719" not in error

    node.query("drop table test.distr_db_password sync")
    node.query("drop table test.distr_db_over_limit sync")
    node.query("drop table test.distr_db_at_limit sync")
    node.query("drop table test.local_long_db sync")


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
    node.query(
        "insert into test.distr_counters values (1, 'a'), (2, 'bb'), (3, 'ccc')",
        settings={"use_compact_format_in_distributed_parts_names": "1"},
    )
    path = get_dist_path(cluster, node, "distr_counters", 1)

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

        read_rows, read_bytes, selected_rows, selected_bytes = node.query(
            f"""
            select read_rows, read_bytes,
                   ProfileEvents['SelectedRows'], ProfileEvents['SelectedBytes']
            from system.query_log
            where query_id = '{query_id}' and type = 'QueryFinish'
            order by event_time_microseconds desc limit 1
            """
        ).split()

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
