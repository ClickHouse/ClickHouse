import os
import random
import sys
import time
import kazoo
import pytest
import string
import random
from contextlib import contextmanager
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

import docker

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(CURRENT_TEST_DIR))

COPYING_FAIL_PROBABILITY = 0.2
MOVING_FAIL_PROBABILITY = 0.2

cluster = ClickHouseCluster(__file__)


def generateRandomString(count):
    return "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(count)
    )


def check_all_hosts_sucesfully_executed(tsv_content, num_hosts):
    M = TSV.toMat(tsv_content)
    hosts = [(l[0], l[1]) for l in M]  # (host, port)
    codes = [l[2] for l in M]

    assert len(hosts) == num_hosts and len(set(hosts)) == num_hosts, "\n" + tsv_content
    assert len(set(codes)) == 1, "\n" + tsv_content
    assert codes[0] == "0", "\n" + tsv_content


def ddl_check_query(instance, query, num_hosts=3):
    contents = instance.query(query)
    check_all_hosts_sucesfully_executed(contents, num_hosts)
    return contents


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        clusters_schema = {
            "0": {"0": ["0", "1"], "1": ["0"]},
            "1": {"0": ["0", "1"], "1": ["0"]},
        }

        for cluster_name, shards in clusters_schema.items():
            for shard_name, replicas in shards.items():
                for replica_name in replicas:
                    name = "s{}_{}_{}".format(cluster_name, shard_name, replica_name)
                    cluster.add_instance(
                        name,
                        main_configs=[
                            "configs/conf.d/query_log.xml",
                            "configs/conf.d/ddl.xml",
                            "configs/conf.d/clusters.xml",
                        ],
                        user_configs=["configs/users.xml"],
                        macros={
                            "cluster": cluster_name,
                            "shard": shard_name,
                            "replica": replica_name,
                        },
                        with_zookeeper=True,
                    )

        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


class Task1:
    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = "/clickhouse-copier/task_simple_" + generateRandomString(10)
        self.container_task_file = "/task0_description.xml"

        for instance_name, _ in cluster.instances.items():
            instance = cluster.instances[instance_name]
            instance.copy_file_to_container(
                os.path.join(CURRENT_TEST_DIR, "./task0_description.xml"),
                self.container_task_file,
            )
            print(
                "Copied task file to container of '{}' instance. Path {}".format(
                    instance_name, self.container_task_file
                )
            )

    def start(self):
        instance = cluster.instances["s0_0_0"]

        for cluster_num in ["0", "1"]:
            ddl_check_query(
                instance,
                "DROP DATABASE IF EXISTS default ON CLUSTER cluster{} SYNC".format(
                    cluster_num
                ),
            )
            ddl_check_query(
                instance,
                "CREATE DATABASE default ON CLUSTER cluster{} ".format(cluster_num),
            )

        ddl_check_query(
            instance,
            "CREATE TABLE hits ON CLUSTER cluster0 (d UInt64, d1 UInt64 MATERIALIZED d+1) "
            + "ENGINE=ReplicatedMergeTree "
            + "PARTITION BY d % 3 ORDER BY (d, sipHash64(d)) SAMPLE BY sipHash64(d) SETTINGS index_granularity = 16",
        )
        ddl_check_query(
            instance,
            "CREATE TABLE hits_all ON CLUSTER cluster0 (d UInt64) ENGINE=Distributed(cluster0, default, hits, d)",
        )
        ddl_check_query(
            instance,
            "CREATE TABLE hits_all ON CLUSTER cluster1 (d UInt64) ENGINE=Distributed(cluster1, default, hits, d + 1)",
        )
        instance.query(
            "INSERT INTO hits_all SELECT * FROM system.numbers LIMIT 1002",
            settings={"insert_distributed_sync": 1},
        )

    def check(self):
        assert (
            self.cluster.instances["s0_0_0"]
            .query("SELECT count() FROM hits_all")
            .strip()
            == "1002"
        )
        assert (
            self.cluster.instances["s1_0_0"]
            .query("SELECT count() FROM hits_all")
            .strip()
            == "1002"
        )

        assert (
            self.cluster.instances["s1_0_0"]
            .query("SELECT DISTINCT d % 2 FROM hits")
            .strip()
            == "1"
        )
        assert (
            self.cluster.instances["s1_1_0"]
            .query("SELECT DISTINCT d % 2 FROM hits")
            .strip()
            == "0"
        )

        instance = self.cluster.instances["s0_0_0"]
        ddl_check_query(instance, "DROP TABLE hits_all ON CLUSTER cluster0")
        ddl_check_query(instance, "DROP TABLE hits_all ON CLUSTER cluster1")
        ddl_check_query(instance, "DROP TABLE hits ON CLUSTER cluster0")
        ddl_check_query(instance, "DROP TABLE hits ON CLUSTER cluster1")


class Task2:
    def __init__(self, cluster, unique_zk_path):
        self.cluster = cluster
        self.zk_task_path = (
            "/clickhouse-copier/task_month_to_week_partition_" + generateRandomString(5)
        )
        self.unique_zk_path = generateRandomString(10)
        self.container_task_file = "/task_month_to_week_description.xml"

        for instance_name, _ in cluster.instances.items():
            instance = cluster.instances[instance_name]
            instance.copy_file_to_container(
                os.path.join(CURRENT_TEST_DIR, "./task_month_to_week_description.xml"),
                self.container_task_file,
            )
            print(
                "Copied task file to container of '{}' instance. Path {}".format(
                    instance_name, self.container_task_file
                )
            )

    def start(self):
        instance = cluster.instances["s0_0_0"]

        for cluster_num in ["0", "1"]:
            ddl_check_query(
                instance,
                "DROP DATABASE IF EXISTS default ON CLUSTER cluster{}".format(
                    cluster_num
                ),
            )
            ddl_check_query(
                instance,
                "CREATE DATABASE IF NOT EXISTS default ON CLUSTER cluster{}".format(
                    cluster_num
                ),
            )

        ddl_check_query(
            instance,
            "CREATE TABLE a ON CLUSTER cluster0 (date Date, d UInt64, d1 UInt64 ALIAS d+1) "
            "ENGINE=ReplicatedMergeTree('/clickhouse/tables/cluster_{cluster}/{shard}/"
            + self.unique_zk_path
            + "', "
            "'{replica}', date, intHash64(d), (date, intHash64(d)), 8192)",
        )
        ddl_check_query(
            instance,
            "CREATE TABLE a_all ON CLUSTER cluster0 (date Date, d UInt64) ENGINE=Distributed(cluster0, default, a, d)",
        )

        instance.query(
            "INSERT INTO a_all SELECT toDate(17581 + number) AS date, number AS d FROM system.numbers LIMIT 85",
            settings={"insert_distributed_sync": 1},
        )

    def check(self):
        assert TSV(
            self.cluster.instances["s0_0_0"].query(
                "SELECT count() FROM cluster(cluster0, default, a)"
            )
        ) == TSV("85\n")
        assert TSV(
            self.cluster.instances["s1_0_0"].query(
                "SELECT count(), uniqExact(date) FROM cluster(cluster1, default, b)"
            )
        ) == TSV("85\t85\n")

        assert TSV(
            self.cluster.instances["s1_0_0"].query(
                "SELECT DISTINCT jumpConsistentHash(intHash64(d), 2) FROM b"
            )
        ) == TSV("0\n")
        assert TSV(
            self.cluster.instances["s1_1_0"].query(
                "SELECT DISTINCT jumpConsistentHash(intHash64(d), 2) FROM b"
            )
        ) == TSV("1\n")

        assert TSV(
            self.cluster.instances["s1_0_0"].query(
                "SELECT uniqExact(partition) IN (12, 13) FROM system.parts WHERE active AND database='default' AND table='b'"
            )
        ) == TSV("1\n")
        assert TSV(
            self.cluster.instances["s1_1_0"].query(
                "SELECT uniqExact(partition) IN (12, 13) FROM system.parts WHERE active AND database='default' AND table='b'"
            )
        ) == TSV("1\n")

        instance = cluster.instances["s0_0_0"]
        ddl_check_query(instance, "DROP TABLE a ON CLUSTER cluster0")
        ddl_check_query(instance, "DROP TABLE b ON CLUSTER cluster1")


class Task_test_block_size:
    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = (
            "/clickhouse-copier/task_test_block_size_" + generateRandomString(5)
        )
        self.rows = 1000000
        self.container_task_file = "/task_test_block_size.xml"

        for instance_name, _ in cluster.instances.items():
            instance = cluster.instances[instance_name]
            instance.copy_file_to_container(
                os.path.join(CURRENT_TEST_DIR, "./task_test_block_size.xml"),
                self.container_task_file,
            )
            print(
                "Copied task file to container of '{}' instance. Path {}".format(
                    instance_name, self.container_task_file
                )
            )

    def start(self):
        instance = cluster.instances["s0_0_0"]

        ddl_check_query(
            instance,
            """
            CREATE TABLE test_block_size ON CLUSTER shard_0_0 (partition Date, d UInt64)
            ENGINE=ReplicatedMergeTree
            ORDER BY (d, sipHash64(d)) SAMPLE BY sipHash64(d)""",
            2,
        )

        instance.query(
            "INSERT INTO test_block_size SELECT toDate(0) AS partition, number as d FROM system.numbers LIMIT {}".format(
                self.rows
            )
        )

    def check(self):
        assert TSV(
            self.cluster.instances["s1_0_0"].query(
                "SELECT count() FROM cluster(cluster1, default, test_block_size)"
            )
        ) == TSV("{}\n".format(self.rows))

        instance = cluster.instances["s0_0_0"]
        ddl_check_query(instance, "DROP TABLE test_block_size ON CLUSTER shard_0_0", 2)
        ddl_check_query(instance, "DROP TABLE test_block_size ON CLUSTER cluster1")


class Task_no_index:
    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = "/clickhouse-copier/task_no_index_" + generateRandomString(
            5
        )
        self.rows = 1000000
        self.container_task_file = "/task_no_index.xml"

        for instance_name, _ in cluster.instances.items():
            instance = cluster.instances[instance_name]
            instance.copy_file_to_container(
                os.path.join(CURRENT_TEST_DIR, "./task_no_index.xml"),
                self.container_task_file,
            )
            print(
                "Copied task file to container of '{}' instance. Path {}".format(
                    instance_name, self.container_task_file
                )
            )

    def start(self):
        instance = cluster.instances["s0_0_0"]
        instance.query("DROP TABLE IF EXISTS ontime SYNC")
        instance.query(
            "create table IF NOT EXISTS ontime (Year UInt16, FlightDate String) ENGINE = Memory"
        )
        instance.query(
            "insert into ontime values (2016, 'test6'), (2017, 'test7'), (2018, 'test8')"
        )

    def check(self):
        assert TSV(
            self.cluster.instances["s1_1_0"].query("SELECT Year FROM ontime22")
        ) == TSV("2017\n")
        instance = cluster.instances["s0_0_0"]
        instance.query("DROP TABLE ontime")
        instance = cluster.instances["s1_1_0"]
        instance.query("DROP TABLE ontime22")


class Task_no_arg:
    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = "/clickhouse-copier/task_no_arg"
        self.rows = 1000000
        self.container_task_file = "/task_no_arg.xml"

        for instance_name, _ in cluster.instances.items():
            instance = cluster.instances[instance_name]
            instance.copy_file_to_container(
                os.path.join(CURRENT_TEST_DIR, "./task_no_arg.xml"),
                self.container_task_file,
            )
            print(
                "Copied task file to container of '{}' instance. Path {}".format(
                    instance_name, self.container_task_file
                )
            )

    def start(self):
        instance = cluster.instances["s0_0_0"]
        instance.query("DROP TABLE IF EXISTS copier_test1 SYNC")
        instance.query(
            "create table if not exists copier_test1 (date Date, id UInt32) engine = MergeTree PARTITION BY date ORDER BY date SETTINGS index_granularity = 8192"
        )
        instance.query("insert into copier_test1 values ('2016-01-01', 10);")

    def check(self):
        assert TSV(
            self.cluster.instances["s1_1_0"].query("SELECT date FROM copier_test1_1")
        ) == TSV("2016-01-01\n")
        instance = cluster.instances["s0_0_0"]
        instance.query("DROP TABLE copier_test1 SYNC")
        instance = cluster.instances["s1_1_0"]
        instance.query("DROP TABLE copier_test1_1 SYNC")


class Task_non_partitioned_table:
    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = "/clickhouse-copier/task_non_partitoned_table"
        self.rows = 1000000
        self.container_task_file = "/task_non_partitioned_table.xml"

        for instance_name, _ in cluster.instances.items():
            instance = cluster.instances[instance_name]
            instance.copy_file_to_container(
                os.path.join(CURRENT_TEST_DIR, "./task_non_partitioned_table.xml"),
                self.container_task_file,
            )
            print(
                "Copied task file to container of '{}' instance. Path {}".format(
                    instance_name, self.container_task_file
                )
            )

    def start(self):
        instance = cluster.instances["s0_0_0"]
        instance.query("DROP TABLE IF EXISTS copier_test1 SYNC")
        instance.query(
            "create table copier_test1 (date Date, id UInt32) engine = MergeTree ORDER BY date SETTINGS index_granularity = 8192"
        )
        instance.query("insert into copier_test1 values ('2016-01-01', 10);")

    def check(self):
        assert TSV(
            self.cluster.instances["s1_1_0"].query("SELECT date FROM copier_test1_1")
        ) == TSV("2016-01-01\n")
        instance = cluster.instances["s0_0_0"]
        instance.query("DROP TABLE copier_test1")
        instance = cluster.instances["s1_1_0"]
        instance.query("DROP TABLE copier_test1_1")


class Task_self_copy:
    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = "/clickhouse-copier/task_self_copy"
        self.container_task_file = "/task_self_copy.xml"

        for instance_name, _ in cluster.instances.items():
            instance = cluster.instances[instance_name]
            instance.copy_file_to_container(
                os.path.join(CURRENT_TEST_DIR, "./task_self_copy.xml"),
                self.container_task_file,
            )
            print(
                "Copied task file to container of '{}' instance. Path {}".format(
                    instance_name, self.container_task_file
                )
            )

    def start(self):
        instance = cluster.instances["s0_0_0"]
        instance.query("DROP DATABASE IF EXISTS db1 SYNC")
        instance.query("DROP DATABASE IF EXISTS db2 SYNC")
        instance.query("CREATE DATABASE IF NOT EXISTS db1;")
        instance.query(
            "CREATE TABLE IF NOT EXISTS db1.source_table (`a` Int8, `b` String, `c` Int8) ENGINE = MergeTree PARTITION BY a ORDER BY a SETTINGS index_granularity = 8192"
        )
        instance.query("CREATE DATABASE IF NOT EXISTS db2;")
        instance.query(
            "CREATE TABLE IF NOT EXISTS db2.destination_table (`a` Int8, `b` String, `c` Int8) ENGINE = MergeTree PARTITION BY a ORDER BY a SETTINGS index_granularity = 8192"
        )
        instance.query("INSERT INTO db1.source_table VALUES (1, 'ClickHouse', 1);")
        instance.query("INSERT INTO db1.source_table VALUES (2, 'Copier', 2);")

    def check(self):
        instance = cluster.instances["s0_0_0"]
        assert TSV(
            instance.query("SELECT * FROM db2.destination_table ORDER BY a")
        ) == TSV(instance.query("SELECT * FROM db1.source_table ORDER BY a"))
        instance = cluster.instances["s0_0_0"]
        instance.query("DROP DATABASE IF EXISTS db1 SYNC")
        instance.query("DROP DATABASE IF EXISTS db2 SYNC")


def execute_task(started_cluster, task, cmd_options):
    task.start()

    zk = started_cluster.get_kazoo_client("zoo1")
    print("Use ZooKeeper server: {}:{}".format(zk.hosts[0][0], zk.hosts[0][1]))

    try:
        zk.delete("/clickhouse-copier", recursive=True)
    except kazoo.exceptions.NoNodeError:
        print("No node /clickhouse-copier. It is Ok in first test.")

    # Run cluster-copier processes on each node
    docker_api = started_cluster.docker_client.api
    copiers_exec_ids = []

    cmd = [
        "/usr/bin/clickhouse",
        "copier",
        "--config",
        "/etc/clickhouse-server/config-copier.xml",
        "--task-path",
        task.zk_task_path,
        "--task-file",
        task.container_task_file,
        "--task-upload-force",
        "true",
        "--base-dir",
        "/var/log/clickhouse-server/copier",
    ]
    cmd += cmd_options

    print(cmd)

    copiers = random.sample(list(started_cluster.instances.keys()), 3)

    for instance_name in copiers:
        instance = started_cluster.instances[instance_name]
        container = instance.get_docker_handle()
        instance.copy_file_to_container(
            os.path.join(CURRENT_TEST_DIR, "configs/config-copier.xml"),
            "/etc/clickhouse-server/config-copier.xml",
        )
        print("Copied copier config to {}".format(instance.name))
        exec_id = docker_api.exec_create(container.id, cmd, stderr=True)
        output = docker_api.exec_start(exec_id).decode("utf8")
        print(output)
        copiers_exec_ids.append(exec_id)
        print(
            "Copier for {} ({}) has started".format(instance.name, instance.ip_address)
        )

    # Wait for copiers stopping and check their return codes
    for exec_id, instance_name in zip(copiers_exec_ids, copiers):
        instance = started_cluster.instances[instance_name]
        while True:
            res = docker_api.exec_inspect(exec_id)
            if not res["Running"]:
                break
            time.sleep(0.5)

        assert res["ExitCode"] == 0, "Instance: {} ({}). Info: {}".format(
            instance.name, instance.ip_address, repr(res)
        )

    try:
        task.check()
    finally:
        zk.delete(task.zk_task_path, recursive=True)


# Tests


@pytest.mark.parametrize(("use_sample_offset"), [False, True])
def test_copy_simple(started_cluster, use_sample_offset):
    if use_sample_offset:
        execute_task(
            started_cluster,
            Task1(started_cluster),
            ["--experimental-use-sample-offset", "1"],
        )
    else:
        execute_task(started_cluster, Task1(started_cluster), [])


@pytest.mark.parametrize(("use_sample_offset"), [False, True])
def test_copy_with_recovering(started_cluster, use_sample_offset):
    if use_sample_offset:
        execute_task(
            started_cluster,
            Task1(started_cluster),
            [
                "--copy-fault-probability",
                str(COPYING_FAIL_PROBABILITY),
                "--experimental-use-sample-offset",
                "1",
            ],
        )
    else:
        execute_task(
            started_cluster,
            Task1(started_cluster),
            ["--copy-fault-probability", str(COPYING_FAIL_PROBABILITY)],
        )


@pytest.mark.parametrize(("use_sample_offset"), [False, True])
def test_copy_with_recovering_after_move_faults(started_cluster, use_sample_offset):
    if use_sample_offset:
        execute_task(
            started_cluster,
            Task1(started_cluster),
            [
                "--move-fault-probability",
                str(MOVING_FAIL_PROBABILITY),
                "--experimental-use-sample-offset",
                "1",
            ],
        )
    else:
        execute_task(
            started_cluster,
            Task1(started_cluster),
            ["--move-fault-probability", str(MOVING_FAIL_PROBABILITY)],
        )


@pytest.mark.timeout(600)
def test_copy_month_to_week_partition(started_cluster):
    execute_task(started_cluster, Task2(started_cluster, "test1"), [])


@pytest.mark.timeout(600)
def test_copy_month_to_week_partition_with_recovering(started_cluster):
    execute_task(
        started_cluster,
        Task2(started_cluster, "test2"),
        ["--copy-fault-probability", str(COPYING_FAIL_PROBABILITY)],
    )


@pytest.mark.timeout(600)
def test_copy_month_to_week_partition_with_recovering_after_move_faults(
    started_cluster,
):
    execute_task(
        started_cluster,
        Task2(started_cluster, "test3"),
        ["--move-fault-probability", str(MOVING_FAIL_PROBABILITY)],
    )


def test_block_size(started_cluster):
    execute_task(started_cluster, Task_test_block_size(started_cluster), [])


def test_no_index(started_cluster):
    execute_task(started_cluster, Task_no_index(started_cluster), [])


def test_no_arg(started_cluster):
    execute_task(started_cluster, Task_no_arg(started_cluster), [])


def test_non_partitioned_table(started_cluster):
    execute_task(started_cluster, Task_non_partitioned_table(started_cluster), [])


def test_self_copy(started_cluster):
    execute_task(started_cluster, Task_self_copy(started_cluster), [])
