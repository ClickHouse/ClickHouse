from random import randint
import pytest
import os.path
import time
import concurrent
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry


cluster = ClickHouseCluster(__file__)

num_nodes = 10


def generate_cluster_def():
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "./_gen/cluster_for_concurrency_test.xml",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("<clickhouse>\n\t<remote_servers>\n\t\t<cluster>\n\t\t\t<shard>\n")
        for i in range(num_nodes):
            f.write(
                f"\t\t\t\t<replica>\n\t\t\t\t\t<host>node{i}</host>\n\t\t\t\t\t<port>9000</port>\n\t\t\t\t</replica>\n"
            )
        f.write("\t\t\t</shard>\n\t\t</cluster>\n\t</remote_servers>\n</clickhouse>")
    return path


main_configs = ["configs/backups_disk.xml", generate_cluster_def()]

user_configs = ["configs/allow_database_types.xml"]

nodes = []
for i in range(num_nodes):
    nodes.append(
        cluster.add_instance(
            f"node{i}",
            main_configs=main_configs,
            user_configs=user_configs,
            external_dirs=["/backups/"],
            macros={"replica": f"node{i}", "shard": "shard1"},
            with_zookeeper=True,
        )
    )

node0 = nodes[0]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_after_test():
    try:
        yield
    finally:
        node0.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster' NO DELAY")
        node0.query("DROP DATABASE IF EXISTS mydb ON CLUSTER 'cluster' NO DELAY")


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}')"


def create_and_fill_table():
    node0.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x Int32"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )
    for i in range(num_nodes):
        nodes[i].query(f"INSERT INTO tbl VALUES ({i})")


expected_sum = num_nodes * (num_nodes - 1) // 2


def test_replicated_table():
    create_and_fill_table()

    backup_name = new_backup_name()
    node0.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node0.query(f"DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")
    node0.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node0.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    for i in range(num_nodes):
        assert nodes[i].query("SELECT sum(x) FROM tbl") == TSV([expected_sum])


num_concurrent_backups = 4


def test_concurrent_backups_on_same_node():
    create_and_fill_table()

    backup_names = [new_backup_name() for _ in range(num_concurrent_backups)]

    ids = []
    for backup_name in backup_names:
        id = node0.query(
            f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} ASYNC"
        ).split("\t")[0]
        ids.append(id)

    ids_list = "[" + ", ".join([f"'{id}'" for id in ids]) + "]"

    assert_eq_with_retry(
        node0,
        f"SELECT status FROM system.backups WHERE status == 'CREATING_BACKUP' AND id IN {ids_list}",
        "",
    )

    assert node0.query(
        f"SELECT status, error FROM system.backups WHERE id IN {ids_list}"
    ) == TSV([["BACKUP_CREATED", ""]] * num_concurrent_backups)

    for backup_name in backup_names:
        node0.query(f"DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")
        node0.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
        node0.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")
        for i in range(num_nodes):
            assert nodes[i].query("SELECT sum(x) FROM tbl") == TSV([expected_sum])


def test_concurrent_backups_on_different_nodes():
    create_and_fill_table()

    assert num_concurrent_backups <= num_nodes
    backup_names = [new_backup_name() for _ in range(num_concurrent_backups)]

    ids = []
    for i in range(num_concurrent_backups):
        id = (
            nodes[i]
            .query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_names[i]} ASYNC")
            .split("\t")[0]
        )
        ids.append(id)

    for i in range(num_concurrent_backups):
        assert_eq_with_retry(
            nodes[i],
            f"SELECT status FROM system.backups WHERE status == 'CREATING_BACKUP' AND id = '{ids[i]}'",
            "",
        )

    for i in range(num_concurrent_backups):
        assert nodes[i].query(
            f"SELECT status, error FROM system.backups WHERE id = '{ids[i]}'"
        ) == TSV([["BACKUP_CREATED", ""]])

    for i in range(num_concurrent_backups):
        nodes[i].query(f"DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")
        nodes[i].query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_names[i]}")
        nodes[i].query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")
        for j in range(num_nodes):
            assert nodes[j].query("SELECT sum(x) FROM tbl") == TSV([expected_sum])


@pytest.mark.parametrize(
    "db_engine, table_engine",
    [("Replicated", "ReplicatedMergeTree"), ("Ordinary", "MergeTree")],
)
def test_create_or_drop_tables_during_backup(db_engine, table_engine):
    if db_engine == "Replicated":
        db_engine = "Replicated('/clickhouse/path/','{shard}','{replica}')"
    if table_engine.endswith("MergeTree"):
        table_engine += " ORDER BY tuple()"

    node0.query(f"CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE={db_engine}")

    # Will do this test for 60 seconds
    start_time = time.time()
    end_time = start_time + 60

    def create_table():
        while time.time() < end_time:
            node = nodes[randint(0, num_nodes - 1)]
            table_name = f"mydb.tbl{randint(1, num_nodes)}"
            node.query(
                f"CREATE TABLE IF NOT EXISTS {table_name}(x Int32) ENGINE={table_engine}"
            )
            node.query_and_get_answer_with_error(
                f"INSERT INTO {table_name} SELECT rand32() FROM numbers(10)"
            )

    def drop_table():
        while time.time() < end_time:
            table_name = f"mydb.tbl{randint(1, num_nodes)}"
            node = nodes[randint(0, num_nodes - 1)]
            node.query(f"DROP TABLE IF EXISTS {table_name} NO DELAY")

    def rename_table():
        while time.time() < end_time:
            table_name1 = f"mydb.tbl{randint(1, num_nodes)}"
            table_name2 = f"mydb.tbl{randint(1, num_nodes)}"
            node = nodes[randint(0, num_nodes - 1)]
            node.query_and_get_answer_with_error(
                f"RENAME TABLE {table_name1} TO {table_name2}"
            )

    def make_backup():
        ids = []
        while time.time() < end_time:
            time.sleep(
                5
            )  # 1 minute total, and around 5 seconds per each backup => around 12 backups should be created
            backup_name = new_backup_name()
            id = node0.query(
                f"BACKUP DATABASE mydb ON CLUSTER 'cluster' TO {backup_name} ASYNC"
            ).split("\t")[0]
            ids.append(id)
        return ids

    ids = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        ids_future = executor.submit(make_backup)
        futures.append(ids_future)
        futures.append(executor.submit(create_table))
        futures.append(executor.submit(drop_table))
        futures.append(executor.submit(rename_table))
        for future in futures:
            future.result()
        ids = ids_future.result()

    ids_list = "[" + ", ".join([f"'{id}'" for id in ids]) + "]"
    for node in nodes:
        assert_eq_with_retry(
            node,
            f"SELECT status from system.backups WHERE id IN {ids_list} AND (status == 'CREATING_BACKUP')",
            "",
        )

    for node in nodes:
        assert_eq_with_retry(
            node,
            f"SELECT status, error from system.backups WHERE id IN {ids_list} AND (status == 'BACKUP_FAILED')",
            "",
        )

    backup_names = {}
    for node in nodes:
        for id in ids:
            backup_name = node.query(
                f"SELECT name FROM system.backups WHERE id='{id}' FORMAT RawBLOB"
            ).strip()
            if backup_name:
                backup_names[id] = backup_name

    for id in ids:
        node0.query("DROP DATABASE mydb ON CLUSTER 'cluster'")
        node0.query(
            f"RESTORE DATABASE mydb ON CLUSTER 'cluster' FROM {backup_names[id]}"
        )
