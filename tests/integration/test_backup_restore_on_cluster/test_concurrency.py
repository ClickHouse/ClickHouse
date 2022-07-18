import pytest
import os.path
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


nodes = []
for i in range(num_nodes):
    nodes.append(
        cluster.add_instance(
            f"node{i}",
            main_configs=main_configs,
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
        f"SELECT status, error FROM system.backups WHERE status != 'BACKUP_COMPLETE' AND status != 'FAILED_TO_BACKUP' AND uuid IN {ids_list}",
        "",
    )

    for backup_name in backup_names:
        node0.query(f"DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")
        node0.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
        node0.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")
        for i in range(num_nodes):
            assert nodes[i].query("SELECT sum(x) FROM tbl") == TSV([expected_sum])


def test_concurrent_backups_on_different_nodes():
    create_and_fill_table()

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
            f"SELECT status, error FROM system.backups WHERE status != 'BACKUP_COMPLETE' AND status != 'FAILED_TO_BACKUP' AND uuid = '{ids[i]}'",
            "",
        )

    for i in range(num_concurrent_backups):
        assert nodes[i].query(
            f"SELECT status, error FROM system.backups WHERE uuid = '{ids[i]}'"
        ) == TSV([["BACKUP_COMPLETE", ""]])

    for i in range(num_concurrent_backups):
        nodes[i].query(f"DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")
        nodes[i].query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_names[i]}")
        nodes[i].query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")
        for j in range(num_nodes):
            assert nodes[j].query("SELECT sum(x) FROM tbl") == TSV([expected_sum])
