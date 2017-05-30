import os.path as p
import time
import datetime
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import TSV


def check_all_hosts_sucesfully_executed(tsv_content, num_hosts=None):
    if num_hosts is None:
        num_hosts = len(cluster.instances)

    M = TSV.toMat(tsv_content)
    hosts = [l[0] for l in M]
    codes = [l[1] for l in M]
    messages = [l[2] for l in M]

    assert len(hosts) == num_hosts and len(set(hosts)) == num_hosts, tsv_content
    assert len(set(codes)) == 1, tsv_content
    assert codes[0] == "0", tsv_content


def ddl_check_query(instance, query, num_hosts=None):
    contents = instance.query(query)
    check_all_hosts_sucesfully_executed(contents, num_hosts)
    return contents


cluster = ClickHouseCluster(__file__)
TEST_REPLICATED_ALTERS=True


@pytest.fixture(scope="module")
def started_cluster():
    try:
        for i in xrange(4):
            macroses = {"layer": 0, "shard": i/2 + 1, "replica": i%2 + 1}
            cluster.add_instance('ch{}'.format(i+1), config_dir="configs", macroses=macroses, with_zookeeper=True)

        cluster.start()

        # Initialize databases and service tables
        instance = cluster.instances['ch1']

        ddl_check_query(instance, """
CREATE TABLE IF NOT EXISTS all_tables ON CLUSTER 'cluster_no_replicas'
    (database String, name String, engine String, metadata_modification_time DateTime)
    ENGINE = Distributed('cluster_no_replicas', 'system', 'tables')
        """)

        ddl_check_query(instance, "CREATE DATABASE IF NOT EXISTS test ON CLUSTER 'cluster'")

        yield cluster

        ddl_check_query(instance, "DROP DATABASE test ON CLUSTER 'cluster'")
        ddl_check_query(instance, "DROP DATABASE IF EXISTS test2 ON CLUSTER 'cluster'")

    finally:
        pass
        cluster.shutdown()


def test_default_database(started_cluster):
    instance = cluster.instances['ch3']

    ddl_check_query(instance, "CREATE DATABASE IF NOT EXISTS test2 ON CLUSTER 'cluster'")
    ddl_check_query(instance, "DROP TABLE IF EXISTS null ON CLUSTER 'cluster'")
    ddl_check_query(instance, "CREATE TABLE null ON CLUSTER 'cluster2' (s String DEFAULT 'escape\t\nme') ENGINE = Null")

    contents = instance.query("SELECT hostName() AS h, database FROM all_tables WHERE name = 'null' ORDER BY h")
    assert TSV(contents) == TSV("ch1\tdefault\nch2\ttest2\nch3\tdefault\nch4\ttest2\n")

    ddl_check_query(instance, "DROP TABLE IF EXISTS null ON CLUSTER cluster2")
    ddl_check_query(instance, "DROP DATABASE IF EXISTS test2 ON CLUSTER 'cluster'")


def test_on_server_fail(started_cluster):
    instance = cluster.instances['ch1']
    kill_instance = cluster.instances['ch2']

    ddl_check_query(instance, "DROP TABLE IF EXISTS test.test_server_fail ON CLUSTER 'cluster'")

    kill_instance.get_docker_handle().stop()
    request = instance.get_query_request("CREATE TABLE test.test_server_fail ON CLUSTER 'cluster' (i Int8) ENGINE=Null", timeout=30)
    kill_instance.get_docker_handle().start()

    ddl_check_query(instance, "DROP TABLE IF EXISTS test.__nope__ ON CLUSTER 'cluster'")

    # Check query itself
    check_all_hosts_sucesfully_executed(request.get_answer())

    # And check query artefacts
    contents = instance.query("SELECT hostName() AS h FROM all_tables WHERE database='test' AND name='test_server_fail' ORDER BY h")
    assert TSV(contents) == TSV("ch1\nch2\nch3\nch4\n")

    ddl_check_query(instance, "DROP TABLE IF EXISTS test.test_server_fail ON CLUSTER 'cluster'")


def _test_on_connection_losses(cluster, zk_timeout):
    instance = cluster.instances['ch1']
    kill_instance = cluster.instances['ch2']

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(kill_instance)
        request = instance.get_query_request("DROP TABLE IF EXISTS test.__nope__ ON CLUSTER 'cluster'", timeout=10)
        time.sleep(zk_timeout)
        pm.restore_instance_zk_connections(kill_instance)

    check_all_hosts_sucesfully_executed(request.get_answer())


def test_on_connection_loss(started_cluster):
    _test_on_connection_losses(cluster, 1.5) # connection loss will occur only (3 sec ZK timeout in config)


def test_on_session_expired(started_cluster):
    _test_on_connection_losses(cluster, 4) # session should be expired (3 sec ZK timeout in config)


def test_replicated_alters(started_cluster):
    instance = cluster.instances['ch2']

    ddl_check_query(instance, "DROP TABLE IF EXISTS merge ON CLUSTER cluster")
    ddl_check_query(instance, "DROP TABLE IF EXISTS all_merge_32 ON CLUSTER cluster")
    ddl_check_query(instance, "DROP TABLE IF EXISTS all_merge_64 ON CLUSTER cluster")

    if not TEST_REPLICATED_ALTERS:
        return

    ddl_check_query(instance, """
CREATE TABLE IF NOT EXISTS merge ON CLUSTER cluster (p Date, i Int32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/hits', '{replica}', p, p, 1)
""")
    ddl_check_query(instance, """
CREATE TABLE IF NOT EXISTS all_merge_32 ON CLUSTER cluster (p Date, i Int32)
ENGINE = Distributed(cluster, default, merge, i)
""")
    ddl_check_query(instance, """
CREATE TABLE IF NOT EXISTS all_merge_64 ON CLUSTER cluster (p Date, i Int64, s String)
ENGINE = Distributed(cluster, default, merge, i)
""")

    for i in xrange(4):
        k = (i / 2) * 2
        cluster.instances['ch{}'.format(i + 1)].query("INSERT INTO merge (i) VALUES ({})({})".format(k, k+1))

    assert TSV(instance.query("SELECT i FROM all_merge_32 ORDER BY i")) == TSV(''.join(['{}\n'.format(x) for x in xrange(4)]))


    ddl_check_query(instance, "ALTER TABLE merge ON CLUSTER cluster MODIFY COLUMN i Int64")
    ddl_check_query(instance, "ALTER TABLE merge ON CLUSTER cluster ADD COLUMN s DEFAULT toString(i)")

    assert TSV(instance.query("SELECT i, s FROM all_merge_64 ORDER BY i")) == TSV(''.join(['{}\t{}\n'.format(x,x) for x in xrange(4)]))


    for i in xrange(4):
        k = (i / 2) * 2 + 4
        cluster.instances['ch{}'.format(i + 1)].query("INSERT INTO merge (p, i) VALUES (31, {})(31, {})".format(k, k+1))

    assert TSV(instance.query("SELECT i, s FROM all_merge_64 ORDER BY i")) == TSV(''.join(['{}\t{}\n'.format(x,x) for x in xrange(8)]))


    ddl_check_query(instance, "ALTER TABLE merge ON CLUSTER cluster DETACH PARTITION 197002")
    assert TSV(instance.query("SELECT i, s FROM all_merge_64 ORDER BY i")) == TSV(''.join(['{}\t{}\n'.format(x,x) for x in xrange(4)]))

    ddl_check_query(instance, "DROP TABLE merge ON CLUSTER cluster")
    ddl_check_query(instance, "DROP TABLE all_merge_32 ON CLUSTER cluster")
    ddl_check_query(instance, "DROP TABLE all_merge_64 ON CLUSTER cluster")


def test_simple_alters(started_cluster):
    instance = cluster.instances['ch2']

    ddl_check_query(instance, "DROP TABLE IF EXISTS merge ON CLUSTER cluster_without_replication")
    ddl_check_query(instance, "DROP TABLE IF EXISTS all_merge_32 ON CLUSTER cluster_without_replication")
    ddl_check_query(instance, "DROP TABLE IF EXISTS all_merge_64 ON CLUSTER cluster_without_replication")

    ddl_check_query(instance, """
CREATE TABLE IF NOT EXISTS merge ON CLUSTER cluster_without_replication (p Date, i Int32)
ENGINE = MergeTree(p, p, 1)
""")
    ddl_check_query(instance, """
CREATE TABLE IF NOT EXISTS all_merge_32 ON CLUSTER cluster_without_replication (p Date, i Int32)
ENGINE = Distributed(cluster_without_replication, default, merge, i)
""")
    ddl_check_query(instance, """
CREATE TABLE IF NOT EXISTS all_merge_64 ON CLUSTER cluster_without_replication (p Date, i Int64, s String)
ENGINE = Distributed(cluster_without_replication, default, merge, i)
""")

    for i in xrange(4):
        k = (i / 2) * 2
        cluster.instances['ch{}'.format(i + 1)].query("INSERT INTO merge (i) VALUES ({})({})".format(k, k+1))

    assert TSV(instance.query("SELECT i FROM all_merge_32 ORDER BY i")) == TSV(''.join(['{}\n'.format(x) for x in xrange(4)]))


    ddl_check_query(instance, "ALTER TABLE merge ON CLUSTER cluster_without_replication MODIFY COLUMN i Int64")
    ddl_check_query(instance, "ALTER TABLE merge ON CLUSTER cluster_without_replication ADD COLUMN s DEFAULT toString(i)")

    assert TSV(instance.query("SELECT i, s FROM all_merge_64 ORDER BY i")) == TSV(''.join(['{}\t{}\n'.format(x,x) for x in xrange(4)]))


    for i in xrange(4):
        k = (i / 2) * 2 + 4
        cluster.instances['ch{}'.format(i + 1)].query("INSERT INTO merge (p, i) VALUES (31, {})(31, {})".format(k, k+1))

    assert TSV(instance.query("SELECT i, s FROM all_merge_64 ORDER BY i")) == TSV(''.join(['{}\t{}\n'.format(x,x) for x in xrange(8)]))


    ddl_check_query(instance, "ALTER TABLE merge ON CLUSTER cluster_without_replication DETACH PARTITION 197002")
    assert TSV(instance.query("SELECT i, s FROM all_merge_64 ORDER BY i")) == TSV(''.join(['{}\t{}\n'.format(x,x) for x in xrange(4)]))

    ddl_check_query(instance, "DROP TABLE merge ON CLUSTER cluster_without_replication")
    ddl_check_query(instance, "DROP TABLE all_merge_32 ON CLUSTER cluster_without_replication")
    ddl_check_query(instance, "DROP TABLE all_merge_64 ON CLUSTER cluster_without_replication")
