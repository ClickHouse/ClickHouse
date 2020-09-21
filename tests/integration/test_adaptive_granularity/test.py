import time
import pytest

from helpers.cluster import ClickHouseCluster
from multiprocessing.dummy import Pool
from helpers.client import QueryRuntimeException, QueryTimeoutExceedException

from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', config_dir="configs", main_configs=['configs/remote_servers.xml', 'configs/log_conf.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', config_dir="configs", main_configs=['configs/remote_servers.xml', 'configs/log_conf.xml'], with_zookeeper=True)

node3 = cluster.add_instance('node3', config_dir="configs", main_configs=['configs/remote_servers.xml', 'configs/log_conf.xml'], with_zookeeper=True, image='yandex/clickhouse-server:19.6.3.18', with_installed_binary=True)
node4 = cluster.add_instance('node4', config_dir="configs", main_configs=['configs/remote_servers.xml', 'configs/log_conf.xml'], with_zookeeper=True)

node5 = cluster.add_instance('node5', config_dir="configs", main_configs=['configs/remote_servers.xml', 'configs/log_conf.xml'], with_zookeeper=True, image='yandex/clickhouse-server:19.1.15', with_installed_binary=True)
node6 = cluster.add_instance('node6', config_dir="configs", main_configs=['configs/remote_servers.xml', 'configs/log_conf.xml'], with_zookeeper=True)

node7 = cluster.add_instance('node7', config_dir="configs", main_configs=['configs/remote_servers.xml', 'configs/log_conf.xml'], with_zookeeper=True, image='yandex/clickhouse-server:19.6.3.18', stay_alive=True, with_installed_binary=True)
node8 = cluster.add_instance('node8', config_dir="configs", main_configs=['configs/remote_servers.xml', 'configs/log_conf.xml'], with_zookeeper=True, image='yandex/clickhouse-server:19.1.15', stay_alive=True, with_installed_binary=True)

node9 = cluster.add_instance('node9', config_dir="configs", main_configs=['configs/remote_servers.xml', 'configs/log_conf.xml', 'configs/merge_tree_settings.xml'], with_zookeeper=True, image='yandex/clickhouse-server:19.1.15', stay_alive=True, with_installed_binary=True)
node10 = cluster.add_instance('node10', config_dir="configs", main_configs=['configs/remote_servers.xml', 'configs/log_conf.xml', 'configs/merge_tree_settings.xml'], with_zookeeper=True, image='yandex/clickhouse-server:19.6.3.18', stay_alive=True, with_installed_binary=True)

node11 = cluster.add_instance('node11', config_dir="configs", main_configs=['configs/remote_servers.xml', 'configs/log_conf.xml'], with_zookeeper=True, image='yandex/clickhouse-server:19.1.15', stay_alive=True, with_installed_binary=True)
node12 = cluster.add_instance('node12', config_dir="configs", main_configs=['configs/remote_servers.xml', 'configs/log_conf.xml'], with_zookeeper=True, image='yandex/clickhouse-server:19.1.15', stay_alive=True, with_installed_binary=True)


def prepare_single_pair_with_setting(first_node, second_node, group):
    for node in (first_node, second_node):
        node.query("CREATE DATABASE IF NOT EXISTS test")

    # Two tables with adaptive granularity
    first_node.query(
    '''
        CREATE TABLE table_by_default(date Date, id UInt32, dummy UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{g}/table_by_default', '1')
        PARTITION BY toYYYYMM(date)
        ORDER BY id
        SETTINGS index_granularity_bytes = 10485760
    '''.format(g=group))

    second_node.query(
    '''
        CREATE TABLE table_by_default(date Date, id UInt32, dummy UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{g}/table_by_default', '2')
        PARTITION BY toYYYYMM(date)
        ORDER BY id
        SETTINGS index_granularity_bytes = 10485760
    '''.format(g=group))

    # Two tables with fixed granularity
    first_node.query(
    '''
        CREATE TABLE table_with_fixed_granularity(date Date, id UInt32, dummy UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{g}/table_with_fixed_granularity', '1')
        PARTITION BY toYYYYMM(date)
        ORDER BY id
        SETTINGS index_granularity_bytes = 0
    '''.format(g=group))

    second_node.query(
    '''
        CREATE TABLE table_with_fixed_granularity(date Date, id UInt32, dummy UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{g}/table_with_fixed_granularity', '2')
        PARTITION BY toYYYYMM(date)
        ORDER BY id
        SETTINGS index_granularity_bytes = 0
    '''.format(g=group))

    # Two tables with different granularity
    with pytest.raises(QueryRuntimeException):
        first_node.query(
        '''
            CREATE TABLE table_with_different_granularity(date Date, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{g}/table_with_different_granularity', '1')
            PARTITION BY toYYYYMM(date)
            ORDER BY id
            SETTINGS index_granularity_bytes = 10485760
        '''.format(g=group))

        second_node.query(
        '''
            CREATE TABLE table_with_different_granularity(date Date, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{g}/table_with_different_granularity', '2')
            PARTITION BY toYYYYMM(date)
            ORDER BY id
            SETTINGS index_granularity_bytes = 0
        '''.format(g=group))

        # Two tables with different granularity, but enabled mixed parts
        first_node.query(
        '''
            CREATE TABLE table_with_mixed_granularity(date Date, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{g}/table_with_mixed_granularity', '1')
            PARTITION BY toYYYYMM(date)
            ORDER BY id
            SETTINGS index_granularity_bytes = 10485760, enable_mixed_granularity_parts=1
        '''.format(g=group))

        second_node.query(
        '''
            CREATE TABLE table_with_mixed_granularity(date Date, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{g}/table_with_mixed_granularity', '2')
            PARTITION BY toYYYYMM(date)
            ORDER BY id
            SETTINGS index_granularity_bytes = 0, enable_mixed_granularity_parts=1
        '''.format(g=group))


def prepare_single_pair_without_setting(first_node, second_node, group):
    for node in (first_node, second_node):
        node.query("CREATE DATABASE IF NOT EXISTS test")

    # Two tables with fixed granularity
    first_node.query(
    '''
        CREATE TABLE table_with_fixed_granularity(date Date, id UInt32, dummy UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{g}/table_with_fixed_granularity', '1')
        PARTITION BY toYYYYMM(date)
        ORDER BY id
    '''.format(g=group))

    second_node.query(
    '''
        CREATE TABLE table_with_fixed_granularity(date Date, id UInt32, dummy UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{g}/table_with_fixed_granularity', '2')
        PARTITION BY toYYYYMM(date)
        ORDER BY id
        SETTINGS index_granularity_bytes = 0
    '''.format(g=group))


@pytest.fixture(scope="module")
def start_static_cluster():
    try:
        cluster.start()

        prepare_single_pair_with_setting(node1, node2, "shard1")
        prepare_single_pair_with_setting(node3, node4, "shard2")
        prepare_single_pair_without_setting(node5, node6, "shard3")
        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    ('first_node', 'second_node', 'table'),
    [
        (node1, node2, 'table_by_default'),
        (node1, node2, 'table_with_fixed_granularity'),
        (node3, node4, 'table_by_default'),
        (node3, node4, 'table_with_fixed_granularity'),
        (node5, node6, 'table_with_fixed_granularity'),
    ]
)
def test_different_versions_cluster(start_static_cluster, first_node, second_node, table):
    counter = 1
    for n1, n2 in ((first_node, second_node), (second_node, first_node)):
        n1.query("INSERT INTO {tbl} VALUES (toDate('2018-10-01'), {c1}, 333), (toDate('2018-10-02'), {c2}, 444)".format(tbl=table, c1=counter * 2, c2=counter * 2 + 1))
        n2.query("SYSTEM SYNC REPLICA {tbl}".format(tbl=table))
        assert_eq_with_retry(n2, "SELECT count() from {tbl}".format(tbl=table), str(counter * 2))
        n1.query("DETACH TABLE {tbl}".format(tbl=table))
        n2.query("DETACH TABLE {tbl}".format(tbl=table))
        n1.query("ATTACH TABLE {tbl}".format(tbl=table))
        n2.query("ATTACH TABLE {tbl}".format(tbl=table))
        assert_eq_with_retry(n1, "SELECT count() from {tbl}".format(tbl=table), str(counter * 2))
        assert_eq_with_retry(n2, "SELECT count() from {tbl}".format(tbl=table), str(counter * 2))
        n1.query("OPTIMIZE TABLE {tbl} FINAL".format(tbl=table))
        n2.query("SYSTEM SYNC REPLICA {tbl}".format(tbl=table))
        assert_eq_with_retry(n1, "SELECT count() from {tbl}".format(tbl=table), str(counter * 2))
        assert_eq_with_retry(n2, "SELECT count() from {tbl}".format(tbl=table), str(counter * 2))
        counter += 1

@pytest.fixture(scope="module")
def start_dynamic_cluster():
    try:
        cluster.start()
        node7.query(
        '''
            CREATE TABLE table_with_default_granularity(date Date, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/7/table_with_default_granularity', '1')
            PARTITION BY toYYYYMM(date)
            ORDER BY id
        ''')

        node7.query(
        '''
            CREATE TABLE table_with_adaptive_default_granularity(date Date, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/7/table_with_adaptive_default_granularity', '1')
            PARTITION BY toYYYYMM(date)
            ORDER BY id
            SETTINGS index_granularity_bytes=10485760
        ''')

        node8.query(
        '''
            CREATE TABLE table_with_default_granularity(date Date, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/8/table_with_default_granularity', '1')
            PARTITION BY toYYYYMM(date)
            ORDER BY id
        ''')

        node9.query(
        '''
            CREATE TABLE table_with_default_granularity(date Date, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/9/table_with_default_granularity', '1')
            PARTITION BY toYYYYMM(date)
            ORDER BY id
        ''')

        node10.query(
        '''
            CREATE TABLE table_with_default_granularity(date Date, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/10/table_with_default_granularity', '1')
            PARTITION BY toYYYYMM(date)
            ORDER BY id
        ''')

        node11.query(
        '''
            CREATE TABLE table_with_default_granularity(date Date, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/shard11/table_with_default_granularity', '1')
            PARTITION BY toYYYYMM(date)
            ORDER BY id
        ''')

        node12.query(
        '''
            CREATE TABLE table_with_default_granularity(date Date, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/shard11/table_with_default_granularity', '2')
            PARTITION BY toYYYYMM(date)
            ORDER BY id
        ''')


        yield cluster

    finally:
        cluster.shutdown()

@pytest.mark.parametrize(
    ('n', 'tables'),
    [
        (node7, ['table_with_default_granularity', 'table_with_adaptive_default_granularity']),
        (node8, ['table_with_default_granularity']),
    ]
)
def test_version_single_node_update(start_dynamic_cluster, n, tables):
    for table in tables:
        n.query("INSERT INTO {tbl} VALUES (toDate('2018-10-01'), 1, 333), (toDate('2018-10-02'), 2, 444)".format(tbl=table))
    n.restart_with_latest_version()
    for table in tables:
        assert n.query("SELECT count() from {tbl}".format(tbl=table)) == '2\n'
        n.query("INSERT INTO {tbl} VALUES (toDate('2018-10-01'), 3, 333), (toDate('2018-10-02'), 4, 444)".format(tbl=table))
        assert n.query("SELECT count() from {tbl}".format(tbl=table)) == '4\n'

@pytest.mark.parametrize(
    ('node',),
    [
        (node9,),
        (node10,)
    ]
)
def test_mixed_granularity_single_node(start_dynamic_cluster, node):
    node.query("INSERT INTO table_with_default_granularity VALUES (toDate('2018-10-01'), 1, 333), (toDate('2018-10-02'), 2, 444)")
    node.query("INSERT INTO table_with_default_granularity VALUES (toDate('2018-09-01'), 1, 333), (toDate('2018-09-02'), 2, 444)")

    def callback(n):
        n.replace_config("/etc/clickhouse-server/merge_tree_settings.xml", "<yandex><merge_tree><enable_mixed_granularity_parts>1</enable_mixed_granularity_parts></merge_tree></yandex>")
        n.replace_config("/etc/clickhouse-server/config.d/merge_tree_settings.xml", "<yandex><merge_tree><enable_mixed_granularity_parts>1</enable_mixed_granularity_parts></merge_tree></yandex>")

    node.restart_with_latest_version(callback_onstop=callback)
    node.query("SYSTEM RELOAD CONFIG")
    assert_eq_with_retry(node, "SELECT value FROM system.merge_tree_settings WHERE name='enable_mixed_granularity_parts'", '1')
    assert node.query("SELECT count() from table_with_default_granularity") == '4\n'
    node.query("INSERT INTO table_with_default_granularity VALUES (toDate('2018-10-01'), 3, 333), (toDate('2018-10-02'), 4, 444)")
    assert node.query("SELECT count() from table_with_default_granularity") == '6\n'
    node.query("OPTIMIZE TABLE table_with_default_granularity PARTITION 201810 FINAL")
    assert node.query("SELECT count() from table_with_default_granularity") == '6\n'
    path_to_merged_part = node.query("SELECT path FROM system.parts WHERE table = 'table_with_default_granularity' AND active=1 ORDER BY partition DESC LIMIT 1").strip()
    node.exec_in_container(["bash", "-c", "find {p} -name '*.mrk2' | grep '.*'".format(p=path_to_merged_part)]) # check that we have adaptive files

    path_to_old_part = node.query("SELECT path FROM system.parts WHERE table = 'table_with_default_granularity' AND active=1 ORDER BY partition ASC LIMIT 1").strip()

    node.exec_in_container(["bash", "-c", "find {p} -name '*.mrk' | grep '.*'".format(p=path_to_old_part)]) # check that we have non adaptive files

    node.query("ALTER TABLE table_with_default_granularity UPDATE dummy = dummy + 1 WHERE 1")
    # still works
    assert node.query("SELECT count() from table_with_default_granularity") == '6\n'

    node.query("ALTER TABLE table_with_default_granularity MODIFY COLUMN dummy String")
    node.query("ALTER TABLE table_with_default_granularity ADD COLUMN dummy2 Float64")

    #still works
    assert node.query("SELECT count() from table_with_default_granularity") == '6\n'


def test_version_update_two_nodes(start_dynamic_cluster):
    node11.query("INSERT INTO table_with_default_granularity VALUES (toDate('2018-10-01'), 1, 333), (toDate('2018-10-02'), 2, 444)")
    node12.query("SYSTEM SYNC REPLICA table_with_default_granularity")
    assert node12.query("SELECT COUNT() FROM table_with_default_granularity") == '2\n'
    node12.restart_with_latest_version()
    node12.query("INSERT INTO table_with_default_granularity VALUES (toDate('2018-10-01'), 3, 333), (toDate('2018-10-02'), 4, 444)")
    node11.query("SYSTEM SYNC REPLICA table_with_default_granularity")
    assert node11.query("SELECT COUNT() FROM table_with_default_granularity") == '4\n'

    node12.query(
    '''
        CREATE TABLE table_with_default_granularity_new(date Date, id UInt32, dummy UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/shard11/table_with_default_granularity_new', '2')
        PARTITION BY toYYYYMM(date)
        ORDER BY id
    ''')

    node11.query(
    '''
        CREATE TABLE table_with_default_granularity_new(date Date, id UInt32, dummy UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/shard11/table_with_default_granularity_new', '1')
        PARTITION BY toYYYYMM(date)
        ORDER BY id
    ''')

    node12.query("INSERT INTO table_with_default_granularity_new VALUES (toDate('2018-10-01'), 1, 333), (toDate('2018-10-02'), 2, 444)")
    with pytest.raises(QueryTimeoutExceedException):
        node11.query("SYSTEM SYNC REPLICA table_with_default_granularity_new", timeout=5)
    node12.query("INSERT INTO table_with_default_granularity_new VALUES (toDate('2018-10-01'), 3, 333), (toDate('2018-10-02'), 4, 444)")

    node11.restart_with_latest_version() # just to be sure

    node11.query("SYSTEM SYNC REPLICA table_with_default_granularity_new", timeout=5)
    node12.query("SYSTEM SYNC REPLICA table_with_default_granularity_new", timeout=5)
    node11.query("SELECT COUNT() FROM table_with_default_granularity_new") == "4\n"
    node12.query("SELECT COUNT() FROM table_with_default_granularity_new") == "4\n"

    node11.query("SYSTEM SYNC REPLICA table_with_default_granularity")
    node11.query("INSERT INTO table_with_default_granularity VALUES (toDate('2018-10-01'), 5, 333), (toDate('2018-10-02'), 6, 444)")
    node12.query("SYSTEM SYNC REPLICA table_with_default_granularity")
    assert node12.query("SELECT COUNT() FROM table_with_default_granularity") == '6\n'
