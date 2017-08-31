from helpers.cluster import ClickHouseCluster
import pytest


def test_chroot_with_same_root():

    cluster_1 = ClickHouseCluster(__file__, zookeeper_config_path='configs/zookeeper_config_root_a.xml')
    cluster_2 = ClickHouseCluster(__file__, zookeeper_config_path='configs/zookeeper_config_root_a.xml')

    node1 = cluster_1.add_instance('node1', config_dir='configs', with_zookeeper=True)
    node2 = cluster_2.add_instance('node2', config_dir='configs', with_zookeeper=True)
    nodes = [node1, node2]

    cluster_1.add_zookeeper_startup_command('create /root_a ""')
    cluster_1.add_zookeeper_startup_command('ls / ')

    try:
        cluster_1.start()

        try:
            cluster_2.start(destroy_dirs=False)
            for i, node in enumerate(nodes):
                node.query('''
                CREATE TABLE simple (date Date, id UInt32) 
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', '{replica}', date, id, 8192);
                '''.format(replica=node.name))
                node.query("INSERT INTO simple VALUES ({0}, {0})".format(i))

            assert node1.query('select count() from simple').strip() == '2'
            assert node2.query('select count() from simple').strip() == '2'

        finally:
            cluster_2.shutdown()

    finally:
        cluster_1.shutdown()


def test_chroot_with_different_root():

    cluster_1 = ClickHouseCluster(__file__, zookeeper_config_path='configs/zookeeper_config_root_a.xml')
    cluster_2 = ClickHouseCluster(__file__, zookeeper_config_path='configs/zookeeper_config_root_b.xml')

    node1 = cluster_1.add_instance('node1', config_dir='configs', with_zookeeper=True)
    node2 = cluster_2.add_instance('node2', config_dir='configs', with_zookeeper=True)
    nodes = [node1, node2]

    cluster_1.add_zookeeper_startup_command('create /root_a ""')
    cluster_1.add_zookeeper_startup_command('create /root_b ""')
    cluster_1.add_zookeeper_startup_command('ls / ')

    try:
        cluster_1.start()

        try:
            cluster_2.start(destroy_dirs=False)

            for i, node in enumerate(nodes):
                node.query('''
                CREATE TABLE simple (date Date, id UInt32) 
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', '{replica}', date, id, 8192);
                '''.format(replica=node.name))
                node.query("INSERT INTO simple VALUES ({0}, {0})".format(i))

            assert node1.query('select count() from simple').strip() == '1'
            assert node2.query('select count() from simple').strip() == '1'

        finally:
            cluster_2.shutdown()

    finally:
        cluster_1.shutdown()


def test_identity():

    cluster_1 = ClickHouseCluster(__file__, zookeeper_config_path='configs/zookeeper_config_with_password.xml')
    cluster_2 = ClickHouseCluster(__file__)

    node1 = cluster_1.add_instance('node1', config_dir='configs', with_zookeeper=True)
    node2 = cluster_2.add_instance('node2', config_dir='configs', with_zookeeper=True)

    try:
        cluster_1.start()

        # node1.query('''
        # CREATE TABLE simple (date Date, id UInt32)
        # ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', '{replica}', date, id, 8192);
        # '''.format(replica=node1.name))

        with pytest.raises(Exception):
            cluster_2.start(destroy_dirs=False)

    finally:
        cluster_1.shutdown()
        cluster_2.shutdown()
