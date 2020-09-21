from __future__ import print_function
from helpers.cluster import ClickHouseCluster
import helpers
import pytest
import time
from tempfile import NamedTemporaryFile
from os import path as p, unlink


def test_chroot_with_same_root():

    cluster_1 = ClickHouseCluster(__file__, zookeeper_config_path='configs/zookeeper_config_root_a.xml')
    cluster_2 = ClickHouseCluster(__file__, zookeeper_config_path='configs/zookeeper_config_root_a.xml')

    node1 = cluster_1.add_instance('node1', config_dir='configs', with_zookeeper=True, zookeeper_use_tmpfs=False)
    node2 = cluster_2.add_instance('node2', config_dir='configs', with_zookeeper=True, zookeeper_use_tmpfs=False)
    nodes = [node1, node2]

    def create_zk_root(zk):
        zk.ensure_path('/root_a')
        print(zk.get_children('/'))
    cluster_1.add_zookeeper_startup_command(create_zk_root)

    try:
        cluster_1.start()

        try:
            cluster_2.start(destroy_dirs=False)
            for i, node in enumerate(nodes):
                node.query('''
                CREATE TABLE simple (date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', '{replica}', date, id, 8192);
                '''.format(replica=node.name))
                for j in range(2): # Second insert to test deduplication
                    node.query("INSERT INTO simple VALUES ({0}, {0})".format(i))

            time.sleep(1)

            assert node1.query('select count() from simple').strip() == '2'
            assert node2.query('select count() from simple').strip() == '2'

        finally:
            cluster_2.shutdown()

    finally:
        cluster_1.shutdown()


def test_chroot_with_different_root():

    cluster_1 = ClickHouseCluster(__file__, zookeeper_config_path='configs/zookeeper_config_root_a.xml')
    cluster_2 = ClickHouseCluster(__file__, zookeeper_config_path='configs/zookeeper_config_root_b.xml')

    node1 = cluster_1.add_instance('node1', config_dir='configs', with_zookeeper=True, zookeeper_use_tmpfs=False)
    node2 = cluster_2.add_instance('node2', config_dir='configs', with_zookeeper=True, zookeeper_use_tmpfs=False)
    nodes = [node1, node2]

    def create_zk_roots(zk):
        zk.ensure_path('/root_a')
        zk.ensure_path('/root_b')
        print(zk.get_children('/'))
    cluster_1.add_zookeeper_startup_command(create_zk_roots)

    try:
        cluster_1.start()

        try:
            cluster_2.start(destroy_dirs=False)

            for i, node in enumerate(nodes):
                node.query('''
                CREATE TABLE simple (date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', '{replica}', date, id, 8192);
                '''.format(replica=node.name))
                for j in range(2): # Second insert to test deduplication
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

    node1 = cluster_1.add_instance('node1', config_dir='configs', with_zookeeper=True, zookeeper_use_tmpfs=False)
    node2 = cluster_2.add_instance('node2', config_dir='configs', with_zookeeper=True, zookeeper_use_tmpfs=False)

    try:
        cluster_1.start()

        node1.query('''
        CREATE TABLE simple (date Date, id UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', '{replica}', date, id, 8192);
        '''.format(replica=node1.name))

        with pytest.raises(Exception):
            cluster_2.start(destroy_dirs=False)
            node2.query('''
            CREATE TABLE simple (date Date, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', '1', date, id, 8192);
            ''')

    finally:
        cluster_1.shutdown()
        cluster_2.shutdown()


def test_secure_connection():
    # We need absolute path in zookeeper volumes. Generate it dynamically.
    TEMPLATE = '''
    zoo{zoo_id}:
        image: zookeeper:3.5.6
        restart: always
        environment:
            ZOO_TICK_TIME: 500
            ZOO_MY_ID: {zoo_id}
            ZOO_SERVERS: server.1=zoo1:2888:3888;2181 server.2=zoo2:2888:3888;2181 server.3=zoo3:2888:3888;2181
            ZOO_SECURE_CLIENT_PORT: 2281
        volumes:
           - {helpers_dir}/zookeeper-ssl-entrypoint.sh:/zookeeper-ssl-entrypoint.sh
           - {configs_dir}:/clickhouse-config
        command: ["zkServer.sh", "start-foreground"]
        entrypoint: /zookeeper-ssl-entrypoint.sh
    '''
    configs_dir = p.abspath(p.join(p.dirname(__file__), 'configs_secure'))
    helpers_dir = p.abspath(p.dirname(helpers.__file__))

    cluster = ClickHouseCluster(__file__, zookeeper_config_path='configs/zookeeper_config_with_ssl.xml')

    docker_compose = NamedTemporaryFile(delete=False)

    docker_compose.write(
        "version: '2.3'\nservices:\n" +
        TEMPLATE.format(zoo_id=1, configs_dir=configs_dir, helpers_dir=helpers_dir) +
        TEMPLATE.format(zoo_id=2, configs_dir=configs_dir, helpers_dir=helpers_dir) +
        TEMPLATE.format(zoo_id=3, configs_dir=configs_dir, helpers_dir=helpers_dir)
    )
    docker_compose.close()

    node1 = cluster.add_instance('node1', config_dir='configs_secure', with_zookeeper=True,
                                 zookeeper_docker_compose_path=docker_compose.name, zookeeper_use_tmpfs=False)
    node2 = cluster.add_instance('node2', config_dir='configs_secure', with_zookeeper=True,
                                 zookeeper_docker_compose_path=docker_compose.name, zookeeper_use_tmpfs=False)

    try:
        cluster.start()

        assert node1.query("SELECT count() FROM system.zookeeper WHERE path = '/'") == '2\n'
        assert node2.query("SELECT count() FROM system.zookeeper WHERE path = '/'") == '2\n'

    finally:
        cluster.shutdown()
        unlink(docker_compose.name)
