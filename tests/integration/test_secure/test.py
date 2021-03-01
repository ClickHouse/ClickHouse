import itertools
import os.path
import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

NODES = {'node' + str(i): None for i in (1, 2)}

config = '''<yandex>
    <profiles>
        <default>
            <sleep_in_send_data>{sleep_in_send_data}</sleep_in_send_data>
        </default>
    </profiles>
</yandex>'''


@pytest.fixture(scope="module")
def started_cluster(request):
    cluster.__with_ssl_config = True
    main_configs = [
        "configs_secure/config.d/remote_servers.xml",
        "configs_secure/server.crt",
        "configs_secure/server.key",
        "configs_secure/dhparam.pem",
        "configs_secure/config.d/ssl_conf.xml",
    ]

    NODES['node1'] =  cluster.add_instance('node1', main_configs=main_configs, user_configs=["configs_secure/users.d/users.xml"])
    NODES['node2'] =  cluster.add_instance('node2', main_configs=main_configs, user_configs=["configs_secure/users.d/users.xml"])
    try:
        cluster.start()
        NODES['node2'].query("CREATE TABLE base_table (x UInt64) ENGINE = MergeTree  ORDER BY x;")
        NODES['node2'].query("INSERT INTO base_table VALUES (5);")
        NODES['node1'].query("CREATE TABLE distributed_table (x UInt64) ENGINE = Distributed(test_cluster, default, base_table);")

        yield cluster

    finally:
        cluster.shutdown()

@pytest.mark.timeout(100)
def test(started_cluster):
    NODES['node2'].replace_config('/etc/clickhouse-server/users.d/users.xml', config.format(sleep_in_send_data=1000))

    time.sleep(3)

    NODES['node1'].query_and_get_error('SELECT * FROM distributed_table settings receive_timeout=10;')

