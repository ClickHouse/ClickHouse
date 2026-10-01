import pytest
from helpers.cluster import ClickHouseCluster


def test_no_encryption_key_in_zk():
    cluster = ClickHouseCluster(__file__)
    node = cluster.add_instance("node", with_zookeeper=True, main_configs=["configs/config_no_encryption_key_in_zk.xml"], user_configs=["configs/users.xml"])

    try:
        cluster.start()
        assert False
    except Exception as e:
        assert "Container status: exited" in str(e)
        assert node.contains_in_log("DB::Exception: Got an encryption key with unexpected size 0, the size should be 16", from_host=True)
