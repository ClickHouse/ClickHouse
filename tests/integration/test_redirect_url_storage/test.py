import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_zookeeper=False, with_hdfs=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_url_without_redirect(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    hdfs_api.write_data("/simple_storage", "1\tMark\t72.53\n")
    assert hdfs_api.read_data("/simple_storage") == "1\tMark\t72.53\n"

    # access datanode port directly
    node1.query(
        "create table WebHDFSStorage (id UInt32, name String, weight Float64) ENGINE = URL('http://hdfs1:50075/webhdfs/v1/simple_storage?op=OPEN&namenoderpcaddress=hdfs1:9000&offset=0', 'TSV')")
    assert node1.query("select * from WebHDFSStorage") == "1\tMark\t72.53\n"


def test_url_with_globs(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    hdfs_api.write_data("/simple_storage_1_1", "1\n")
    hdfs_api.write_data("/simple_storage_1_2", "2\n")
    hdfs_api.write_data("/simple_storage_1_3", "3\n")
    hdfs_api.write_data("/simple_storage_2_1", "4\n")
    hdfs_api.write_data("/simple_storage_2_2", "5\n")
    hdfs_api.write_data("/simple_storage_2_3", "6\n")

    result = node1.query(
        "select * from url('http://hdfs1:50075/webhdfs/v1/simple_storage_{1..2}_{1..3}?op=OPEN&namenoderpcaddress=hdfs1:9000&offset=0', 'TSV', 'data String') as data order by data")
    assert result == "1\n2\n3\n4\n5\n6\n"


def test_url_with_globs_and_failover(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    hdfs_api.write_data("/simple_storage_1_1", "1\n")
    hdfs_api.write_data("/simple_storage_1_2", "2\n")
    hdfs_api.write_data("/simple_storage_1_3", "3\n")
    hdfs_api.write_data("/simple_storage_3_1", "4\n")
    hdfs_api.write_data("/simple_storage_3_2", "5\n")
    hdfs_api.write_data("/simple_storage_3_3", "6\n")

    result = node1.query(
        "select * from url('http://hdfs1:50075/webhdfs/v1/simple_storage_{0|1|2|3}_{1..3}?op=OPEN&namenoderpcaddress=hdfs1:9000&offset=0', 'TSV', 'data String') as data order by data")
    assert result == "1\n2\n3\n"


def test_url_with_redirect_not_allowed(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    hdfs_api.write_data("/simple_storage", "1\tMark\t72.53\n")
    assert hdfs_api.read_data("/simple_storage") == "1\tMark\t72.53\n"

    # access proxy port without allowing redirects
    node1.query(
        "create table WebHDFSStorageWithoutRedirect (id UInt32, name String, weight Float64) ENGINE = URL('http://hdfs1:50070/webhdfs/v1/simple_storage?op=OPEN&namenoderpcaddress=hdfs1:9000&offset=0', 'TSV')")
    with pytest.raises(Exception):
        assert node1.query("select * from WebHDFSStorageWithoutRedirect") == "1\tMark\t72.53\n"


def test_url_with_redirect_allowed(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    hdfs_api.write_data("/simple_storage", "1\tMark\t72.53\n")
    assert hdfs_api.read_data("/simple_storage") == "1\tMark\t72.53\n"

    # access proxy port with allowing redirects
    # http://localhost:50070/webhdfs/v1/b?op=OPEN&namenoderpcaddress=hdfs1:9000&offset=0
    node1.query(
        "create table WebHDFSStorageWithRedirect (id UInt32, name String, weight Float64) ENGINE = URL('http://hdfs1:50070/webhdfs/v1/simple_storage?op=OPEN&namenoderpcaddress=hdfs1:9000&offset=0', 'TSV')")
    assert node1.query("SET max_http_get_redirects=1; select * from WebHDFSStorageWithRedirect") == "1\tMark\t72.53\n"
