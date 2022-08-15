import os

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_hdfs=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_read_write_storage(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    node1.query(
        "create table SimpleHDFSStorage (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/simple_storage', 'TSV')")
    node1.query("insert into SimpleHDFSStorage values (1, 'Mark', 72.53)")
    assert hdfs_api.read_data("/simple_storage") == "1\tMark\t72.53\n"
    assert node1.query("select * from SimpleHDFSStorage") == "1\tMark\t72.53\n"


def test_read_write_storage_with_globs(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    node1.query(
        "create table HDFSStorageWithRange (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/storage{1..5}', 'TSV')")
    node1.query(
        "create table HDFSStorageWithEnum (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/storage{1,2,3,4,5}', 'TSV')")
    node1.query(
        "create table HDFSStorageWithQuestionMark (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/storage?', 'TSV')")
    node1.query(
        "create table HDFSStorageWithAsterisk (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/storage*', 'TSV')")

    for i in ["1", "2", "3"]:
        hdfs_api.write_data("/storage" + i, i + "\tMark\t72.53\n")
        assert hdfs_api.read_data("/storage" + i) == i + "\tMark\t72.53\n"

    assert node1.query("select count(*) from HDFSStorageWithRange") == "3\n"
    assert node1.query("select count(*) from HDFSStorageWithEnum") == "3\n"
    assert node1.query("select count(*) from HDFSStorageWithQuestionMark") == "3\n"
    assert node1.query("select count(*) from HDFSStorageWithAsterisk") == "3\n"

    try:
        node1.query("insert into HDFSStorageWithEnum values (1, 'NEW', 4.2)")
        assert False, "Exception have to be thrown"
    except Exception as ex:
        print(ex)
        assert "in readonly mode" in str(ex)

    try:
        node1.query("insert into HDFSStorageWithQuestionMark values (1, 'NEW', 4.2)")
        assert False, "Exception have to be thrown"
    except Exception as ex:
        print(ex)
        assert "in readonly mode" in str(ex)

    try:
        node1.query("insert into HDFSStorageWithAsterisk values (1, 'NEW', 4.2)")
        assert False, "Exception have to be thrown"
    except Exception as ex:
        print(ex)
        assert "in readonly mode" in str(ex)


def test_read_write_table(started_cluster):
    hdfs_api = started_cluster.hdfs_api


    data = "1\tSerialize\t555.222\n2\tData\t777.333\n"
    hdfs_api.write_data("/simple_table_function", data)

    assert hdfs_api.read_data("/simple_table_function") == data

    assert node1.query(
        "select * from hdfs('hdfs://hdfs1:9000/simple_table_function', 'TSV', 'id UInt64, text String, number Float64')") == data


def test_write_table(started_cluster):
    hdfs_api = started_cluster.hdfs_api


    node1.query(
        "create table OtherHDFSStorage (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')")
    node1.query("insert into OtherHDFSStorage values (10, 'tomas', 55.55), (11, 'jack', 32.54)")

    result = "10\ttomas\t55.55\n11\tjack\t32.54\n"
    assert hdfs_api.read_data("/other_storage") == result
    assert node1.query("select * from OtherHDFSStorage order by id") == result


def test_bad_hdfs_uri(started_cluster):
    try:
        node1.query(
            "create table BadStorage1 (id UInt32, name String, weight Float64) ENGINE = HDFS('hads:hgsdfs100500:9000/other_storage', 'TSV')")
    except Exception as ex:
        print(ex)
        assert "Bad hdfs url" in str(ex)
    try:
        node1.query(
            "create table BadStorage2 (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs100500:9000/other_storage', 'TSV')")
    except Exception as ex:
        print(ex)
        assert "Unable to create builder to connect to HDFS" in str(ex)

    try:
        node1.query(
            "create table BadStorage3 (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/<>', 'TSV')")
    except Exception as ex:
        print(ex)
        assert "Unable to open HDFS file" in str(ex)

@pytest.mark.timeout(800)
def test_globs_in_read_table(started_cluster):
    hdfs_api = started_cluster.hdfs_api


    some_data = "1\tSerialize\t555.222\n2\tData\t777.333\n"
    globs_dir = "/dir_for_test_with_globs/"
    files = ["dir1/dir_dir/file1", "dir2/file2", "simple_table_function", "dir/file", "some_dir/dir1/file",
             "some_dir/dir2/file", "some_dir/file", "table1_function", "table2_function", "table3_function"]
    for filename in files:
        hdfs_api.write_data(globs_dir + filename, some_data)

    test_requests = [("dir{1..5}/dir_dir/file1", 1, 1),
                     ("*_table_functio?", 1, 1),
                     ("dir/fil?", 1, 1),
                     ("table{3..8}_function", 1, 1),
                     ("table{2..8}_function", 2, 2),
                     ("dir/*", 1, 1),
                     ("dir/*?*?*?*?*", 1, 1),
                     ("dir/*?*?*?*?*?*", 0, 0),
                     ("some_dir/*/file", 2, 1),
                     ("some_dir/dir?/*", 2, 1),
                     ("*/*/*", 3, 2),
                     ("?", 0, 0)]

    for pattern, paths_amount, files_amount in test_requests:
        inside_table_func = "'hdfs://hdfs1:9000" + globs_dir + pattern + "', 'TSV', 'id UInt64, text String, number Float64'"
        print("inside_table_func ", inside_table_func)
        assert node1.query("select * from hdfs(" + inside_table_func + ")") == paths_amount * some_data
        assert node1.query("select count(distinct _path) from hdfs(" + inside_table_func + ")").rstrip() == str(
            paths_amount)
        assert node1.query("select count(distinct _file) from hdfs(" + inside_table_func + ")").rstrip() == str(
            files_amount)


def test_read_write_gzip_table(started_cluster):
    hdfs_api = started_cluster.hdfs_api


    data = "1\tHello Jessica\t555.222\n2\tI rolled a joint\t777.333\n"
    hdfs_api.write_gzip_data("/simple_table_function.gz", data)

    assert hdfs_api.read_gzip_data("/simple_table_function.gz") == data

    assert node1.query(
        "select * from hdfs('hdfs://hdfs1:9000/simple_table_function.gz', 'TSV', 'id UInt64, text String, number Float64')") == data


def test_read_write_gzip_table_with_parameter_gzip(started_cluster):
    hdfs_api = started_cluster.hdfs_api


    data = "1\tHello Jessica\t555.222\n2\tI rolled a joint\t777.333\n"
    hdfs_api.write_gzip_data("/simple_table_function", data)

    assert hdfs_api.read_gzip_data("/simple_table_function") == data

    assert node1.query(
        "select * from hdfs('hdfs://hdfs1:9000/simple_table_function', 'TSV', 'id UInt64, text String, number Float64', 'gzip')") == data


def test_read_write_table_with_parameter_none(started_cluster):
    hdfs_api = started_cluster.hdfs_api


    data = "1\tHello Jessica\t555.222\n2\tI rolled a joint\t777.333\n"
    hdfs_api.write_data("/simple_table_function.gz", data)

    assert hdfs_api.read_data("/simple_table_function.gz") == data

    assert node1.query(
        "select * from hdfs('hdfs://hdfs1:9000/simple_table_function.gz', 'TSV', 'id UInt64, text String, number Float64', 'none')") == data


def test_read_write_gzip_table_with_parameter_auto_gz(started_cluster):
    hdfs_api = started_cluster.hdfs_api


    data = "1\tHello Jessica\t555.222\n2\tI rolled a joint\t777.333\n"
    hdfs_api.write_gzip_data("/simple_table_function.gz", data)

    assert hdfs_api.read_gzip_data("/simple_table_function.gz") == data

    assert node1.query(
        "select * from hdfs('hdfs://hdfs1:9000/simple_table_function.gz', 'TSV', 'id UInt64, text String, number Float64', 'auto')") == data


def test_write_gz_storage(started_cluster):
    hdfs_api = started_cluster.hdfs_api


    node1.query(
        "create table GZHDFSStorage (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/storage.gz', 'TSV')")
    node1.query("insert into GZHDFSStorage values (1, 'Mark', 72.53)")
    assert hdfs_api.read_gzip_data("/storage.gz") == "1\tMark\t72.53\n"
    assert node1.query("select * from GZHDFSStorage") == "1\tMark\t72.53\n"


def test_write_gzip_storage(started_cluster):
    hdfs_api = started_cluster.hdfs_api


    node1.query(
        "create table GZIPHDFSStorage (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/gzip_storage', 'TSV', 'gzip')")
    node1.query("insert into GZIPHDFSStorage values (1, 'Mark', 72.53)")
    assert hdfs_api.read_gzip_data("/gzip_storage") == "1\tMark\t72.53\n"
    assert node1.query("select * from GZIPHDFSStorage") == "1\tMark\t72.53\n"


def test_virtual_columns(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    node1.query("create table virtual_cols (id UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/file*', 'TSV')")
    hdfs_api.write_data("/file1", "1\n")
    hdfs_api.write_data("/file2", "2\n")
    hdfs_api.write_data("/file3", "3\n")
    expected = "1\tfile1\thdfs://hdfs1:9000//file1\n2\tfile2\thdfs://hdfs1:9000//file2\n3\tfile3\thdfs://hdfs1:9000//file3\n"
    assert node1.query("select id, _file as file_name, _path as file_path from virtual_cols order by id") == expected


def test_read_files_with_spaces(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    hdfs_api.write_data("/test test test 1.txt", "1\n")
    hdfs_api.write_data("/test test test 2.txt", "2\n")
    hdfs_api.write_data("/test test test 3.txt", "3\n")
    node1.query("create table test (id UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/test*', 'TSV')")
    assert node1.query("select * from test order by id") == "1\n2\n3\n"


def test_truncate_table(started_cluster):
    hdfs_api = started_cluster.hdfs_api
    node1.query(
        "create table test_truncate (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/tr', 'TSV')")
    node1.query("insert into test_truncate values (1, 'Mark', 72.53)")
    assert hdfs_api.read_data("/tr") == "1\tMark\t72.53\n"
    assert node1.query("select * from test_truncate") == "1\tMark\t72.53\n"
    node1.query("truncate table test_truncate")
    assert node1.query("select * from test_truncate") == ""
    node1.query("drop table test_truncate")


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
