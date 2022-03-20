import os

import pytest
from helpers.cluster import ClickHouseCluster
from pyhdfs import HdfsClient

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
    node1.query("drop table if exists SimpleHDFSStorage SYNC")
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

    fs = HdfsClient(hosts=started_cluster.hdfs_ip)
    dir = '/test_spaces'
    exists = fs.exists(dir)
    if exists:
        fs.delete(dir, recursive=True)
    fs.mkdirs(dir)

    hdfs_api.write_data(f"{dir}/test test test 1.txt", "1\n")
    hdfs_api.write_data(f"{dir}/test test test 2.txt", "2\n")
    hdfs_api.write_data(f"{dir}/test test test 3.txt", "3\n")

    node1.query(f"create table test (id UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{dir}/test*', 'TSV')")
    assert node1.query("select * from test order by id") == "1\n2\n3\n"
    fs.delete(dir, recursive=True)



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


def test_partition_by(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    file_name = "test_{_partition_id}"
    partition_by = "column3"
    values = "(1, 2, 3), (3, 2, 1), (1, 3, 2)"
    table_function = f"hdfs('hdfs://hdfs1:9000/{file_name}', 'TSV', '{table_format}')"

    node1.query(f"insert into table function {table_function} PARTITION BY {partition_by} values {values}")
    result = node1.query(f"select * from hdfs('hdfs://hdfs1:9000/test_1', 'TSV', '{table_format}')")
    assert(result.strip() == "3\t2\t1")
    result = node1.query(f"select * from hdfs('hdfs://hdfs1:9000/test_2', 'TSV', '{table_format}')")
    assert(result.strip() == "1\t3\t2")
    result = node1.query(f"select * from hdfs('hdfs://hdfs1:9000/test_3', 'TSV', '{table_format}')")
    assert(result.strip() == "1\t2\t3")

    file_name = "test2_{_partition_id}"
    node1.query(f"create table p(column1 UInt32, column2 UInt32, column3 UInt32) engine = HDFS('hdfs://hdfs1:9000/{file_name}', 'TSV') partition by column3")
    node1.query(f"insert into p values {values}")
    result = node1.query(f"select * from hdfs('hdfs://hdfs1:9000/test2_1', 'TSV', '{table_format}')")
    assert(result.strip() == "3\t2\t1")
    result = node1.query(f"select * from hdfs('hdfs://hdfs1:9000/test2_2', 'TSV', '{table_format}')")
    assert(result.strip() == "1\t3\t2")
    result = node1.query(f"select * from hdfs('hdfs://hdfs1:9000/test2_3', 'TSV', '{table_format}')")
    assert(result.strip() == "1\t2\t3")


def test_seekable_formats(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    table_function = f"hdfs('hdfs://hdfs1:9000/parquet', 'Parquet', 'a Int32, b String')"
    node1.query(f"insert into table function {table_function} SELECT number, randomString(100) FROM numbers(5000000)")

    result = node1.query(f"SELECT count() FROM {table_function}")
    assert(int(result) == 5000000)

    table_function = f"hdfs('hdfs://hdfs1:9000/orc', 'ORC', 'a Int32, b String')"
    node1.query(f"insert into table function {table_function} SELECT number, randomString(100) FROM numbers(5000000)")
    result = node1.query(f"SELECT count() FROM {table_function}")
    assert(int(result) == 5000000)


def test_read_table_with_default(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    data = "n\n100\n"
    hdfs_api.write_data("/simple_table_function", data)
    assert hdfs_api.read_data("/simple_table_function") == data

    output = "n\tm\n100\t200\n"
    assert node1.query(
        "select * from hdfs('hdfs://hdfs1:9000/simple_table_function', 'TSVWithNames', 'n UInt32, m UInt32 DEFAULT n * 2') FORMAT TSVWithNames") == output


def test_schema_inference(started_cluster):
    node1.query(f"insert into table function hdfs('hdfs://hdfs1:9000/native', 'Native', 'a Int32, b String') SELECT number, randomString(100) FROM numbers(5000000)")

    result = node1.query(f"desc hdfs('hdfs://hdfs1:9000/native', 'Native')")
    assert result == "a\tInt32\t\t\t\t\t\nb\tString\t\t\t\t\t\n"

    result = node1.query(f"select count(*) from hdfs('hdfs://hdfs1:9000/native', 'Native')")
    assert(int(result) == 5000000)

    node1.query(f"create table schema_inference engine=HDFS('hdfs://hdfs1:9000/native', 'Native')")
    result = node1.query(f"desc schema_inference")
    assert result == "a\tInt32\t\t\t\t\t\nb\tString\t\t\t\t\t\n"

    result = node1.query(f"select count(*) from schema_inference")
    assert(int(result) == 5000000)


def test_hdfsCluster(started_cluster):
    hdfs_api = started_cluster.hdfs_api
    fs = HdfsClient(hosts=started_cluster.hdfs_ip)
    dir = '/test_hdfsCluster'
    exists = fs.exists(dir)
    if exists:
        fs.delete(dir, recursive=True)
    fs.mkdirs(dir)
    hdfs_api.write_data("/test_hdfsCluster/file1", "1\n")
    hdfs_api.write_data("/test_hdfsCluster/file2", "2\n")
    hdfs_api.write_data("/test_hdfsCluster/file3", "3\n")

    actual = node1.query("select id, _file as file_name, _path as file_path from hdfs('hdfs://hdfs1:9000/test_hdfsCluster/file*', 'TSV', 'id UInt32') order by id")
    expected = "1\tfile1\thdfs://hdfs1:9000/test_hdfsCluster/file1\n2\tfile2\thdfs://hdfs1:9000/test_hdfsCluster/file2\n3\tfile3\thdfs://hdfs1:9000/test_hdfsCluster/file3\n"
    assert actual == expected

    actual = node1.query("select id, _file as file_name, _path as file_path from hdfsCluster('test_cluster_two_shards', 'hdfs://hdfs1:9000/test_hdfsCluster/file*', 'TSV', 'id UInt32') order by id")
    expected = "1\tfile1\thdfs://hdfs1:9000/test_hdfsCluster/file1\n2\tfile2\thdfs://hdfs1:9000/test_hdfsCluster/file2\n3\tfile3\thdfs://hdfs1:9000/test_hdfsCluster/file3\n"
    assert actual == expected
    fs.delete(dir, recursive=True)

def test_hdfs_directory_not_exist(started_cluster):
    ddl ="create table HDFSStorageWithNotExistDir (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/data/not_eixst', 'TSV')";
    node1.query(ddl)
    assert "" == node1.query("select * from HDFSStorageWithNotExistDir")

def test_overwrite(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    table_function = f"hdfs('hdfs://hdfs1:9000/data', 'Parquet', 'a Int32, b String')"
    node1.query(f"create table test_overwrite as {table_function}")
    node1.query(f"insert into test_overwrite select number, randomString(100) from numbers(5)")
    node1.query_and_get_error(f"insert into test_overwrite select number, randomString(100) FROM numbers(10)")
    node1.query(f"insert into test_overwrite select number, randomString(100) from numbers(10) settings hdfs_truncate_on_insert=1")

    result = node1.query(f"select count() from test_overwrite")
    assert(int(result) == 10)


def test_multiple_inserts(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    table_function = f"hdfs('hdfs://hdfs1:9000/data_multiple_inserts', 'Parquet', 'a Int32, b String')"
    node1.query(f"create table test_multiple_inserts as {table_function}")
    node1.query(f"insert into test_multiple_inserts select number, randomString(100) from numbers(10)")
    node1.query(f"insert into test_multiple_inserts select number, randomString(100) from numbers(20) settings hdfs_create_new_file_on_insert=1")
    node1.query(f"insert into test_multiple_inserts select number, randomString(100) from numbers(30) settings hdfs_create_new_file_on_insert=1")

    result = node1.query(f"select count() from test_multiple_inserts")
    assert(int(result) == 60)

    result = node1.query(f"drop table test_multiple_inserts")

    table_function = f"hdfs('hdfs://hdfs1:9000/data_multiple_inserts.gz', 'Parquet', 'a Int32, b String')"
    node1.query(f"create table test_multiple_inserts as {table_function}")
    node1.query(f"insert into test_multiple_inserts select number, randomString(100) FROM numbers(10)")
    node1.query(f"insert into test_multiple_inserts select number, randomString(100) FROM numbers(20) settings hdfs_create_new_file_on_insert=1")
    node1.query(f"insert into test_multiple_inserts select number, randomString(100) FROM numbers(30) settings hdfs_create_new_file_on_insert=1")

    result = node1.query(f"select count() from test_multiple_inserts")
    assert(int(result) == 60)

    
def test_format_detection(started_cluster):
    node1.query(f"create table arrow_table (x UInt64) engine=HDFS('hdfs://hdfs1:9000/data.arrow')")
    node1.query(f"insert into arrow_table select 1")
    result = node1.query(f"select * from hdfs('hdfs://hdfs1:9000/data.arrow')")
    assert(int(result) == 1)


def test_schema_inference_with_globs(started_cluster):
    node1.query(f"insert into table function hdfs('hdfs://hdfs1:9000/data1.jsoncompacteachrow', 'JSONCompactEachRow', 'x Nullable(UInt32)') select NULL")
    node1.query(f"insert into table function hdfs('hdfs://hdfs1:9000/data2.jsoncompacteachrow', 'JSONCompactEachRow', 'x Nullable(UInt32)') select 0")
    
    result = node1.query(f"desc hdfs('hdfs://hdfs1:9000/data*.jsoncompacteachrow')")
    assert(result.strip() == 'c1\tNullable(Float64)')

    result = node1.query(f"select * from hdfs('hdfs://hdfs1:9000/data*.jsoncompacteachrow')")
    assert(sorted(result.split()) == ['0', '\\N'])


def test_insert_select_schema_inference(started_cluster):
    node1.query(f"insert into table function hdfs('hdfs://hdfs1:9000/test.native.zst') select toUInt64(1) as x")

    result = node1.query(f"desc hdfs('hdfs://hdfs1:9000/test.native.zst')")
    assert(result.strip() == 'x\tUInt64')

    result = node1.query(f"select * from hdfs('hdfs://hdfs1:9000/test.native.zst')")
    assert(int(result) == 1)


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
