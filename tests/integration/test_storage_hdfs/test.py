import os

import pytest
import time
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from pyhdfs import HdfsClient

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/macro.xml",
        "configs/schema_cache.xml",
        "configs/cluster.xml",
    ],
    with_hdfs=True,
)


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
        "create table SimpleHDFSStorage (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/simple_storage', 'TSV')"
    )
    node1.query("insert into SimpleHDFSStorage values (1, 'Mark', 72.53)")
    assert hdfs_api.read_data("/simple_storage") == "1\tMark\t72.53\n"
    assert node1.query("select * from SimpleHDFSStorage") == "1\tMark\t72.53\n"


def test_read_write_storage_with_globs(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    node1.query(
        "create table HDFSStorageWithRange (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/storage{1..5}', 'TSV')"
    )
    node1.query(
        "create table HDFSStorageWithEnum (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/storage{1,2,3,4,5}', 'TSV')"
    )
    node1.query(
        "create table HDFSStorageWithQuestionMark (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/storage?', 'TSV')"
    )
    node1.query(
        "create table HDFSStorageWithAsterisk (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/storage*', 'TSV')"
    )

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


def test_storage_with_multidirectory_glob(started_cluster):
    hdfs_api = started_cluster.hdfs_api
    for i in ["1", "2"]:
        hdfs_api.write_data(
            f"/multiglob/p{i}/path{i}/postfix/data{i}", f"File{i}\t{i}{i}\n"
        )
        assert (
            hdfs_api.read_data(f"/multiglob/p{i}/path{i}/postfix/data{i}")
            == f"File{i}\t{i}{i}\n"
        )

    r = node1.query(
        "SELECT * FROM hdfs('hdfs://hdfs1:9000/multiglob/{p1/path1,p2/path2}/postfix/data{1,2}', TSV)"
    )
    assert (r == f"File1\t11\nFile2\t22\n") or (r == f"File2\t22\nFile1\t11\n")

    try:
        node1.query(
            "SELECT * FROM hdfs('hdfs://hdfs1:9000/multiglob/{p4/path1,p2/path3}/postfix/data{1,2}.nonexist', TSV)"
        )
        assert False, "Exception have to be thrown"
    except Exception as ex:
        print(ex)
        assert "no files" in str(ex)


def test_read_write_table(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    data = "1\tSerialize\t555.222\n2\tData\t777.333\n"
    hdfs_api.write_data("/simple_table_function", data)

    assert hdfs_api.read_data("/simple_table_function") == data

    assert (
        node1.query(
            "select * from hdfs('hdfs://hdfs1:9000/simple_table_function', 'TSV', 'id UInt64, text String, number Float64')"
        )
        == data
    )


def test_write_table(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    node1.query(
        "create table OtherHDFSStorage (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')"
    )
    node1.query(
        "insert into OtherHDFSStorage values (10, 'tomas', 55.55), (11, 'jack', 32.54)"
    )

    result = "10\ttomas\t55.55\n11\tjack\t32.54\n"
    assert hdfs_api.read_data("/other_storage") == result
    assert node1.query("select * from OtherHDFSStorage order by id") == result


def test_bad_hdfs_uri(started_cluster):
    try:
        node1.query(
            "create table BadStorage1 (id UInt32, name String, weight Float64) ENGINE = HDFS('hads:hgsdfs100500:9000/other_storage', 'TSV')"
        )
    except Exception as ex:
        print(ex)
        assert "Bad hdfs url" in str(ex)
    try:
        node1.query(
            "create table BadStorage2 (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs100500:9000/other_storage', 'TSV')"
        )
    except Exception as ex:
        print(ex)
        assert "Unable to create builder to connect to HDFS" in str(ex)

    try:
        node1.query(
            "create table BadStorage3 (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/<>', 'TSV')"
        )
    except Exception as ex:
        print(ex)
        assert "Unable to open HDFS file" in str(ex)


@pytest.mark.timeout(800)
def test_globs_in_read_table(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    some_data = "1\tSerialize\t555.222\n2\tData\t777.333\n"
    globs_dir = "/dir_for_test_with_globs/"
    files = [
        "dir1/dir_dir/file1",
        "dir2/file2",
        "simple_table_function",
        "dir/file",
        "some_dir/dir1/file",
        "some_dir/dir2/file",
        "some_dir/file",
        "table1_function",
        "table2_function",
        "table3_function",
    ]
    for filename in files:
        hdfs_api.write_data(globs_dir + filename, some_data)

    test_requests = [
        ("dir{1..5}/dir_dir/file1", 1, 1),
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
        ("?", 0, 0),
    ]

    for pattern, paths_amount, files_amount in test_requests:
        inside_table_func = (
            "'hdfs://hdfs1:9000"
            + globs_dir
            + pattern
            + "', 'TSV', 'id UInt64, text String, number Float64'"
        )
        print("inside_table_func ", inside_table_func)
        assert (
            node1.query("select * from hdfs(" + inside_table_func + ")")
            == paths_amount * some_data
        )
        assert node1.query(
            "select count(distinct _path) from hdfs(" + inside_table_func + ")"
        ).rstrip() == str(paths_amount)
        assert node1.query(
            "select count(distinct _file) from hdfs(" + inside_table_func + ")"
        ).rstrip() == str(files_amount)


def test_read_write_gzip_table(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    data = "1\tHello Jessica\t555.222\n2\tI rolled a joint\t777.333\n"
    hdfs_api.write_gzip_data("/simple_table_function.gz", data)

    assert hdfs_api.read_gzip_data("/simple_table_function.gz") == data

    assert (
        node1.query(
            "select * from hdfs('hdfs://hdfs1:9000/simple_table_function.gz', 'TSV', 'id UInt64, text String, number Float64')"
        )
        == data
    )


def test_read_write_gzip_table_with_parameter_gzip(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    data = "1\tHello Jessica\t555.222\n2\tI rolled a joint\t777.333\n"
    hdfs_api.write_gzip_data("/simple_table_function", data)

    assert hdfs_api.read_gzip_data("/simple_table_function") == data

    assert (
        node1.query(
            "select * from hdfs('hdfs://hdfs1:9000/simple_table_function', 'TSV', 'id UInt64, text String, number Float64', 'gzip')"
        )
        == data
    )


def test_read_write_table_with_parameter_none(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    data = "1\tHello Jessica\t555.222\n2\tI rolled a joint\t777.333\n"
    hdfs_api.write_data("/simple_table_function.gz", data)

    assert hdfs_api.read_data("/simple_table_function.gz") == data

    assert (
        node1.query(
            "select * from hdfs('hdfs://hdfs1:9000/simple_table_function.gz', 'TSV', 'id UInt64, text String, number Float64', 'none')"
        )
        == data
    )


def test_read_write_gzip_table_with_parameter_auto_gz(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    data = "1\tHello Jessica\t555.222\n2\tI rolled a joint\t777.333\n"
    hdfs_api.write_gzip_data("/simple_table_function.gz", data)

    assert hdfs_api.read_gzip_data("/simple_table_function.gz") == data

    assert (
        node1.query(
            "select * from hdfs('hdfs://hdfs1:9000/simple_table_function.gz', 'TSV', 'id UInt64, text String, number Float64', 'auto')"
        )
        == data
    )


def test_write_gz_storage(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    node1.query(
        "create table GZHDFSStorage (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/storage.gz', 'TSV')"
    )
    node1.query("insert into GZHDFSStorage values (1, 'Mark', 72.53)")
    assert hdfs_api.read_gzip_data("/storage.gz") == "1\tMark\t72.53\n"
    assert node1.query("select * from GZHDFSStorage") == "1\tMark\t72.53\n"


def test_write_gzip_storage(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    node1.query(
        "create table GZIPHDFSStorage (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/gzip_storage', 'TSV', 'gzip')"
    )
    node1.query("insert into GZIPHDFSStorage values (1, 'Mark', 72.53)")
    assert hdfs_api.read_gzip_data("/gzip_storage") == "1\tMark\t72.53\n"
    assert node1.query("select * from GZIPHDFSStorage") == "1\tMark\t72.53\n"


def test_virtual_columns(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    node1.query(
        "create table virtual_cols (id UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/file*', 'TSV')"
    )
    hdfs_api.write_data("/file1", "1\n")
    hdfs_api.write_data("/file2", "2\n")
    hdfs_api.write_data("/file3", "3\n")
    expected = "1\tfile1\thdfs://hdfs1:9000/file1\n2\tfile2\thdfs://hdfs1:9000/file2\n3\tfile3\thdfs://hdfs1:9000/file3\n"
    assert (
        node1.query(
            "select id, _file as file_name, _path as file_path from virtual_cols order by id"
        )
        == expected
    )


def test_read_files_with_spaces(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    fs = HdfsClient(hosts=started_cluster.hdfs_ip)
    dir = "/test_spaces"
    exists = fs.exists(dir)
    if exists:
        fs.delete(dir, recursive=True)
    fs.mkdirs(dir)

    hdfs_api.write_data(f"{dir}/test test test 1.txt", "1\n")
    hdfs_api.write_data(f"{dir}/test test test 2.txt", "2\n")
    hdfs_api.write_data(f"{dir}/test test test 3.txt", "3\n")

    node1.query(
        f"create table test (id UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{dir}/test*', 'TSV')"
    )
    assert node1.query("select * from test order by id") == "1\n2\n3\n"
    fs.delete(dir, recursive=True)


def test_truncate_table(started_cluster):
    hdfs_api = started_cluster.hdfs_api
    node1.query(
        "create table test_truncate (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/tr', 'TSV')"
    )
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

    node1.query(
        f"insert into table function {table_function} PARTITION BY {partition_by} values {values}"
    )
    result = node1.query(
        f"select * from hdfs('hdfs://hdfs1:9000/test_1', 'TSV', '{table_format}')"
    )
    assert result.strip() == "3\t2\t1"
    result = node1.query(
        f"select * from hdfs('hdfs://hdfs1:9000/test_2', 'TSV', '{table_format}')"
    )
    assert result.strip() == "1\t3\t2"
    result = node1.query(
        f"select * from hdfs('hdfs://hdfs1:9000/test_3', 'TSV', '{table_format}')"
    )
    assert result.strip() == "1\t2\t3"

    file_name = "test2_{_partition_id}"
    node1.query(
        f"create table p(column1 UInt32, column2 UInt32, column3 UInt32) engine = HDFS('hdfs://hdfs1:9000/{file_name}', 'TSV') partition by column3"
    )
    node1.query(f"insert into p values {values}")
    result = node1.query(
        f"select * from hdfs('hdfs://hdfs1:9000/test2_1', 'TSV', '{table_format}')"
    )
    assert result.strip() == "3\t2\t1"
    result = node1.query(
        f"select * from hdfs('hdfs://hdfs1:9000/test2_2', 'TSV', '{table_format}')"
    )
    assert result.strip() == "1\t3\t2"
    result = node1.query(
        f"select * from hdfs('hdfs://hdfs1:9000/test2_3', 'TSV', '{table_format}')"
    )
    assert result.strip() == "1\t2\t3"


def test_seekable_formats(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    table_function = (
        f"hdfs('hdfs://hdfs1:9000/parquet', 'Parquet', 'a Int32, b String')"
    )
    node1.query(
        f"insert into table function {table_function} SELECT number, randomString(100) FROM numbers(5000000)"
    )

    result = node1.query(f"SELECT count() FROM {table_function}")
    assert int(result) == 5000000

    table_function = f"hdfs('hdfs://hdfs1:9000/orc', 'ORC', 'a Int32, b String')"
    node1.query(
        f"insert into table function {table_function} SELECT number, randomString(100) FROM numbers(5000000)"
    )
    result = node1.query(f"SELECT count() FROM {table_function}")
    assert int(result) == 5000000


def test_read_table_with_default(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    data = "n\n100\n"
    hdfs_api.write_data("/simple_table_function", data)
    assert hdfs_api.read_data("/simple_table_function") == data

    output = "n\tm\n100\t200\n"
    assert (
        node1.query(
            "select * from hdfs('hdfs://hdfs1:9000/simple_table_function', 'TSVWithNames', 'n UInt32, m UInt32 DEFAULT n * 2') FORMAT TSVWithNames"
        )
        == output
    )


def test_schema_inference(started_cluster):
    node1.query(
        f"insert into table function hdfs('hdfs://hdfs1:9000/native', 'Native', 'a Int32, b String') SELECT number, randomString(100) FROM numbers(5000000)"
    )

    result = node1.query(f"desc hdfs('hdfs://hdfs1:9000/native', 'Native')")
    assert result == "a\tInt32\t\t\t\t\t\nb\tString\t\t\t\t\t\n"

    result = node1.query(
        f"select count(*) from hdfs('hdfs://hdfs1:9000/native', 'Native')"
    )
    assert int(result) == 5000000

    node1.query(
        f"create table schema_inference engine=HDFS('hdfs://hdfs1:9000/native', 'Native')"
    )
    result = node1.query(f"desc schema_inference")
    assert result == "a\tInt32\t\t\t\t\t\nb\tString\t\t\t\t\t\n"

    result = node1.query(f"select count(*) from schema_inference")
    assert int(result) == 5000000


def test_hdfsCluster(started_cluster):
    hdfs_api = started_cluster.hdfs_api
    fs = HdfsClient(hosts=started_cluster.hdfs_ip)
    dir = "/test_hdfsCluster"
    exists = fs.exists(dir)
    if exists:
        fs.delete(dir, recursive=True)
    fs.mkdirs(dir)
    hdfs_api.write_data("/test_hdfsCluster/file1", "1\n")
    hdfs_api.write_data("/test_hdfsCluster/file2", "2\n")
    hdfs_api.write_data("/test_hdfsCluster/file3", "3\n")

    actual = node1.query(
        "select id, _file as file_name, _path as file_path from hdfs('hdfs://hdfs1:9000/test_hdfsCluster/file*', 'TSV', 'id UInt32') order by id"
    )
    expected = "1\tfile1\thdfs://hdfs1:9000/test_hdfsCluster/file1\n2\tfile2\thdfs://hdfs1:9000/test_hdfsCluster/file2\n3\tfile3\thdfs://hdfs1:9000/test_hdfsCluster/file3\n"
    assert actual == expected

    actual = node1.query(
        "select id, _file as file_name, _path as file_path from hdfsCluster('test_cluster_two_shards', 'hdfs://hdfs1:9000/test_hdfsCluster/file*', 'TSV', 'id UInt32') order by id"
    )
    expected = "1\tfile1\thdfs://hdfs1:9000/test_hdfsCluster/file1\n2\tfile2\thdfs://hdfs1:9000/test_hdfsCluster/file2\n3\tfile3\thdfs://hdfs1:9000/test_hdfsCluster/file3\n"
    assert actual == expected
    fs.delete(dir, recursive=True)


def test_hdfs_directory_not_exist(started_cluster):
    ddl = "create table HDFSStorageWithNotExistDir (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/data/not_eixst', 'TSV')"
    node1.query(ddl)
    assert "" == node1.query("select * from HDFSStorageWithNotExistDir")


def test_overwrite(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    table_function = f"hdfs('hdfs://hdfs1:9000/data', 'Parquet', 'a Int32, b String')"
    node1.query(f"create table test_overwrite as {table_function}")
    node1.query(
        f"insert into test_overwrite select number, randomString(100) from numbers(5)"
    )
    node1.query_and_get_error(
        f"insert into test_overwrite select number, randomString(100) FROM numbers(10)"
    )
    node1.query(
        f"insert into test_overwrite select number, randomString(100) from numbers(10) settings hdfs_truncate_on_insert=1"
    )

    result = node1.query(f"select count() from test_overwrite")
    assert int(result) == 10


def test_multiple_inserts(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    table_function = f"hdfs('hdfs://hdfs1:9000/data_multiple_inserts', 'Parquet', 'a Int32, b String')"
    node1.query(f"create table test_multiple_inserts as {table_function}")
    node1.query(
        f"insert into test_multiple_inserts select number, randomString(100) from numbers(10)"
    )
    node1.query(
        f"insert into test_multiple_inserts select number, randomString(100) from numbers(20) settings hdfs_create_new_file_on_insert=1"
    )
    node1.query(
        f"insert into test_multiple_inserts select number, randomString(100) from numbers(30) settings hdfs_create_new_file_on_insert=1"
    )

    result = node1.query(f"select count() from test_multiple_inserts")
    assert int(result) == 60

    result = node1.query(f"drop table test_multiple_inserts")

    table_function = f"hdfs('hdfs://hdfs1:9000/data_multiple_inserts.gz', 'Parquet', 'a Int32, b String')"
    node1.query(f"create table test_multiple_inserts as {table_function}")
    node1.query(
        f"insert into test_multiple_inserts select number, randomString(100) FROM numbers(10)"
    )
    node1.query(
        f"insert into test_multiple_inserts select number, randomString(100) FROM numbers(20) settings hdfs_create_new_file_on_insert=1"
    )
    node1.query(
        f"insert into test_multiple_inserts select number, randomString(100) FROM numbers(30) settings hdfs_create_new_file_on_insert=1"
    )

    result = node1.query(f"select count() from test_multiple_inserts")
    assert int(result) == 60


def test_format_detection(started_cluster):
    node1.query(
        f"create table arrow_table (x UInt64) engine=HDFS('hdfs://hdfs1:9000/data.arrow')"
    )
    node1.query(f"insert into arrow_table select 1")
    result = node1.query(f"select * from hdfs('hdfs://hdfs1:9000/data.arrow')")
    assert int(result) == 1


def test_schema_inference_with_globs(started_cluster):
    node1.query(
        f"insert into table function hdfs('hdfs://hdfs1:9000/data1.jsoncompacteachrow', 'JSONCompactEachRow', 'x Nullable(UInt32)') select NULL"
    )
    node1.query(
        f"insert into table function hdfs('hdfs://hdfs1:9000/data2.jsoncompacteachrow', 'JSONCompactEachRow', 'x Nullable(UInt32)') select 0"
    )

    result = node1.query(f"desc hdfs('hdfs://hdfs1:9000/data*.jsoncompacteachrow')")
    assert result.strip() == "c1\tNullable(Int64)"

    result = node1.query(
        f"select * from hdfs('hdfs://hdfs1:9000/data*.jsoncompacteachrow')"
    )
    assert sorted(result.split()) == ["0", "\\N"]

    node1.query(
        f"insert into table function hdfs('hdfs://hdfs1:9000/data3.jsoncompacteachrow', 'JSONCompactEachRow', 'x Nullable(UInt32)') select NULL"
    )

    filename = "data{1,3}.jsoncompacteachrow"

    result = node1.query_and_get_error(
        f"desc hdfs('hdfs://hdfs1:9000/{filename}') settings schema_inference_use_cache_for_hdfs=0"
    )

    assert "All attempts to extract table structure from files failed" in result

    node1.query(
        f"insert into table function hdfs('hdfs://hdfs1:9000/data0.jsoncompacteachrow', 'TSV', 'x String') select '[123;]'"
    )

    result = node1.query_and_get_error(
        f"desc hdfs('hdfs://hdfs1:9000/data*.jsoncompacteachrow') settings schema_inference_use_cache_for_hdfs=0"
    )

    assert (
        "Cannot extract table structure from JSONCompactEachRow format file" in result
    )


def test_insert_select_schema_inference(started_cluster):
    node1.query(
        f"insert into table function hdfs('hdfs://hdfs1:9000/test.native.zst') select toUInt64(1) as x"
    )

    result = node1.query(f"desc hdfs('hdfs://hdfs1:9000/test.native.zst')")
    assert result.strip() == "x\tUInt64"

    result = node1.query(f"select * from hdfs('hdfs://hdfs1:9000/test.native.zst')")
    assert int(result) == 1


def test_cluster_join(started_cluster):
    result = node1.query(
        """
        SELECT l.id,r.id FROM hdfsCluster('test_cluster_two_shards', 'hdfs://hdfs1:9000/test_hdfsCluster/file*', 'TSV', 'id UInt32') as l
        JOIN hdfsCluster('test_cluster_two_shards', 'hdfs://hdfs1:9000/test_hdfsCluster/file*', 'TSV', 'id UInt32') as r
        ON l.id = r.id
    """
    )
    assert "AMBIGUOUS_COLUMN_NAME" not in result


def test_cluster_macro(started_cluster):
    with_macro = node1.query(
        """
        SELECT id FROM hdfsCluster('{default_cluster_macro}', 'hdfs://hdfs1:9000/test_hdfsCluster/file*', 'TSV', 'id UInt32')
    """
    )

    no_macro = node1.query(
        """
        SELECT id FROM hdfsCluster('test_cluster_two_shards', 'hdfs://hdfs1:9000/test_hdfsCluster/file*', 'TSV', 'id UInt32')
    """
    )

    assert TSV(with_macro) == TSV(no_macro)


def test_virtual_columns_2(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    table_function = (
        f"hdfs('hdfs://hdfs1:9000/parquet_2', 'Parquet', 'a Int32, b String')"
    )
    node1.query(f"insert into table function {table_function} SELECT 1, 'kek'")

    result = node1.query(f"SELECT _path FROM {table_function}")
    assert result.strip() == "hdfs://hdfs1:9000/parquet_2"

    table_function = (
        f"hdfs('hdfs://hdfs1:9000/parquet_3', 'Parquet', 'a Int32, _path String')"
    )
    node1.query(f"insert into table function {table_function} SELECT 1, 'kek'")

    result = node1.query(f"SELECT _path FROM {table_function}")
    assert result.strip() == "kek"


def check_profile_event_for_query(node, file, profile_event, amount=1):
    node.query("system flush logs")
    query_pattern = f"hdfs('hdfs://hdfs1:9000/{file}'".replace("'", "\\'")
    assert (
        int(
            node.query(
                f"select ProfileEvents['{profile_event}'] from system.query_log where query like '%{query_pattern}%' and type = 'QueryFinish' order by query_start_time_microseconds desc limit 1"
            )
        )
        == amount
    )


def check_cache_misses(node1, file, amount=1):
    check_profile_event_for_query(node1, file, "SchemaInferenceCacheMisses", amount)


def check_cache_hits(node1, file, amount=1):
    check_profile_event_for_query(node1, file, "SchemaInferenceCacheHits", amount)


def check_cache_invalidations(node1, file, amount=1):
    check_profile_event_for_query(
        node1, file, "SchemaInferenceCacheInvalidations", amount
    )


def check_cache_evictions(node1, file, amount=1):
    check_profile_event_for_query(node1, file, "SchemaInferenceCacheEvictions", amount)


def check_cache_num_rows_hits(node1, file, amount=1):
    check_profile_event_for_query(
        node1, file, "SchemaInferenceCacheNumRowsHits", amount
    )


def check_cache(node1, expected_files):
    sources = node1.query("select source from system.schema_inference_cache")
    assert sorted(map(lambda x: x.strip().split("/")[-1], sources.split())) == sorted(
        expected_files
    )


def run_describe_query(node, file):
    query = f"desc hdfs('hdfs://hdfs1:9000/{file}')"
    node.query(query)


def run_count_query(node, file):
    query = f"select count() from hdfs('hdfs://hdfs1:9000/{file}', auto, 'x UInt64')"
    return node.query(query)


def test_schema_inference_cache(started_cluster):
    node1.query("system drop schema cache")
    node1.query(
        f"insert into function hdfs('hdfs://hdfs1:9000/test_cache0.jsonl') select * from numbers(100) settings hdfs_truncate_on_insert=1"
    )
    time.sleep(1)

    run_describe_query(node1, "test_cache0.jsonl")
    check_cache(node1, ["test_cache0.jsonl"])
    check_cache_misses(node1, "test_cache0.jsonl")

    run_describe_query(node1, "test_cache0.jsonl")
    check_cache_hits(node1, "test_cache0.jsonl")

    node1.query(
        f"insert into function hdfs('hdfs://hdfs1:9000/test_cache0.jsonl') select * from numbers(100) settings hdfs_truncate_on_insert=1"
    )
    time.sleep(1)

    run_describe_query(node1, "test_cache0.jsonl")
    check_cache_invalidations(node1, "test_cache0.jsonl")

    node1.query(
        f"insert into function hdfs('hdfs://hdfs1:9000/test_cache1.jsonl') select * from numbers(100) settings hdfs_truncate_on_insert=1"
    )
    time.sleep(1)

    run_describe_query(node1, "test_cache1.jsonl")
    check_cache(node1, ["test_cache0.jsonl", "test_cache1.jsonl"])
    check_cache_misses(node1, "test_cache1.jsonl")

    run_describe_query(node1, "test_cache1.jsonl")
    check_cache_hits(node1, "test_cache1.jsonl")

    node1.query(
        f"insert into function hdfs('hdfs://hdfs1:9000/test_cache2.jsonl') select * from numbers(100) settings hdfs_truncate_on_insert=1"
    )
    time.sleep(1)

    run_describe_query(node1, "test_cache2.jsonl")
    check_cache(node1, ["test_cache1.jsonl", "test_cache2.jsonl"])
    check_cache_misses(node1, "test_cache2.jsonl")
    check_cache_evictions(node1, "test_cache2.jsonl")

    run_describe_query(node1, "test_cache2.jsonl")
    check_cache_hits(node1, "test_cache2.jsonl")

    run_describe_query(node1, "test_cache1.jsonl")
    check_cache_hits(node1, "test_cache1.jsonl")

    run_describe_query(node1, "test_cache0.jsonl")
    check_cache(node1, ["test_cache0.jsonl", "test_cache1.jsonl"])
    check_cache_misses(node1, "test_cache0.jsonl")
    check_cache_evictions(node1, "test_cache0.jsonl")

    run_describe_query(node1, "test_cache2.jsonl")
    check_cache(node1, ["test_cache0.jsonl", "test_cache2.jsonl"])
    check_cache_misses(node1, "test_cache2.jsonl")
    check_cache_evictions(node1, "test_cache2.jsonl")

    run_describe_query(node1, "test_cache2.jsonl")
    check_cache_hits(node1, "test_cache2.jsonl")

    run_describe_query(node1, "test_cache0.jsonl")
    check_cache_hits(node1, "test_cache0.jsonl")

    node1.query(
        f"insert into function hdfs('hdfs://hdfs1:9000/test_cache3.jsonl') select * from numbers(100) settings hdfs_truncate_on_insert=1"
    )
    time.sleep(1)

    files = "test_cache{0,1,2,3}.jsonl"
    run_describe_query(node1, files)
    check_cache_hits(node1, files)

    node1.query(f"system drop schema cache for hdfs")
    check_cache(node1, [])

    run_describe_query(node1, files)
    check_cache_misses(node1, files, 4)

    node1.query("system drop schema cache")
    check_cache(node1, [])

    run_describe_query(node1, files)
    check_cache_misses(node1, files, 4)

    node1.query("system drop schema cache")
    check_cache(node1, [])

    node1.query(
        f"insert into function hdfs('hdfs://hdfs1:9000/test_cache0.csv') select * from numbers(100) settings hdfs_truncate_on_insert=1"
    )
    time.sleep(1)

    res = run_count_query(node1, "test_cache0.csv")
    assert int(res) == 100
    check_cache(node1, ["test_cache0.csv"])
    check_cache_misses(node1, "test_cache0.csv")

    res = run_count_query(node1, "test_cache0.csv")
    assert int(res) == 100
    check_cache_hits(node1, "test_cache0.csv")

    node1.query(
        f"insert into function hdfs('hdfs://hdfs1:9000/test_cache0.csv') select * from numbers(200) settings hdfs_truncate_on_insert=1"
    )
    time.sleep(1)

    res = run_count_query(node1, "test_cache0.csv")
    assert int(res) == 200
    check_cache_invalidations(node1, "test_cache0.csv")

    node1.query(
        f"insert into function hdfs('hdfs://hdfs1:9000/test_cache1.csv') select * from numbers(100) settings hdfs_truncate_on_insert=1"
    )
    time.sleep(1)

    res = run_count_query(node1, "test_cache1.csv")
    assert int(res) == 100
    check_cache(node1, ["test_cache0.csv", "test_cache1.csv"])
    check_cache_misses(node1, "test_cache1.csv")

    res = run_count_query(node1, "test_cache1.csv")
    assert int(res) == 100
    check_cache_hits(node1, "test_cache1.csv")

    res = run_count_query(node1, "test_cache{0,1}.csv")
    assert int(res) == 300
    check_cache_hits(node1, "test_cache{0,1}.csv", 2)

    node1.query(f"system drop schema cache for hdfs")
    check_cache(node1, [])

    res = run_count_query(node1, "test_cache{0,1}.csv")
    assert int(res) == 300
    check_cache_misses(node1, "test_cache{0,1}.csv", 2)

    node1.query(f"system drop schema cache for hdfs")
    check_cache(node1, [])

    node1.query(
        f"insert into function hdfs('hdfs://hdfs1:9000/test_cache.parquet') select * from numbers(100) settings hdfs_truncate_on_insert=1"
    )
    time.sleep(1)
    res = node1.query(
        f"select count() from hdfs('hdfs://hdfs1:9000/test_cache.parquet')"
    )
    assert int(res) == 100
    check_cache_misses(node1, "test_cache.parquet")
    check_cache_hits(node1, "test_cache.parquet")
    check_cache_num_rows_hits(node1, "test_cache.parquet")


def test_hdfsCluster_skip_unavailable_shards(started_cluster):
    # Although skip_unavailable_shards is not set, cluster table functions should always skip unavailable shards.
    hdfs_api = started_cluster.hdfs_api
    node = started_cluster.instances["node1"]
    data = "1\tSerialize\t555.222\n2\tData\t777.333\n"
    hdfs_api.write_data("/skip_unavailable_shards", data)

    assert (
        node1.query(
            "select * from hdfsCluster('cluster_non_existent_port', 'hdfs://hdfs1:9000/skip_unavailable_shards', 'TSV', 'id UInt64, text String, number Float64') settings skip_unavailable_shards = 1"
        )
        == data
    )


def test_hdfsCluster_unset_skip_unavailable_shards(started_cluster):
    hdfs_api = started_cluster.hdfs_api
    node = started_cluster.instances["node1"]
    data = "1\tSerialize\t555.222\n2\tData\t777.333\n"
    hdfs_api.write_data("/unskip_unavailable_shards", data)

    assert (
        node1.query(
            "select * from hdfsCluster('cluster_non_existent_port', 'hdfs://hdfs1:9000/skip_unavailable_shards', 'TSV', 'id UInt64, text String, number Float64')"
        )
        == data
    )


def test_skip_empty_files(started_cluster):
    node = started_cluster.instances["node1"]

    node.query(
        f"insert into function hdfs('hdfs://hdfs1:9000/skip_empty_files1.parquet', TSVRaw) select * from numbers(0) settings hdfs_truncate_on_insert=1"
    )

    node.query(
        f"insert into function hdfs('hdfs://hdfs1:9000/skip_empty_files2.parquet') select * from numbers(1) settings hdfs_truncate_on_insert=1"
    )

    node.query_and_get_error(
        f"select * from hdfs('hdfs://hdfs1:9000/skip_empty_files1.parquet') settings hdfs_skip_empty_files=0"
    )

    node.query_and_get_error(
        f"select * from hdfs('hdfs://hdfs1:9000/skip_empty_files1.parquet', auto, 'number UINt64') settings hdfs_skip_empty_files=0"
    )

    node.query_and_get_error(
        f"select * from hdfs('hdfs://hdfs1:9000/skip_empty_files1.parquet') settings hdfs_skip_empty_files=1"
    )

    res = node.query(
        f"select * from hdfs('hdfs://hdfs1:9000/skip_empty_files1.parquet', auto, 'number UInt64') settings hdfs_skip_empty_files=1"
    )

    assert len(res) == 0

    node.query_and_get_error(
        f"select * from hdfs('hdfs://hdfs1:9000/skip_empty_files*.parquet') settings hdfs_skip_empty_files=0"
    )

    node.query_and_get_error(
        f"select * from hdfs('hdfs://hdfs1:9000/skip_empty_files*.parquet', auto, 'number UInt64') settings hdfs_skip_empty_files=0"
    )

    res = node.query(
        f"select * from hdfs('hdfs://hdfs1:9000/skip_empty_files*.parquet') settings hdfs_skip_empty_files=1"
    )

    assert int(res) == 0

    res = node.query(
        f"select * from hdfs('hdfs://hdfs1:9000/skip_empty_files*.parquet', auto, 'number UInt64') settings hdfs_skip_empty_files=1"
    )

    assert int(res) == 0


def test_read_subcolumns(started_cluster):
    node = started_cluster.instances["node1"]

    node.query(
        f"insert into function hdfs('hdfs://hdfs1:9000/test_subcolumns.tsv', auto, 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)') select ((1, 2), 3)"
    )

    node.query(
        f"insert into function hdfs('hdfs://hdfs1:9000/test_subcolumns.jsonl', auto, 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)') select ((1, 2), 3)"
    )

    res = node.query(
        f"select a.b.d, _path, a.b, _file, a.e from hdfs('hdfs://hdfs1:9000/test_subcolumns.tsv', auto, 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)')"
    )

    assert (
        res
        == "2\thdfs://hdfs1:9000/test_subcolumns.tsv\t(1,2)\ttest_subcolumns.tsv\t3\n"
    )

    res = node.query(
        f"select a.b.d, _path, a.b, _file, a.e from hdfs('hdfs://hdfs1:9000/test_subcolumns.jsonl', auto, 'a Tuple(b Tuple(c UInt32, d UInt32), e UInt32)')"
    )

    assert (
        res
        == "2\thdfs://hdfs1:9000/test_subcolumns.jsonl\t(1,2)\ttest_subcolumns.jsonl\t3\n"
    )

    res = node.query(
        f"select x.b.d, _path, x.b, _file, x.e from hdfs('hdfs://hdfs1:9000/test_subcolumns.jsonl', auto, 'x Tuple(b Tuple(c UInt32, d UInt32), e UInt32)')"
    )

    assert (
        res
        == "0\thdfs://hdfs1:9000/test_subcolumns.jsonl\t(0,0)\ttest_subcolumns.jsonl\t0\n"
    )

    res = node.query(
        f"select x.b.d, _path, x.b, _file, x.e from hdfs('hdfs://hdfs1:9000/test_subcolumns.jsonl', auto, 'x Tuple(b Tuple(c UInt32, d UInt32), e UInt32) default ((42, 42), 42)')"
    )

    assert (
        res
        == "42\thdfs://hdfs1:9000/test_subcolumns.jsonl\t(42,42)\ttest_subcolumns.jsonl\t42\n"
    )


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
