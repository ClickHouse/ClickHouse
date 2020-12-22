import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node')
path_to_userfiles_from_defaut_config = "/var/lib/clickhouse/user_files/"  # should be the same as in config file


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)
        raise ex
    finally:
        cluster.shutdown()


def test_strange_filenames(start_cluster):
    # 2 rows data
    some_data = "\t111.222\nData\t333.444"

    node.exec_in_container(['bash', '-c', 'mkdir {}strange_names/'.format(path_to_userfiles_from_defaut_config)],
                           privileged=True, user='root')

    files = ["p.o.i.n.t.s",
             "b}{ra{ces",
             "b}.o{t.h"]

    # filename inside testing data for debug simplicity
    for filename in files:
        node.exec_in_container(['bash', '-c', 'echo "{}{}" > {}strange_names/{}'.format(filename, some_data,
                                                                                        path_to_userfiles_from_defaut_config,
                                                                                        filename)], privileged=True,
                               user='root')

    test_requests = [("p.o.??n.t.s", "2"),
                     ("p.o.*t.s", "2"),
                     ("b}{r?{ces", "2"),
                     ("b}*ces", "2"),
                     ("b}.?{t.h", "2")]

    for pattern, value in test_requests:
        assert node.query('''
            select count(*) from file('strange_names/{}', 'TSV', 'text String, number Float64')
        '''.format(pattern)) == '{}\n'.format(value)
        assert node.query('''
            select count(*) from file('{}strange_names/{}', 'TSV', 'text String, number Float64')
        '''.format(path_to_userfiles_from_defaut_config, pattern)) == '{}\n'.format(value)


def test_linear_structure(start_cluster):
    # 2 rows data
    some_data = "\t123.456\nData\t789.012"

    files = ["file1", "file2", "file3", "file4", "file5",
             "file000", "file111", "file222", "file333", "file444",
             "a_file", "b_file", "c_file", "d_file", "e_file",
             "a_data", "b_data", "c_data", "d_data", "e_data"]

    # filename inside testing data for debug simplicity
    for filename in files:
        node.exec_in_container(['bash', '-c',
                                'echo "{}{}" > {}{}'.format(filename, some_data, path_to_userfiles_from_defaut_config,
                                                            filename)], privileged=True, user='root')

    test_requests = [("file{0..9}", "10"),
                     ("file?", "10"),
                     ("nothing*", "0"),
                     ("file{0..9}{0..9}{0..9}", "10"),
                     ("file{000..999}", "10"),
                     ("file???", "10"),
                     ("file*", "20"),
                     ("a_{file,data}", "4"),
                     ("?_{file,data}", "20"),
                     ("{a,b,c,d,e}_{file,data}", "20"),
                     ("{a,b,c,d,e}?{file,data}", "20"),
                     ("*", "40")]

    for pattern, value in test_requests:
        assert node.query('''
            select count(*) from file('{}', 'TSV', 'text String, number Float64')
        '''.format(pattern)) == '{}\n'.format(value)
        assert node.query('''
            select count(*) from file('{}{}', 'TSV', 'text String, number Float64')
        '''.format(path_to_userfiles_from_defaut_config, pattern)) == '{}\n'.format(value)


def test_deep_structure(start_cluster):
    # 2 rows data
    some_data = "\t135.791\nData\t246.802"
    dirs = ["directory1/", "directory2/", "some_more_dir/", "we/",
            "directory1/big_dir/",
            "directory1/dir1/", "directory1/dir2/", "directory1/dir3/",
            "directory2/dir1/", "directory2/dir2/", "directory2/one_more_dir/",
            "some_more_dir/yet_another_dir/",
            "we/need/", "we/need/to/", "we/need/to/go/", "we/need/to/go/deeper/"]

    for dir in dirs:
        node.exec_in_container(['bash', '-c', 'mkdir {}{}'.format(path_to_userfiles_from_defaut_config, dir)],
                               privileged=True, user='root')

    # all directories appeared in files must be listed in dirs
    files = []
    for i in range(10):
        for j in range(10):
            for k in range(10):
                files.append("directory1/big_dir/file" + str(i) + str(j) + str(k))

    for dir in dirs:
        files.append(dir + "file")

    # filename inside testing data for debug simplicity
    for filename in files:
        node.exec_in_container(['bash', '-c',
                                'echo "{}{}" > {}{}'.format(filename, some_data, path_to_userfiles_from_defaut_config,
                                                            filename)], privileged=True, user='root')

    test_requests = [("directory{1..5}/big_dir/*", "2002"), ("directory{0..6}/big_dir/*{0..9}{0..9}{0..9}", "2000"),
                     ("?", "0"),
                     ("directory{0..5}/dir{1..3}/file", "10"), ("directory{0..5}/dir?/file", "10"),
                     ("we/need/to/go/deeper/file", "2"), ("*/*/*/*/*/*", "2"), ("we/need/??/go/deeper/*?*?*?*?*", "2")]

    for pattern, value in test_requests:
        assert node.query('''
            select count(*) from file('{}', 'TSV', 'text String, number Float64')
        '''.format(pattern)) == '{}\n'.format(value)
        assert node.query('''
            select count(*) from file('{}{}', 'TSV', 'text String, number Float64')
        '''.format(path_to_userfiles_from_defaut_config, pattern)) == '{}\n'.format(value)


def test_table_function_and_virtual_columns(start_cluster):
    node.exec_in_container(['bash', '-c', 'mkdir -p {}some/path/to/'.format(path_to_userfiles_from_defaut_config)])
    node.exec_in_container(['bash', '-c', 'touch {}some/path/to/data.CSV'.format(path_to_userfiles_from_defaut_config)])
    node.query(
        "insert into table function file('some/path/to/data.CSV', CSV, 'n UInt8, s String') select number, concat('str_', toString(number)) from numbers(100000)")
    assert node.query(
        "select count() from file('some/path/to/data.CSV', CSV, 'n UInt8, s String')").rstrip() == '100000'
    node.query("insert into table function file('nonexist.csv', 'CSV', 'val1 UInt32') values (1)")
    assert node.query("select * from file('nonexist.csv', 'CSV', 'val1 UInt32')").rstrip() == '1'
    assert "nonexist.csv" in node.query("select _path from file('nonexis?.csv', 'CSV', 'val1 UInt32')").rstrip()
    assert "nonexist.csv" in node.query("select _path from file('nonexist.csv', 'CSV', 'val1 UInt32')").rstrip()
    assert "nonexist.csv" == node.query("select _file from file('nonexis?.csv', 'CSV', 'val1 UInt32')").rstrip()
    assert "nonexist.csv" == node.query("select _file from file('nonexist.csv', 'CSV', 'val1 UInt32')").rstrip()
