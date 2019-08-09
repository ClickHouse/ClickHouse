import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node')

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_globs(start_cluster):
    some_data = "\t555.222\nData\t777.333"
    path_to_userfiles = "/var/lib/clickhouse/user_files/"
    dirs = ["dir1/", "dir2/"]

    for dir in dirs:
        node.exec_in_container(['bash', '-c', 'mkdir {}{}'.format(path_to_userfiles, dir)], privileged=True, user='root')

    # all directories appeared in files must be listed in dirs
    files = ["dir1/file1", "dir1/file2",
             "dir2/file1", "dir2/file2", "dir2/file11",
             "file1"]

    for filename in files:
        node.exec_in_container(['bash', '-c', 'echo "{}{}" > {}{}'.format(filename, some_data, path_to_userfiles, filename)], privileged=True, user='root')

    test_requests = [("dir1/file*", "4"),
                     ("dir1/file?", "4"),
                     ("dir1/file{0..9}", "4"),
                     ("dir2/file*", "6"),
                     ("dir2/file?", "4"),
                     ("*", "2")]

    for pattern, value in test_requests:
        assert node.query('''
            select count(*) from file('{}', 'TSV', 'text String, number Float64')
        '''.format(pattern)) == '{}\n'.format(value)