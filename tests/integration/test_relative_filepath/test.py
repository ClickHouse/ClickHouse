import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/config.xml'])
path_to_userfiles_from_defaut_config = "user_files"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_filepath(start_cluster):
    # 2 rows data
    some_data = "Test\t111.222\nData\t333.444"

    node.exec_in_container(['bash', '-c', 'mkdir -p {}'.format(
        path_to_userfiles_from_defaut_config
    )], privileged=True, user='root')

    node.exec_in_container(['bash', '-c', 'echo "{}" > {}'.format(
        some_data,
        path_to_userfiles_from_defaut_config + "/relative_user_file_test"
    )], privileged=True, user='root')

    test_requests = [("relative_user_file_test", "2"),
                     ("../" + path_to_userfiles_from_defaut_config + "/relative_user_file_test", "2")]

    for pattern, value in test_requests:
        assert node.query('''
            select count() from file('{}', 'TSV', 'text String, number Float64')
        '''.format(pattern)) == '{}\n'.format(value)
