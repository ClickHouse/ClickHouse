import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/config.xml"])

# The config sets user_files_path to the relative value "user_files".
# The server resolves it against the main path setting to /var/lib/clickhouse/user_files/.
user_files_absolute = "/var/lib/clickhouse/user_files"
user_files_dirname = "user_files"


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

    node.exec_in_container(
        ["bash", "-c", "mkdir -p {}".format(user_files_absolute)],
        privileged=True,
        user="root",
    )

    node.exec_in_container(
        [
            "bash",
            "-c",
            'echo "{}" > {}'.format(
                some_data,
                user_files_absolute + "/relative_user_file_test",
            ),
        ],
        privileged=True,
        user="root",
    )

    test_requests = [
        ("relative_user_file_test", "2"),
        (
            "../" + user_files_dirname + "/relative_user_file_test",
            "2",
        ),
    ]

    for pattern, value in test_requests:
        assert (
            node.query(
                """
            select count() from file('{}', 'TSV', 'text String, number Float64')
        """.format(
                    pattern
                )
            )
            == "{}\n".format(value)
        )
