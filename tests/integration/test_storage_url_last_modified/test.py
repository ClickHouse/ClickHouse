import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

cluster = ClickHouseCluster(__file__)
server = cluster.add_instance("node")

PORT = 8000


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        run_last_modified_server()

        yield cluster
    finally:
        cluster.shutdown()


def run_last_modified_server():
    container_id = cluster.get_container_id("node")
    file_name = "last_modified_server.py"

    cluster.copy_file_to_container(
        container_id,
        os.path.join(os.path.dirname(os.path.realpath(__file__)), file_name),
        f"/{file_name}",
    )

    cluster.exec_in_container(
        container_id,
        [
            "bash",
            "-c",
            f"python3 /{file_name} localhost {PORT} > {file_name}.log 2>&1",
        ],
        detach=True,
        user="root",
    )

    wait_condition(
        lambda: cluster.exec_in_container(
            container_id,
            ["curl", "-s", f"http://localhost:{PORT}/"],
            nothrow=True,
        ),
        lambda response: '{"status":"ok"}' in response,
        max_attempts=20,
        delay=0.5,
    )


def query_time(path):
    return server.query(
        f"SELECT toUnixTimestamp(_time) FROM url('http://localhost:{PORT}/{path}', CSV, 'a UInt32')"
    ).strip()


# RFC 9110, 5.6.7: a recipient must accept all three forms of an `HTTP-date`, so `_time` has to be
# populated from `Last-Modified` no matter which one the server used.
@pytest.mark.parametrize(
    "path, expected",
    [
        ("imf-fixdate", "784111777"),  # 1994-11-06 08:49:37 UTC
        ("rfc850", "1162802977"),  # 2006-11-06 08:49:37 UTC
        ("asctime", "784111777"),  # 1994-11-06 08:49:37 UTC
    ],
)
def test_last_modified(started_cluster, path, expected):
    assert query_time(path) == expected


# An unknown modification time must stay NULL rather than become the epoch.
@pytest.mark.parametrize("path", ["missing", "malformed"])
def test_last_modified_unknown(started_cluster, path):
    assert query_time(path) == "\\N"
