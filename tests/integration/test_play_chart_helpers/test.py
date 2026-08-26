"""Executable regression test for the `/play` chart data preparation.

The Web UI orders the chart data by the x column before plotting (uPlot locates the point
under the hover cursor with a binary search that requires ascending x), converts `Date`,
`Date32`, `DateTime` and `DateTime64` strings to Unix timestamps, and formats them back in
ISO 8601 with exactly as much of the time of day as the data carries. The regressions
pinned here: a descending x - including quoted 64-bit integers above 2^53, whose
neighbours collapse into the same double - must be reordered, and a `DateTime64` label
must keep the digits of its scale instead of being cut at milliseconds.

The stateless suite has no JavaScript runtime, so the contracts are driven by a Node.js
harness (`chart_harness.js`) executed inside the `clickhouse/mysql-js-client` container
(node:22-alpine): it fetches `/play` from a real server, extracts the chart helpers from
the page script, and asserts on their results.
"""

import io
import os
import tarfile

import docker
import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_PATH = get_docker_compose_path()

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def nodejs_container(started_cluster):
    docker_compose = os.path.join(
        DOCKER_COMPOSE_PATH, "docker_compose_mysql_js_client.yml"
    )
    run_and_check(
        cluster.compose_cmd(
            "--env-file",
            cluster.instances["node"].env_file,
            "-f",
            docker_compose,
            "up",
            "--force-recreate",
            "-d",
            "--no-build",
        )
    )
    yield docker.DockerClient(
        base_url="unix:///var/run/docker.sock",
        version=cluster.docker_api_version,
        timeout=600,
    ).containers.get(cluster.get_instance_docker_id("mysqljs1"))


def test_play_chart_helpers(started_cluster, nodejs_container):
    tarstream = io.BytesIO()
    with tarfile.open(fileobj=tarstream, mode="w") as tar:
        tar.add(
            os.path.join(SCRIPT_DIR, "chart_harness.js"),
            arcname="chart_harness.js",
        )
    tarstream.seek(0)
    nodejs_container.put_archive("/usr/app", tarstream)

    url = "http://{}:8123/play".format(started_cluster.get_instance_ip("node"))
    code, (stdout, stderr) = nodejs_container.exec_run(
        ["node", "/usr/app/chart_harness.js", url], demux=True
    )
    out = (stdout or b"").decode()
    err = (stderr or b"").decode()
    assert code == 0, "harness failed:\n{}\n{}".format(out, err)
    assert "All scenarios passed" in out
