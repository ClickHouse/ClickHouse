"""Executable regression test for the `/play` NOTEBOOK mode (several cells in one tab).

The Web UI can turn a tab into a notebook: an ordered list of query and Markdown cells. The
display state that used to be tab-wide then belongs to one cell, and the single shared run row
is docked under the cell whose run it reports. This test pins the three contracts that follow
from that: the shared Logs/Metrics toggles and the logo follow the cell the row is docked under
(the running cell while a run is in flight, not the cell the editor moved to), color modes and
pinned columns persist onto the owning cell's own result snapshot without rewriting another
cell's state, and a text cell's Markdown may link to ordinary relative targets while other
schemes and protocol-relative targets stay unrendered.

The stateless suite has no JavaScript runtime, so these contracts are driven by a Node.js
harness (`notebook_harness.js`) executed inside the `clickhouse/mysql-js-client` container
(node:22-alpine): it fetches `/play` from a real server, runs the extracted page script in a
`vm` context with a stubbed browser environment, drives the notebook the way the UI does, and
asserts where the state lands.
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


def test_play_notebook(started_cluster, nodejs_container):
    tarstream = io.BytesIO()
    with tarfile.open(fileobj=tarstream, mode="w") as tar:
        tar.add(
            os.path.join(SCRIPT_DIR, "notebook_harness.js"),
            arcname="notebook_harness.js",
        )
    tarstream.seek(0)
    nodejs_container.put_archive("/usr/app", tarstream)

    url = "http://{}:8123/play".format(started_cluster.get_instance_ip("node"))
    code, (stdout, stderr) = nodejs_container.exec_run(
        ["node", "/usr/app/notebook_harness.js", url], demux=True
    )
    out = (stdout or b"").decode()
    err = (stderr or b"").decode()
    assert code == 0, "harness failed:\n{}\n{}".format(out, err)
    assert "All scenarios passed" in out
    # Pin the scenarios by name so a harness edit that silently drops one cannot pass
    # as "all scenarios passed".
    for scenario in (
        "chrome-follows-running-cell",
        "color-state-is-per-cell",
        "markdown-relative-links",
    ):
        assert (
            "PASS [{}]".format(scenario) in out
        ), "scenario {} did not run:\n{}".format(scenario, out)
