"""Executable regression test for the `/play` startup reconciliation (`reconcileStartup`).

The Web UI reconciles the saved IndexedDB workspace on load: truly blank saved tabs are
pruned, a workspace where every saved tab was blank falls back to a single fresh tab, a
tab whose editor was cleared after a run (it still holds a `result.ran` snapshot) is
preserved, and a stale reload URL naming a just-pruned blank tab does not resurrect it.

The stateless suite has no JavaScript runtime, so the startup contracts are driven by a
Node.js harness (`reconcile_harness.js`) executed inside the `clickhouse/mysql-js-client`
container (node:22-alpine): it fetches `/play` from a real server, runs the extracted page
script in a `vm` context with a stubbed browser environment (including a functional
in-memory IndexedDB fake), seeds saved tabs and `history.state` per scenario, and asserts
both the live tab state and what gets persisted back.
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


def test_play_reconcile_startup(started_cluster, nodejs_container):
    tarstream = io.BytesIO()
    with tarfile.open(fileobj=tarstream, mode="w") as tar:
        tar.add(
            os.path.join(SCRIPT_DIR, "reconcile_harness.js"),
            arcname="reconcile_harness.js",
        )
    tarstream.seek(0)
    nodejs_container.put_archive("/usr/app", tarstream)

    url = "http://{}:8123/play".format(started_cluster.get_instance_ip("node"))
    code, (stdout, stderr) = nodejs_container.exec_run(
        ["node", "/usr/app/reconcile_harness.js", url], demux=True
    )
    out = (stdout or b"").decode()
    err = (stderr or b"").decode()
    assert code == 0, "harness failed:\n{}\n{}".format(out, err)
    assert "All scenarios passed" in out
