"""Executable regression test for the `/play` EXPLAIN PLAN tree.

The Web UI renders a query plan as a collapsible tree (and a graph) of plan nodes in its own Plan view.
The statement the user runs is sent as written; the Plan view makes a second call, the statement with
`json = 1, indexes = 1, header = 1, actions = 1` added to an `EXPLAIN`, or wrapped into one for a `SELECT`.

The contracts pinned here are the ones that decide whether that call works at all. `json` is accepted
only by `EXPLAIN PLAN` (every other kind rejects it with `UNKNOWN_SETTING`), and not together with
`distributed = 1`, so those statements get no Plan view. A `json` the user wrote is never overwritten in
either direction, since `json = 0` is how the indented text is asked for back. The insertion goes at the
front of the settings list and carries its comma only when a list is already there. And because the
decision is made on the lexer's tokens rather than by matching text, an `EXPLAIN` inside a string literal
or a comment is inert, while the insertion offset survives leading comments, odd whitespace and
multi-byte characters.

The stateless suite has no JavaScript runtime, so the contracts are driven by a Node.js harness
(`explain_harness.js`) executed inside the `clickhouse/mysql-js-client` container (node:22-alpine):
it fetches `/play` from a real server, extracts the helpers from the page script, asserts on their
results, and sends the statements and their plan requests to the same server.
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


def test_play_explain_plan(started_cluster, nodejs_container):
    tarstream = io.BytesIO()
    with tarfile.open(fileobj=tarstream, mode="w") as tar:
        tar.add(
            os.path.join(SCRIPT_DIR, "explain_harness.js"),
            arcname="explain_harness.js",
        )
    tarstream.seek(0)
    nodejs_container.put_archive("/usr/app", tarstream)

    url = "http://{}:8123/play".format(started_cluster.get_instance_ip("node"))
    code, (stdout, stderr) = nodejs_container.exec_run(
        ["node", "/usr/app/explain_harness.js", url], demux=True
    )
    out = (stdout or b"").decode()
    err = (stderr or b"").decode()
    assert code == 0, "harness failed:\n{}\n{}".format(out, err)
    assert "All scenarios passed" in out
