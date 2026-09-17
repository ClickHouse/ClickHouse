"""Executable regression test for the `/play` EXPLAIN PLAN tree.

The Web UI renders an `EXPLAIN` as a collapsible tree of plan nodes in its own Plan view, rather than
as the server's indented text. The tree comes from `EXPLAIN PLAN json = 1`, which the page adds to the
statement itself so that plain `EXPLAIN <query>` is enough - only the bytes on the wire change, and the
query kept in the tab, the history and the Download button stays as the user wrote it.

The contracts pinned here are the ones that decide whether a query still runs at all. `json` is
accepted only by `EXPLAIN PLAN` (every other kind rejects it with `UNKNOWN_SETTING`), so the rewrite
must recognise the kind - an absent one means `PLAN` - and leave `PIPELINE`, `AST`, `SYNTAX`,
`QUERY TREE`, `ESTIMATE`, `TABLE OVERRIDE` and `CURRENT TRANSACTION` alone. It must also leave the SVG
path alone, which it gets for free: a `digraph` comes from `graph = 1`, a PIPELINE/AST setting. A
`json` the user wrote is never overwritten in either direction, since `json = 0` is how the indented
text is asked for back. The insertion goes at the front of the settings list and carries its comma
only when a list is already there, so both spellings are valid SQL. And because the decision is made
on the lexer's tokens rather than by matching text, an `EXPLAIN` inside a string literal or a comment
is inert, while the insertion offset - a sum over the tokens, insignificant ones included - survives
leading comments, odd whitespace and multi-byte characters.

The stateless suite has no JavaScript runtime, so the contracts are driven by a Node.js harness
(`explain_harness.js`) executed inside the `clickhouse/mysql-js-client` container (node:22-alpine):
it fetches `/play` from a real server, extracts the helpers from the page script, and asserts on
their results.
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
