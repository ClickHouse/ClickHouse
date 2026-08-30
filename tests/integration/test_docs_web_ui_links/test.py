"""Executable regression test for the link rewriting of the built-in `/docs` page.

The embedded documentation refers to a sibling entity with a bare fragment link, e.g. the
`enable_group_by_top_k_optimization` setting links to
`[query_plan_max_limit_for_top_k_optimization](#query_plan_max_limit_for_top_k_optimization)`.
That works on the documentation website, where all the settings share a single page, but in
the built-in `/docs` page every entity has a page of its own, so the fragment has no target
in the document and clicking such a link did nothing. `rewriteLinks` must resolve a fragment
without an in-page target to the entity it names and open that entity instead, while a
genuine in-page anchor keeps navigating within the page, the "#" heading anchors are left
alone (their href is a whole app state hash, not a link into the document), and an ambiguous
name is not guessed.

The stateless suite has no JavaScript runtime, so the contracts are driven by a Node.js
harness (`docs_links_harness.js`) executed inside the `clickhouse/mysql-js-client` container
(node:22-alpine): it fetches `/docs` and the `system.documentation` corpus from a real
server, extracts the real link-rewriting helpers from the page script, runs `rewriteLinks`
over a minimal DOM shim, clicks the links, and asserts which navigation each click performs.
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


def test_docs_web_ui_links(started_cluster, nodejs_container):
    tarstream = io.BytesIO()
    with tarfile.open(fileobj=tarstream, mode="w") as tar:
        tar.add(
            os.path.join(SCRIPT_DIR, "docs_links_harness.js"),
            arcname="docs_links_harness.js",
        )
    tarstream.seek(0)
    nodejs_container.put_archive("/usr/app", tarstream)

    url = "http://{}:8123".format(started_cluster.get_instance_ip("node"))
    code, (stdout, stderr) = nodejs_container.exec_run(
        ["node", "/usr/app/docs_links_harness.js", url], demux=True
    )
    out = (stdout or b"").decode()
    err = (stderr or b"").decode()
    assert code == 0, "harness failed:\n{}\n{}".format(out, err)
    assert "All scenarios passed" in out
