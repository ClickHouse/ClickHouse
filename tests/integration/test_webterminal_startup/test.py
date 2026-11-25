"""Executable regression test for the `/webterminal` startup and authentication flow.

A web terminal embedded in a host page (`play.html`, or the ClickHouse Cloud console) is loaded
without credentials in its URL and receives them from the host page via `postMessage`. It must
therefore wait for that message rather than open a connection of its own: such a connection can
only authenticate as a passwordless user and is torn down and replaced as soon as the credentials
arrive, which used to print `Connecting...` twice. The other paths - opened as a normal page, or
with a `user` URL parameter - must still connect immediately, and a host page that never sends
credentials must not leave a terminal that neither connects nor prompts.

The stateless suite has no JavaScript runtime, so these contracts are driven by a Node.js harness
(`startup_harness.js`) executed inside the `clickhouse/mysql-js-client` container (node:22-alpine):
it fetches `/webterminal` from a real server and runs the extracted page script in a `vm` context
with a stubbed browser environment (a terminal that records what is displayed and a `WebSocket`
that records every connection attempt), then drives each scenario and asserts both.
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
# The endpoint is enabled by default, so the page is served without any extra configuration.
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


def test_webterminal_startup(started_cluster, nodejs_container):
    tarstream = io.BytesIO()
    with tarfile.open(fileobj=tarstream, mode="w") as tar:
        tar.add(
            os.path.join(SCRIPT_DIR, "startup_harness.js"),
            arcname="startup_harness.js",
        )
    tarstream.seek(0)
    nodejs_container.put_archive("/usr/app", tarstream)

    url = "http://{}:8123/webterminal".format(started_cluster.get_instance_ip("node"))
    code, (stdout, stderr) = nodejs_container.exec_run(
        ["node", "/usr/app/startup_harness.js", url], demux=True
    )
    out = (stdout or b"").decode()
    err = (stderr or b"").decode()
    assert code == 0, "harness failed:\n{}\n{}".format(out, err)
    assert "All scenarios passed" in out
    # Pin the scenarios by name so a harness edit that silently drops one cannot
    # pass as "all scenarios passed". `embedded-credentials` is the regression
    # test for connecting only once, from the host page's credentials.
    for scenario in (
        "standalone",
        "embedded-credentials",
        "embedded-default-user",
        "embedded-rejected",
        "embedded-idle-takeover",
        "embedded-user-in-url",
        "untrusted-credentials",
    ):
        assert "PASS [{}]".format(scenario) in out, "scenario {} did not run:\n{}".format(
            scenario, out
        )
