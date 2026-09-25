"""Executable regression test for the Web UI's password-manager round trip.

The `/play` page offers the login to the browser's password manager after a run
(`storeCredentials`). An empty `user` field authenticates implicitly - as the server's
`default` user, or as the user embedded in the server URL's userinfo - and the login is
remembered under that effective name (`effectiveConnectionUser`). When the password manager
refills that name into the field on the next visit, the connection must not change: the
request URL (`userUrlParam`) must be the same as for an empty field, with no forced
`user=default` that would override the URL userinfo, and the connection identity used by
the history / database-panel gates must compare equal. A field naming a different user
still takes precedence.

The stateless suite has no JavaScript runtime, so the contract is driven by a Node.js
harness (`credentials_harness.js`) executed inside the `clickhouse/mysql-js-client`
container (node:22-alpine): it fetches `/play` from a real server, extracts the real
functions from the page script, runs them in a `vm` context with a fake
`PasswordCredential` / `navigator.credentials` / `fetch`, and asserts what gets stored and
which request URLs are built before and after the refill.
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


def test_play_credentials_store(started_cluster, nodejs_container):
    tarstream = io.BytesIO()
    with tarfile.open(fileobj=tarstream, mode="w") as tar:
        tar.add(
            os.path.join(SCRIPT_DIR, "credentials_harness.js"),
            arcname="credentials_harness.js",
        )
    tarstream.seek(0)
    nodejs_container.put_archive("/usr/app", tarstream)

    url = "http://{}:8123/play".format(started_cluster.get_instance_ip("node"))
    code, (stdout, stderr) = nodejs_container.exec_run(
        ["node", "/usr/app/credentials_harness.js", url], demux=True
    )
    out = (stdout or b"").decode()
    err = (stderr or b"").decode()
    assert code == 0, "harness failed:\n{}\n{}".format(out, err)
    assert "All scenarios passed" in out
    # Pin the round-trip scenarios by name so a harness edit that silently drops one
    # cannot pass as "all scenarios passed".
    for scenario in (
        "implicit-default-round-trip",
        "userinfo-round-trip",
        "explicit-default-overrides-userinfo",
        "no-password-credential-api-skips",
        "no-request-builder-bypasses-userUrlParam",
    ):
        assert (
            "PASS [{}]".format(scenario) in out
        ), "scenario {} did not run:\n{}".format(scenario, out)
