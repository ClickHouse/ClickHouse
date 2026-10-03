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
import urllib.parse

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
    # The `run=1`-marker scenarios are the regression tests for scoping the URL
    # marker to the tab that produced it, and the `dirty-startup-*` ones cover an
    # edit that lands while the saved workspace is still loading; pin them by name
    # so a harness edit that silently drops a scenario cannot pass as "all
    # scenarios passed".
    for scenario in (
        "run-marker-per-tab",
        "legacy-popstate-clears-policy",
        "run-marker-kept-for-own-run",
        "run-marker-plain-load",
        "dirty-startup-run-leak",
        "dirty-startup-adopt-restamp",
        "dirty-startup-allblank-edit-survives",
        "dirty-startup-allblank-entry-reowned",
        "dirty-startup-merge-entry-reowned",
        "shape-not-stamped-before-run",
        "dirty-startup-format",
        "format-connection-change",
        "auth-header-cases",
    ):
        assert "PASS [{}]".format(scenario) in out, "scenario {} did not run:\n{}".format(
            scenario, out
        )


def test_play_auth_headers_preserve_credentials_with_database_path(started_cluster):
    user = "play:юзер"
    password = "  päss 密码  "

    def quote(value):
        return urllib.parse.quote(value, safe="-_.!~*'()")

    node.query("DROP USER IF EXISTS '{}'".format(user))
    try:
        node.query(
            "CREATE USER '{}' IDENTIFIED WITH sha256_password BY '{}'".format(
                user, password
            )
        )

        encoded_headers = {
            "X-ClickHouse-Auth-Encoding": "percent",
            "X-ClickHouse-User": quote(user),
            "X-ClickHouse-Key": quote(password),
        }

        # A scripted /play server_address may include a database path. The encoding marker,
        # rather than Authorization, controls decoding, so an intermediary may strip or rewrite
        # Authorization without corrupting UTF-8 or surrounding-space credentials.
        for authorization in ("never", None, "Basic Zm9vOmJhcg=="):
            headers = dict(encoded_headers)
            if authorization is not None:
                headers["Authorization"] = authorization

            response = node.http_request(
                "default",
                method="POST",
                params={
                    "add_http_cors_header": "1",
                    "http_allow_database_as_path": "1",
                    "http_allow_table_as_file": "0",
                },
                data="SELECT currentUser()",
                headers=headers,
            )
            assert response.status_code == 200, response.text
            assert response.content.decode("utf-8") == user + "\n"
            assert response.headers["X-ClickHouse-Auth-Encoding"] == "percent"
            assert "x-clickhouse-auth-encoding" in response.headers[
                "Access-Control-Expose-Headers"
            ].lower()
    finally:
        node.query("DROP USER IF EXISTS '{}'".format(user))
