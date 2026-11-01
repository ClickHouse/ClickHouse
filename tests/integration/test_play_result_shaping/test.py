"""Executable regression test for the `/play` result shaping.

The Web UI sorts, filters and pages a result server-side: the controls in the column headers,
on the selected cell and in the pager under the table choose a sort, a per-column filter and a
page, and the query is re-run with them as the `order` / `filter` / `limit` + `page`
query-construction settings, which the server materializes by wrapping the query as a derived
table with an outer `ORDER BY` / `WHERE` / `LIMIT`. The contracts pinned here: the sort-arrow
semantics (an active direction deactivates, the other direction replaces it, a plain click makes
the column the only sort key while Shift appends after the keys already in effect and preserves
their order), the back-quoting and literal/`LIKE` escaping that make an arbitrary column name and
an arbitrary cell value usable, the `AND` composition of per-column filters, the translation of a
page into `limit` + `page`, the validation of a shape read back from a URL or a stored snapshot,
and the statement identity a shape is bound to - which must treat the two spellings the launch
paths produce for one statement (with and without its trailing `;`) as the same statement, or a
shape would be dropped by its own re-run.

The stateless suite has no JavaScript runtime, so the contracts are driven by a Node.js
harness (`shape_harness.js`) executed inside the `clickhouse/mysql-js-client` container
(node:22-alpine): it fetches `/play` from a real server, extracts the shape helpers from the
page script, and asserts on their results.
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


def test_play_result_shaping(started_cluster, nodejs_container):
    tarstream = io.BytesIO()
    with tarfile.open(fileobj=tarstream, mode="w") as tar:
        tar.add(
            os.path.join(SCRIPT_DIR, "shape_harness.js"),
            arcname="shape_harness.js",
        )
    tarstream.seek(0)
    nodejs_container.put_archive("/usr/app", tarstream)

    url = "http://{}:8123/play".format(started_cluster.get_instance_ip("node"))
    code, (stdout, stderr) = nodejs_container.exec_run(
        ["node", "/usr/app/shape_harness.js", url], demux=True
    )
    out = (stdout or b"").decode()
    err = (stderr or b"").decode()
    assert code == 0, "harness failed:\n{}\n{}".format(out, err)
    assert "All scenarios passed" in out
