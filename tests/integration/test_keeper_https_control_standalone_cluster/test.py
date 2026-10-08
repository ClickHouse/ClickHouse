#!/usr/bin/env python3

import os
import time

import pytest
import requests
import urllib3

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

SSL_FILES = ["server.crt", "server.key", "dhparam.pem"]

node1 = cluster.add_instance(
    "node1",
    hostname="node1",
    stay_alive=True,
    with_remote_database_disk=False,
)
node2 = cluster.add_instance(
    "node2",
    hostname="node2",
    stay_alive=True,
    with_remote_database_disk=False,
)
node3 = cluster.add_instance(
    "node3",
    hostname="node3",
    stay_alive=True,
    with_remote_database_disk=False,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _install_keeper_configs(node, idx):
    configs_dir = os.path.join(os.path.dirname(__file__), "configs")
    container_conf_dir = "/etc/clickhouse-keeper"

    node.exec_in_container(
        [
            "bash",
            "-c",
            "mkdir -p {conf} "
            "/var/log/clickhouse-keeper "
            "/var/lib/clickhouse-keeper/coordination/log "
            "/var/lib/clickhouse-keeper/coordination/snapshots".format(
                conf=container_conf_dir
            ),
        ],
        user="root",
    )

    node.copy_file_to_container(
        os.path.join(configs_dir, "keeper_config{idx}.xml".format(idx=idx)),
        os.path.join(container_conf_dir, "keeper_config.xml"),
    )
    for name in SSL_FILES:
        node.copy_file_to_container(
            os.path.join(configs_dir, name),
            os.path.join(container_conf_dir, name),
        )


def _start_standalone_keeper(node):
    cmd = [
        "bash",
        "-c",
        "nohup bash -c '"
        "if command -v clickhouse-keeper >/dev/null 2>&1; then "
        "  exec clickhouse-keeper "
        "    --config=/etc/clickhouse-keeper/keeper_config.xml "
        "    --log-file=/var/log/clickhouse-keeper/clickhouse-keeper.log "
        "    --errorlog-file=/var/log/clickhouse-keeper/clickhouse-keeper.err.log; "
        "else "
        "  exec clickhouse keeper "
        "    --config=/etc/clickhouse-keeper/keeper_config.xml "
        "    --log-file=/var/log/clickhouse-keeper/clickhouse-keeper.log "
        "    --errorlog-file=/var/log/clickhouse-keeper/clickhouse-keeper.err.log; "
        "fi"
        "' "
        "> /var/log/clickhouse-keeper/clickhouse-keeper.stdout.log 2>&1 "
        "& echo $!",
    ]
    pid_str = node.exec_in_container(cmd, user="root").strip()
    return int(pid_str)


def _stop_process(node, pid):
    node.exec_in_container(
        ["bash", "-c", "kill {pid} >/dev/null 2>&1 || true".format(pid=pid)],
        user="root",
        nothrow=True,
    )


def _is_process_alive(node, pid):
    return (
        node.exec_in_container(
            [
                "bash",
                "-c",
                "kill -0 {pid} 2>/dev/null && echo yes || echo no".format(pid=pid),
            ],
            user="root",
            nothrow=True,
        ).strip()
        == "yes"
    )


def _read_keeper_logs(node):
    return node.exec_in_container(
        [
            "bash",
            "-c",
            "set -euo pipefail;"
            "for f in "
            "  /var/log/clickhouse-keeper/clickhouse-keeper.stdout.log "
            "  /var/log/clickhouse-keeper/clickhouse-keeper.err.log "
            "  /var/log/clickhouse-keeper/clickhouse-keeper.log; do "
            "  echo \"===== ${f} =====\"; "
            "  if [ -f \"${f}\" ]; then sed -n '1,200p' \"${f}\"; else echo \"<missing>\"; fi; "
            "done",
        ],
        user="root",
        nothrow=True,
    )


def _wait_keeper_ready(node, pid, timeout=60.0):
    start_time = time.time()
    last_exc = None

    while time.time() - start_time < timeout:
        if not _is_process_alive(node, pid):
            break

        try:
            mntr = keeper_utils.send_4lw_cmd(
                cluster, node, "mntr", port=9181, timeout_sec=2
            )
            if mntr != keeper_utils.NOT_SERVING_REQUESTS_ERROR_MSG:
                return
        except Exception as exc:
            last_exc = exc

        time.sleep(0.5)

    logs = _read_keeper_logs(node)
    extra = ""
    if last_exc is not None:
        extra = "\nLast exception: {exc}".format(exc=repr(last_exc))
    raise AssertionError(
        "Keeper did not become ready within {timeout}s (pid={pid}).{extra}\n{logs}".format(
            timeout=timeout, pid=pid, extra=extra, logs=logs
        )
    )


def _get_https_command_response(node, secure_port=9183):
    session = requests.Session()
    session.trust_env = False
    return session.get(
        "https://{host}:{port}/api/v1/commands?command=conf".format(
            host=node.ip_address, port=secure_port
        ),
        verify=False,
        timeout=5,
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start(connection_timeout=180.0)
        yield cluster
    finally:
        cluster.shutdown()


def test_standalone_keeper_cluster_https_control_secure_port(started_cluster):
    nodes = [node1, node2, node3]
    pids = []

    for idx, node in enumerate(nodes, start=1):
        _install_keeper_configs(node, idx)
        pids.append(_start_standalone_keeper(node))

    try:
        for node, pid in zip(nodes, pids):
            _wait_keeper_ready(node, pid, timeout=60.0)

        leader = keeper_utils.get_leader(cluster, nodes)
        response = _get_https_command_response(leader, secure_port=9183)
        assert response.status_code == 200
        command_data = response.json()
        assert command_data["result"] == keeper_utils.send_4lw_cmd(
            cluster, leader, "conf", port=9181
        )

        follower = keeper_utils.get_any_follower(cluster, nodes)
        response = _get_https_command_response(follower, secure_port=9183)
        assert response.status_code == 200
        command_data = response.json()
        assert command_data["result"] == keeper_utils.send_4lw_cmd(
            cluster, follower, "conf", port=9181
        )
    finally:
        for node, pid in zip(nodes, pids):
            _stop_process(node, pid)
