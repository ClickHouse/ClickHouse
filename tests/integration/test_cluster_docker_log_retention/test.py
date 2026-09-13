"""A cluster rebuilt inside one pytest session must not destroy the previous
incarnation's docker.log, which is the only record of its container exit codes.

The failing shape is a fresh ClickHouseCluster constructed for the same base path,
which is what a package-scoped fixture does when it is torn down and re-set-up.
"""

import os
import time

from helpers.cluster import ClickHouseCluster

DOCKER_LOG_TIMEOUT = 30


def read_docker_log_when_nonempty(path, timeout=DOCKER_LOG_TIMEOUT):
    # `docker compose logs --follow` fills the file asynchronously after start().
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            with open(path, "r", errors="replace") as f:
                return f.read()
        time.sleep(0.5)
    return ""


def test_docker_log_retained_across_cluster_rebuild():
    started = []
    try:
        first_cluster = ClickHouseCluster(__file__)
        # Derived rather than read off the cluster, because the retention directory has
        # to keep the `_instances` prefix that the CI log-collection glob matches.
        retention_dir = first_cluster.instances_dir + "-prev-logs"
        preserved = os.path.join(retention_dir, "docker.1.log")
        # The first construction in a process discards a retention directory left behind
        # by an earlier one, so a stale log is never filed as this session's incarnation.
        assert not os.path.exists(retention_dir)

        first_cluster.add_instance("node1")
        started.append(first_cluster)
        first_cluster.start()

        first_log = read_docker_log_when_nonempty(first_cluster.docker_logs_path)
        assert first_log, (
            f"{first_cluster.docker_logs_path} stayed empty for "
            f"{DOCKER_LOG_TIMEOUT}s, so the retention assertions below would be vacuous"
        )

        first_cluster.shutdown()
        started.remove(first_cluster)

        second_cluster = ClickHouseCluster(__file__)
        assert os.path.exists(preserved)
        with open(preserved, "r", errors="replace") as f:
            assert f.read().startswith(first_log[:200])
        assert sorted(os.listdir(retention_dir)) == ["docker.1.log"]
        assert not os.path.exists(second_cluster.instances_dir)

        # A different instance name, so that every retained log names the incarnation it
        # came from: the first 200 bytes of two same-named incarnations are identical.
        second_cluster.add_instance("node2")
        started.append(second_cluster)
        second_cluster.start()
        assert read_docker_log_when_nonempty(second_cluster.docker_logs_path)
        second_cluster.shutdown()
        started.remove(second_cluster)

        ClickHouseCluster(__file__)  # a third construction files the second incarnation
        assert sorted(os.listdir(retention_dir)) == ["docker.1.log", "docker.2.log"]
        with open(preserved, "r", errors="replace") as f:
            assert f.read().startswith(first_log[:200])
        with open(
            os.path.join(retention_dir, "docker.2.log"), "r", errors="replace"
        ) as f:
            assert "node2-1" in f.read()
    finally:
        for cluster in started:
            cluster.shutdown(ignore_fatal=True)
