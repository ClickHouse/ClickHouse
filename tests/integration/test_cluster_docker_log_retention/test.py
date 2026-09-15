"""A cluster rebuilt inside one pytest session must not destroy the previous
incarnation's docker.log, which is the only record of its container exit codes.

Two rebuild shapes delete instances_dir: a second start() on one object, which is what
--dist=each does to a module-scoped fixture, and a fresh ClickHouseCluster for the same
base path, which is what a package-scoped fixture does when it is re-set-up.
"""

import os
import time
import uuid

from helpers.cluster import ClickHouseCluster, get_instances_dir

DOCKER_LOG_TIMEOUT = 30


def read_docker_log_when_nonempty(path, timeout=DOCKER_LOG_TIMEOUT):
    # `docker compose logs --follow` fills the file asynchronously after start().
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            with open(path, "rb") as f:
                return f.read()
        time.sleep(0.5)
    return b""


def capture_docker_log(cluster, incarnation):
    captured = read_docker_log_when_nonempty(cluster.docker_logs_path)
    assert captured, (
        f"{cluster.docker_logs_path} stayed empty for {DOCKER_LOG_TIMEOUT}s after "
        f"{incarnation}, so the retention assertions below would be vacuous"
    )
    return captured


def read_retained(retention_dir, name):
    with open(os.path.join(retention_dir, name), "rb") as f:
        return f.read()


def test_docker_log_retained_across_cluster_rebuild():
    # One cluster name per invocation, so that a repeated run (pytest --count N) does not
    # inherit the previous repeat's retention directory.
    name = "retention_" + uuid.uuid4().hex[:8]
    instances_dir_name = get_instances_dir(name)
    worker = os.getenv("PYTEST_XDIST_WORKER")
    if worker:
        instances_dir_name += f"-{worker}"
    base_dir = os.path.dirname(os.path.abspath(__file__))
    # Derived rather than read off the cluster, because the retention directory has to
    # keep the `_instances` prefix that the CI log-collection glob matches.
    retention_dir = os.path.join(base_dir, instances_dir_name + "-prev-logs")

    # State an earlier process would have left behind. Ordinal 9 so that renaming it,
    # rather than discarding it, cannot satisfy the ordinal assertions below.
    os.makedirs(retention_dir)
    with open(os.path.join(retention_dir, "docker.9.log"), "w") as f:
        f.write("stale log from an earlier process\n")

    started = []
    try:
        first_cluster = ClickHouseCluster(__file__, name=name)
        assert first_cluster.instances_dir == os.path.join(base_dir, instances_dir_name)
        # The first construction in a process discards a retention directory left behind
        # by an earlier one, so a stale log is never filed as this session's incarnation.
        assert not os.path.exists(retention_dir)

        first_cluster.add_instance("node1")
        started.append(first_cluster)
        first_cluster.start()
        first_cluster.shutdown()
        started.remove(first_cluster)
        log_a = capture_docker_log(first_cluster, "the first start()")

        # A second start() on the same object finds instances_dir in place, so it takes
        # start()'s existing-directory branch rather than the constructor's.
        started.append(first_cluster)
        first_cluster.start()
        assert read_retained(retention_dir, "docker.1.log").startswith(log_a)
        assert sorted(os.listdir(retention_dir)) == ["docker.1.log"]

        first_cluster.shutdown()
        started.remove(first_cluster)
        log_b = capture_docker_log(first_cluster, "the second start()")

        second_cluster = ClickHouseCluster(__file__, name=name)
        assert sorted(os.listdir(retention_dir)) == ["docker.1.log", "docker.2.log"]
        assert read_retained(retention_dir, "docker.2.log").startswith(log_b)
        # Still the first incarnation's bytes: an ordinal is never reused.
        assert read_retained(retention_dir, "docker.1.log").startswith(log_a)
        assert not os.path.exists(second_cluster.instances_dir)

        # A different instance name, so that the retained logs name the incarnation they
        # came from: two same-named incarnations are byte-identical for ~950 bytes.
        second_cluster.add_instance("node2")
        started.append(second_cluster)
        second_cluster.start()
        capture_docker_log(second_cluster, "node2's start()")
        second_cluster.shutdown()
        started.remove(second_cluster)

        ClickHouseCluster(__file__, name=name)  # files the third incarnation
        assert sorted(os.listdir(retention_dir)) == [
            "docker.1.log",
            "docker.2.log",
            "docker.3.log",
        ]
        assert b"node2-1" in read_retained(retention_dir, "docker.3.log")
    finally:
        for cluster in started:
            cluster.shutdown(ignore_fatal=True)
