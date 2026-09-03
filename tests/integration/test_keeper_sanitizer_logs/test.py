import os

import pytest

from helpers.cluster import ClickHouseCluster, SANITIZER_SIGN

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True, stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        # The test injects a sanitizer marker into a Keeper's stderr, so the
        # normal teardown would re-raise; ignore it here.
        cluster.shutdown(ignore_sanitizer=True)


def keeper_stderr_path(i):
    return os.path.join(cluster.keeper_instance_dir_prefix + f"{i}", "log", "stderr.log")


def test_keeper_stderr_is_collected():
    # The Keeper entrypoint redirects stderr to a host-mounted file, so each
    # Keeper instance must have a stderr.log next to its other logs. This is
    # where a sanitizer report (written to raw fd 2) ends up, so it is archived
    # with the rest of the integration-test logs.
    for i in range(1, 4):
        path = keeper_stderr_path(i)
        assert os.path.exists(path), f"Keeper stderr log not collected: {path}"


def test_keeper_sanitizer_report_is_detected():
    # Write the sanitizer marker to zoo2's real stderr (fd 2). It must reach the
    # host-mounted stderr.log through the entrypoint redirect, and shutdown() must
    # detect it and fail for that instance. Without the redirect + scan the report
    # would only reach the container stream and go unnoticed.
    marker = SANITIZER_SIGN + " WARNING: ThreadSanitizer: data race (injected by test)"
    cluster.exec_in_container(
        cluster.get_container_id("zoo2"),
        ["bash", "-c", f"echo '{marker}' > /proc/1/fd/2"],
        user="root",
    )

    with open(keeper_stderr_path(2)) as f:
        assert SANITIZER_SIGN in f.read(), "marker did not reach the host-mounted stderr.log"

    with pytest.raises(Exception, match="Sanitizer assert found for instance zoo2"):
        cluster.shutdown()
