import os

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
# The geo files are missing at startup, so the embedded dictionaries keep retrying with a growing delay.
instance = cluster.add_instance("instance", main_configs=["configs/geo.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_start_reload_loads_embedded_dictionaries_skipped_while_stopped(started_cluster):
    instance.query("SYSTEM STOP RELOAD DICTIONARIES")
    instance.exec_in_container(["mkdir", "-p", "/etc/clickhouse-server/geo"])
    for name in ["regions_hierarchy.txt", "regions_names_en.txt"]:
        instance.copy_file_to_container(
            os.path.join(SCRIPT_DIR, "geo", name), f"/etc/clickhouse-server/geo/{name}"
        )

    # Several retries are skipped while reloads are stopped.
    instance.wait_for_log_line(
        "Loading of embedded dictionaries is stopped", timeout=60, repetitions=2
    )
    assert "Embedded dictionaries were not loaded" in instance.query_and_get_error(
        "SELECT regionToCountry(toUInt32(3))"
    )

    # START loads them at once instead of at the next retry, which can be up to
    # `builtin_dictionaries_reload_interval` away.
    instance.query("SYSTEM START RELOAD DICTIONARIES")
    assert instance.query("SELECT regionToCountry(toUInt32(3))") == "2\n"
