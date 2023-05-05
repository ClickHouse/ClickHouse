import logging
import time
import pathlib

import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster


remote_disk_path = (
    pathlib.Path(__file__).parent / "_instances" / "server" / "database" / "remote_disk"
)


def files_number_in_disk(node):
    files = node.exec_in_container(
        ["find", "/var/lib/clickhouse/remote_disk", "-type", "f"]
    ).strip()
    if files == "":
        return 0
    return len(files.split("\n"))


def wait_for_node(node):
    for i in range(5):
        try:
            assert node.query("SELECT 1") == "1\n"
            return
        except QueryRuntimeException as e:
            if i == 4:
                raise
            if (
                e.returncode != 209 and e.returncode != 210
            ):  # Timeout or Connection reset by peer
                raise
        time.sleep(20)


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "server", main_configs=["configs/config.d/server_storage_conf.xml"]
        )
        cluster.add_instance(
            "client",
            main_configs=["configs/config.d/client_storage_conf.xml"],
            stay_alive=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        wait_for_node(cluster.instances["server"])

        # Should restart client because sometimes server starting later than client and client stops because can't connect to disk
        cluster.instances["client"].restart_clickhouse()
        wait_for_node(cluster.instances["client"])

        yield cluster
    finally:
        cluster.shutdown()


# TinyLog: files: id.bin, sizes.json
# INSERT overwrites 1 file (`sizes.json`) and appends 1 file (`id.bin`), so
# files_overhead=1, files_overhead_per_insert=1
#
# Log: files: id.bin, __marks.mrk, sizes.json
# INSERT overwrites 1 file (`sizes.json`), and appends 2 files (`id.bin`, `__marks.mrk`), so
# files_overhead=1, files_overhead_per_insert=2
#
# StripeLog: files: data.bin, index.mrk, sizes.json
# INSERT overwrites 1 file (`sizes.json`), and appends 2 files (`index.mrk`, `data.bin`), so
# files_overhead=1, files_overhead_per_insert=2
@pytest.mark.parametrize(
    "log_engine,files_overhead",
    [("TinyLog", 2), ("Log", 3), ("StripeLog", 3)],
)
def test_log_family_remote_disk(cluster, log_engine, files_overhead):
    client = cluster.instances["client"]
    server = cluster.instances["server"]

    client.query(
        "CREATE TABLE remote_disk_test (id UInt64) ENGINE={} SETTINGS disk = 'remote_disk'".format(
            log_engine
        )
    )

    try:
        client.query("INSERT INTO remote_disk_test SELECT number FROM numbers(5)")
        assert client.query("SELECT * FROM remote_disk_test") == "0\n1\n2\n3\n4\n"
        assert files_number_in_disk(server) == files_overhead

        client.query("INSERT INTO remote_disk_test SELECT number + 5 FROM numbers(3)")
        assert (
            client.query("SELECT * FROM remote_disk_test order by id")
            == "0\n1\n2\n3\n4\n5\n6\n7\n"
        )
        assert files_number_in_disk(server) == files_overhead

        client.query("INSERT INTO remote_disk_test SELECT number + 8 FROM numbers(1)")
        assert (
            client.query("SELECT * FROM remote_disk_test order by id")
            == "0\n1\n2\n3\n4\n5\n6\n7\n8\n"
        )
        assert files_number_in_disk(server) == files_overhead

        client.query("TRUNCATE TABLE remote_disk_test")
        assert files_number_in_disk(server) == 0
    finally:
        client.query("DROP TABLE remote_disk_test NO DELAY")
