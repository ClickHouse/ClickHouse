import logging
import time
import pathlib

import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster


remote_disk_path = (
    pathlib.Path(__file__).parent / "_instances" / "server" / "database" / "remote_disk"
)


def files_number_in_disk():
    return len(
        [
            p.relative_to(remote_disk_path)
            for p in remote_disk_path.rglob("*")
            if p.is_file()
        ]
    )


def wait_for_client(client):
    for i in range(11):
        try:
            assert client.query("SELECT 1") == "1\n"
        except QueryRuntimeException as e:
            if i == 10:
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
            "client", main_configs=["configs/config.d/client_storage_conf.xml"]
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        wait_for_client(cluster.instances["client"])

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
    node = cluster.instances["client"]

    node.query(
        "CREATE TABLE remote_disk_test (id UInt64) ENGINE={} SETTINGS disk = 'remote_disk'".format(
            log_engine
        )
    )

    try:
        node.query("INSERT INTO remote_disk_test SELECT number FROM numbers(5)")
        assert node.query("SELECT * FROM remote_disk_test") == "0\n1\n2\n3\n4\n"
        assert files_number_in_disk() == files_overhead

        node.query("INSERT INTO remote_disk_test SELECT number + 5 FROM numbers(3)")
        assert (
            node.query("SELECT * FROM remote_disk_test order by id")
            == "0\n1\n2\n3\n4\n5\n6\n7\n"
        )
        assert files_number_in_disk() == files_overhead

        node.query("INSERT INTO remote_disk_test SELECT number + 8 FROM numbers(1)")
        assert (
            node.query("SELECT * FROM remote_disk_test order by id")
            == "0\n1\n2\n3\n4\n5\n6\n7\n8\n"
        )
        assert files_number_in_disk() == files_overhead

        node.query("TRUNCATE TABLE remote_disk_test")
        assert files_number_in_disk() == 0
    finally:
        node.query("DROP TABLE remote_disk_test NO DELAY")
