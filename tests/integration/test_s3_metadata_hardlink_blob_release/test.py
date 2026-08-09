import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=["configs/config.d/storage_conf.xml"],
            stay_alive=True,
            with_minio=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        yield cluster
    finally:
        cluster.shutdown()


def test_blobs_of_hardlinked_part_survive_crash_during_removal(cluster):
    """FREEZE hardlinks a part's files into shadow/, so both names share one metadata file and
    therefore one reference count. If the server is killed while UNFREEZE is removing the frozen
    copy, the persisted counts are already decremented but no link is gone. Removing the frozen
    copy afterwards must not delete blobs the live part still points to.
    """
    node = cluster.instances["node"]
    table = "t_hardlink_blob_release"

    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (a UInt64, d UInt64)
        ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 's3'
        """
    )
    node.query(f"INSERT INTO {table} SELECT number, number FROM numbers(10000)")

    expected = node.query(f"SELECT count(), sum(d) FROM {table}").strip()

    node.query(f"ALTER TABLE {table} FREEZE WITH NAME 'backup1'")

    # Park the removal of the frozen copy after its traversal has persisted the reference-count
    # decrements but before anything is unlinked, then kill the server in that window.
    node.query("SYSTEM ENABLE FAILPOINT remove_recursive_operation_pause_after_traverse")
    unfreeze = node.get_query_request(f"ALTER TABLE {table} UNFREEZE WITH NAME 'backup1'")
    node.query(
        "SYSTEM WAIT FAILPOINT remove_recursive_operation_pause_after_traverse PAUSE",
        timeout=180,
    )
    # Kill only the server process: the harness helper pkills every "clickhouse" match,
    # which also hits the container's wrapper shell and takes the container down with it.
    server_pid = node.get_process_pid("clickhouse server")
    assert server_pid is not None
    node.exec_in_container(["bash", "-c", f"kill -9 {server_pid}"], user="root")

    # start_clickhouse() waits for any process matching "clickhouse" to be gone, so the client
    # that issued the parked UNFREEZE has to be reaped too.
    unfreeze.get_answer_and_error()
    for _ in range(60):
        if node.get_process_pid("clickhouse") is None:
            break
        time.sleep(1)
    else:
        raise Exception("clickhouse processes did not exit after SIGKILL")

    node.start_clickhouse()

    # Finish removing the frozen copy, now with reference counts that are one too low.
    node.query(f"ALTER TABLE {table} UNFREEZE WITH NAME 'backup1'")

    # Reading the part proves its blobs were not deleted with the frozen copy.
    assert node.query(f"SELECT count(), sum(d) FROM {table}").strip() == expected

    node.query(f"DROP TABLE {table} SYNC")
