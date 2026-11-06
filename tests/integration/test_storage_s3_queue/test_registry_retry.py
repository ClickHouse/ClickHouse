import uuid

import pytest
from kazoo.exceptions import NoNodeError

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    create_table,
    generate_random_files,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
            ],
            stay_alive=True,
        )

        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_registered_after_retry_is_not_a_fresh_create(started_cluster):
    """A keeper retry while reading the registry must not look like a fresh create.

    `registerNonActive` reports `created_new_metadata` back to `startup()`, which
    passes it as `is_drop` to `ObjectStorageQueueMetadataFactory::remove()` when
    startup fails.  With `is_drop=true` and nothing left in the registry, that
    calls `removeRecursive()` on the whole keeper path, taking processed-file
    state with it.

    `DETACH` and a normal shutdown call `shutdown(is_drop=false)`.  The factory
    unregisters a table only on drop, so the registry entry is still there
    afterwards.  A retry that never submitted a create of `/registry` must report
    `created_new_metadata=false` for that entry.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_register_retry_{uuid.uuid4().hex[:8]}"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    registry_path = f"{keeper_path}/registry"

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            # Commit on SELECT so the file below is durably recorded as processed
            # in Keeper; that committed state is what must survive.
            "commit_on_select": 1,
        },
    )

    generate_random_files(started_cluster, files_path, 1, start_ind=0, row_num=10)
    assert 10 == int(node.query(f"SELECT count() FROM {table_name}"))

    zk = started_cluster.get_kazoo_client("zoo1")

    registry_str = zk.get(registry_path)[0]
    assert registry_str, "table should be registered after creation"

    # DETACH calls shutdown(is_drop=false). unregisterNonActive runs only for a
    # drop, so this entry stays. A normal server shutdown leaves the same state.
    node.query(f"DETACH TABLE {table_name}")
    assert zk.get(registry_path)[0] == registry_str

    # The ONCE failpoint throws before the registry read, so this attempt never
    # submits a create. A probability fault cannot pin the retry to that read.
    # Then fail startup, so the cleanup path has to decide whether to drop.
    node.query("SYSTEM ENABLE FAILPOINT object_storage_queue_fail_register_once")
    node.query("SYSTEM ENABLE FAILPOINT object_storage_queue_fail_startup")
    try:
        assert "Failed to startup" in node.query_and_get_error(
            f"ATTACH TABLE {table_name}"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT object_storage_queue_fail_startup")
        node.query("SYSTEM DISABLE FAILPOINT object_storage_queue_fail_register_once")

    # This table did not create the metadata, so the failed startup must not have
    # dropped it.  A retry that never sent the create used to be read as a fresh
    # create, and the whole path -- processed files included -- was removed here.
    try:
        assert zk.get(registry_path)[0] == registry_str
    except NoNodeError:
        pytest.fail("keeper metadata was dropped by a table that did not create it")

    # startup() runs after the table is in the catalog, so the failed ATTACH left
    # it attached.  Detach before attaching again, and the already consumed file
    # must not be processed a second time.
    assert (
        node.query(f"SELECT count() FROM system.tables WHERE name = '{table_name}'")
        == "1\n"
    )
    node.query(f"DETACH TABLE {table_name}")
    node.query(f"ATTACH TABLE {table_name}")
    assert 0 == int(node.query(f"SELECT count() FROM {table_name}"))
