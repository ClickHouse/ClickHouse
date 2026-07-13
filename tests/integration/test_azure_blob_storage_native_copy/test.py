#!/usr/bin/env python3

import os
import time

import pytest

from helpers.cluster import ClickHouseCluster


def generate_config(port):
    # Per-worker suffix so parallel xdist workers don't race on the generated file.
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "")
    suffix = f"_{worker_id}" if worker_id else ""
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        f"./_gen/storage_conf{suffix}.xml",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        TEMPLATE = """
        <clickhouse>
            <storage_configuration>
                <disks>
                    <disk_azure>
                        <metadata_type>local</metadata_type>
                        <type>object_storage</type>
                        <object_storage_type>azure_blob_storage</object_storage_type>
                        <connection_string>DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:{port}/devstoreaccount1;</connection_string>
                        <container_name>cont</container_name>
                        <skip_access_check>false</skip_access_check>
                        <use_native_copy>true</use_native_copy>
                    </disk_azure>
                    <disk_azure_other_bucket>
                        <metadata_type>local</metadata_type>
                        <type>object_storage</type>
                        <object_storage_type>azure_blob_storage</object_storage_type>
                        <use_native_copy>true</use_native_copy>
                        <connection_string>DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:{port}/devstoreaccount1;</connection_string>
                        <container_name>othercontainer</container_name>
                        <skip_access_check>false</skip_access_check>
                    </disk_azure_other_bucket>
                    <disk_azure_cache>
                        <type>cache</type>
                        <disk>disk_azure</disk>
                        <path>/tmp/azure_cache/</path>
                        <max_size>1000000000</max_size>
                        <cache_on_write_operations>1</cache_on_write_operations>
                    </disk_azure_cache>
                    <disk_azure_small_native_copy>
                        <metadata_type>local</metadata_type>
                        <type>object_storage</type>
                        <object_storage_type>azure_blob_storage</object_storage_type>
                        <connection_string>DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:{port}/devstoreaccount1;</connection_string>
                        <container_name>cont</container_name>
                        <skip_access_check>false</skip_access_check>
                        <use_native_copy>true</use_native_copy>
                        <max_single_part_copy_size>4</max_single_part_copy_size>
                    </disk_azure_small_native_copy>
                </disks>
                <policies>
                    <policy_azure>
                        <volumes>
                            <main>
                                <disk>disk_azure</disk>
                            </main>
                        </volumes>
                    </policy_azure>
                    <policy_azure_other_bucket>
                        <volumes>
                            <main>
                                <disk>disk_azure_other_bucket</disk>
                            </main>
                        </volumes>
                    </policy_azure_other_bucket>
                    <policy_azure_cache>
                        <volumes>
                            <main>
                                <disk>disk_azure_cache</disk>
                            </main>
                        </volumes>
                    </policy_azure_cache>
                </policies>
            </storage_configuration>
            <backups>
                <allowed_disk>disk_azure</allowed_disk>
                <allowed_disk>disk_azure_cache</allowed_disk>
                <allowed_disk>disk_azure_other_bucket</allowed_disk>
            </backups>
        </clickhouse>
        """
        f.write(TEMPLATE.format(port=port))
    return path


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        port = cluster.azurite_port
        path = generate_config(port)
        cluster.add_instance(
            "node1",
            main_configs=[path],
            with_azurite=True,
            # Breaks assertion for "using native copy" (because it will happen for database metadata not the Azure tests)
            with_remote_database_disk=False,
        )
        cluster.add_instance(
            "node2",
            main_configs=[path],
            with_azurite=True,
            # Breaks assertion for "using native copy" (because it will happen for database metadata not the Azure tests)
            with_remote_database_disk=False,
        )
        cluster.add_instance(
            "node3",
            main_configs=[path],
            with_azurite=True,
            # Breaks assertion for "using native copy" (because it will happen for database metadata not the Azure tests)
            with_remote_database_disk=False,
        )
        cluster.add_instance(
            "node4",
            main_configs=[path],
            with_azurite=True,
            # Breaks assertion for "using native copy" (because it will happen for database metadata not the Azure tests)
            with_remote_database_disk=False,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def azure_query(
    node, query, expect_error=False, try_num=10, settings={}, query_on_retry=None
):
    for i in range(try_num):
        try:
            if expect_error:
                return node.query_and_get_error(query, settings=settings)
            else:
                return node.query(query, settings=settings)
        except Exception as ex:
            retriable_errors = [
                "DB::Exception: Azure::Core::Http::TransportException: Connection was closed by the server while trying to read a response",
                "DB::Exception: Azure::Core::Http::TransportException: Connection closed before getting full response or response is less than expected",
                "DB::Exception: Azure::Core::Http::TransportException: Connection was closed by the server while trying to read a response",
                "DB::Exception: Azure::Core::Http::TransportException: Error while polling for socket ready read",
                "Azure::Core::Http::TransportException, e.what() = Connection was closed by the server while trying to read a response",
                "Azure::Core::Http::TransportException, e.what() = Connection closed before getting full response or response is less than expected",
                "Azure::Core::Http::TransportException, e.what() = Connection was closed by the server while trying to read a response",
                "Azure::Core::Http::TransportException, e.what() = Error while polling for socket ready read",
            ]
            retry = False
            for error in retriable_errors:
                if error in str(ex):
                    retry = True
                    print(f"Try num: {i}. Having retriable error: {ex}")
                    time.sleep(i)
                    break
            if not retry or i == try_num - 1:
                raise Exception(ex)
            if query_on_retry is not None:
                node.query(query_on_retry)
            continue


def test_backup_restore_on_merge_tree_same_container(cluster):
    node1 = cluster.instances["node1"]
    azure_query(node1, "DROP TABLE IF EXISTS test_simple_merge_tree SYNC")
    azure_query(
        node1,
        "DROP TABLE IF EXISTS test_simple_merge_tree",
    )
    azure_query(
        node1,
        "CREATE TABLE test_simple_merge_tree(key UInt64, data String) Engine = MergeTree() ORDER BY tuple() SETTINGS storage_policy='policy_azure_cache'",
    )
    azure_query(node1, "INSERT INTO test_simple_merge_tree VALUES (1, 'a')")

    cont = 'cont'+str(time.time_ns())
    backup_destination = f"AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', '{cont}', 'test_simple_merge_tree_backup')"
    print("BACKUP DEST", backup_destination)
    azure_query(
        node1,
        f"BACKUP TABLE test_simple_merge_tree TO {backup_destination}",
    )

    assert node1.contains_in_log("using native copy")

    azure_query(
        node1,
        f"RESTORE TABLE test_simple_merge_tree AS test_simple_merge_tree_restored FROM {backup_destination} SETTINGS allow_azure_native_copy = 1;",
    )
    assert (
        azure_query(node1, "SELECT * from test_simple_merge_tree_restored") == "1\ta\n"
    )

    assert node1.contains_in_log("using native copy")

    azure_query(node1, "DROP TABLE test_simple_merge_tree")
    azure_query(node1, "DROP TABLE test_simple_merge_tree_restored")


def test_backup_restore_on_merge_tree_different_container(cluster):
    node2 = cluster.instances["node2"]
    azure_query(node2, "DROP TABLE IF EXISTS test_simple_merge_tree_different_bucket SYNC")
    azure_query(
        node2,
        "DROP TABLE IF EXISTS test_simple_merge_tree_different_bucket",
    )
    azure_query(
        node2,
        "CREATE TABLE test_simple_merge_tree_different_bucket(key UInt64, data String) Engine = MergeTree() ORDER BY tuple() SETTINGS storage_policy='policy_azure_other_bucket'",
    )
    azure_query(
        node2, "INSERT INTO test_simple_merge_tree_different_bucket VALUES (1, 'a')"
    )

    cont = 'cont'+str(time.time_ns())
    backup_destination = f"AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', '{cont}', 'test_simple_merge_tree_different_bucket_backup_different_bucket')"
    print("BACKUP DEST", backup_destination)
    azure_query(
        node2,
        f"BACKUP TABLE test_simple_merge_tree_different_bucket TO {backup_destination}",
    )

    assert node2.contains_in_log("using native copy")

    azure_query(
        node2,
        f"RESTORE TABLE test_simple_merge_tree_different_bucket AS test_simple_merge_tree_different_bucket_restored FROM {backup_destination};",
    )
    assert (
        azure_query(
            node2, "SELECT * from test_simple_merge_tree_different_bucket_restored"
        )
        == "1\ta\n"
    )

    assert node2.contains_in_log("using native copy")

    azure_query(node2, "DROP TABLE test_simple_merge_tree_different_bucket")
    azure_query(node2, "DROP TABLE test_simple_merge_tree_different_bucket_restored")


def test_backup_restore_on_merge_tree_native_copy_async(cluster):
    node3 = cluster.instances["node3"]
    azure_query(node3, "DROP TABLE IF EXISTS test_simple_merge_tree_async SYNC")
    azure_query(
        node3,
        "DROP TABLE IF EXISTS test_simple_merge_tree_async",
    )
    azure_query(
        node3,
        "CREATE TABLE test_simple_merge_tree_async(key UInt64, data String) Engine = MergeTree() ORDER BY tuple() SETTINGS storage_policy='policy_azure_cache'",
    )
    azure_query(node3, "INSERT INTO test_simple_merge_tree_async VALUES (1, 'a')")

    cont = 'cont'+str(time.time_ns())
    backup_destination = f"AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', '{cont}', 'test_simple_merge_tree_async_backup')"
    print("BACKUP DEST", backup_destination)
    azure_query(
        node3,
        f"BACKUP TABLE test_simple_merge_tree_async TO {backup_destination}",
        settings={"azure_max_single_part_copy_size": 0},
    )

    assert node3.contains_in_log("using native copy")

    azure_query(
        node3,
        f"RESTORE TABLE test_simple_merge_tree_async AS test_simple_merge_tree_async_restored FROM {backup_destination};",
        settings={"azure_max_single_part_copy_size": 0},
    )
    assert (
        azure_query(node3, "SELECT * from test_simple_merge_tree_async_restored")
        == "1\ta\n"
    )

    assert node3.contains_in_log("using native copy")

    azure_query(node3, "DROP TABLE test_simple_merge_tree_async")
    azure_query(node3, "DROP TABLE test_simple_merge_tree_async_restored")


def test_backup_restore_native_copy_disabled_in_query(cluster):
    node4 = cluster.instances["node4"]
    azure_query(node4, "DROP TABLE IF EXISTS test_simple_merge_tree_native_copy_disabled_in_query SYNC")
    azure_query(
        node4,
        "DROP TABLE IF EXISTS test_simple_merge_tree_native_copy_disabled_in_query",
    )
    azure_query(
        node4,
        "CREATE TABLE test_simple_merge_tree_native_copy_disabled_in_query(key UInt64, data String) Engine = MergeTree() ORDER BY tuple() SETTINGS storage_policy='policy_azure'",
    )
    azure_query(
        node4,
        "INSERT INTO test_simple_merge_tree_native_copy_disabled_in_query VALUES (1, 'a')",
    )

    cont = 'cont'+str(time.time_ns())
    backup_destination = f"AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', '{cont}', 'test_simple_merge_tree_native_copy_disabled_in_query_backup')"
    print("BACKUP DEST", backup_destination)
    azure_query(
        node4,
        f"BACKUP TABLE test_simple_merge_tree_native_copy_disabled_in_query TO {backup_destination} SETTINGS allow_azure_native_copy = 0",
    )

    assert not node4.contains_in_log("using native copy")


@pytest.mark.parametrize("inflight", [1, 2])
def test_backup_restore_read_write_multipart_inflight_limit(cluster, inflight):
    # Force the read-then-write (non-native) copy path through a multipart upload
    # bounded by a small `azure_max_inflight_parts_for_one_file`, and verify the
    # restored contents are byte-for-byte identical. This guards the `TaskTracker`
    # path that limits the number of concurrently staged parts: a block-order
    # regression or a broken throttled path would corrupt the restored file.
    node1 = cluster.instances["node1"]
    table = f"test_read_write_multipart_{inflight}"
    restored = f"{table}_restored"
    azure_query(node1, f"DROP TABLE IF EXISTS {table} SYNC")
    azure_query(node1, f"DROP TABLE IF EXISTS {restored} SYNC")
    azure_query(
        node1,
        f"""
        CREATE TABLE {table} (key UInt64, data String CODEC(NONE))
        ENGINE = MergeTree() ORDER BY key
        SETTINGS storage_policy='policy_azure', min_bytes_for_wide_part=0
        """,
    )
    # ~300 KiB of incompressible, position-sensitive data stored in a single wide
    # part, so `data.bin` is large enough to be split into many parts and any
    # out-of-order block commit produces detectably different contents.
    azure_query(
        node1,
        f"INSERT INTO {table} SELECT number, randomPrintableASCII(1024) FROM numbers(300)",
    )

    expected = azure_query(
        node1, f"SELECT count(), sum(cityHash64(key, data)) FROM {table}"
    )

    copy_settings = {
        # Force multipart upload for every non-empty file.
        "azure_max_single_part_upload_size": 1,
        # Small parts so the ~300 KiB `data.bin` is split into many blocks.
        "azure_min_upload_part_size": 16 * 1024,
        # The throttle under test: at most `inflight` parts staged concurrently.
        "azure_max_inflight_parts_for_one_file": inflight,
    }

    cont = "cont" + str(time.time_ns())
    backup_destination = f"AzureBlobStorage('{cluster.env_variables['AZURITE_CONNECTION_STRING']}', '{cont}', '{table}_backup')"

    # `allow_azure_native_copy = 0` disables server-side copy, so the data is read
    # back and re-uploaded via the multipart path on both backup and restore.
    azure_query(
        node1,
        f"BACKUP TABLE {table} TO {backup_destination} SETTINGS allow_azure_native_copy = 0",
        settings=copy_settings,
    )

    azure_query(
        node1,
        f"RESTORE TABLE {table} AS {restored} FROM {backup_destination} SETTINGS allow_azure_native_copy = 0",
        settings=copy_settings,
    )

    assert node1.contains_in_log(f"Reading and writing Blob.*from Container: {cont}")

    assert (
        azure_query(
            node1, f"SELECT count(), sum(cityHash64(key, data)) FROM {restored}"
        )
        == expected
    )

    azure_query(node1, f"DROP TABLE {table} SYNC")
    azure_query(node1, f"DROP TABLE {restored} SYNC")


def test_clickhouse_disks_azure(cluster):
    node4 = cluster.instances["node4"]
    disk = "disk_azure_small_native_copy"
    node4.exec_in_container(
        [
            "bash",
            "-c",
            f"echo 'meow' | /usr/bin/clickhouse disks --disk {disk} --query 'write im_a_file.txt'",
        ]
    )
    out = node4.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--disk",
            disk,
            "--query",
            "read im_a_file.txt",
        ]
    )
    assert out == "meow\n\n"
    node4.exec_in_container(
        [
            "bash",
            "-c",
            f"/usr/bin/clickhouse disks --disk {disk} --log-level trace --query 'copy im_a_file.txt another_file.txt'",
        ]
    )
    out = node4.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--disk",
            disk,
            "--query",
            "read another_file.txt",
        ]
    )
    assert out == "meow\n\n"
