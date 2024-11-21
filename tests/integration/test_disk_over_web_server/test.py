import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

uuids = []


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=["configs/storage_conf.xml", "configs/no_async_load.xml"],
            with_nginx=True,
            use_old_analyzer=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/storage_conf_web.xml", "configs/no_async_load.xml"],
            with_nginx=True,
            stay_alive=True,
            with_zookeeper=True,
            use_old_analyzer=True,
        )
        cluster.add_instance(
            "node3",
            main_configs=["configs/storage_conf_web.xml", "configs/no_async_load.xml"],
            with_nginx=True,
            with_zookeeper=True,
            use_old_analyzer=True,
        )

        cluster.add_instance(
            "node4",
            main_configs=["configs/storage_conf.xml", "configs/no_async_load.xml"],
            with_nginx=True,
            stay_alive=True,
            with_installed_binary=True,
            image="clickhouse/clickhouse-server",
            tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
        )
        cluster.add_instance(
            "node5",
            main_configs=["configs/storage_conf.xml", "configs/no_async_load.xml"],
            with_nginx=True,
            use_old_analyzer=True,
        )

        cluster.start()

        def create_table_and_upload_data(node, i):
            node.query(
                f"CREATE TABLE data{i} (id Int32) ENGINE = MergeTree() ORDER BY id SETTINGS storage_policy = 'def', min_bytes_for_wide_part=1;"
            )

            node.query("SYSTEM STOP MERGES")

            for _ in range(10):
                node.query(
                    f"INSERT INTO data{i} SELECT number FROM numbers(500000 * {i+1})"
                )
            node.query(f"SELECT * FROM data{i} ORDER BY id")

            metadata_path = node.query(
                f"SELECT data_paths FROM system.tables WHERE name='data{i}'"
            )
            metadata_path = metadata_path[
                metadata_path.find("/") : metadata_path.rfind("/") + 1
            ]
            print(f"Metadata: {metadata_path}")

            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "/usr/bin/clickhouse static-files-disk-uploader --test-mode --url http://nginx:80/test1 --metadata-path {}".format(
                        metadata_path
                    ),
                ],
                user="root",
            )
            parts = metadata_path.split("/")
            print(f"UUID: {parts[3]}")
            return parts[3]

        node1 = cluster.instances["node1"]

        global uuids
        for i in range(2):
            uuid = create_table_and_upload_data(node1, i)
            uuids.append(uuid)

        node4 = cluster.instances["node4"]

        uuid = create_table_and_upload_data(node4, 2)
        uuids.append(uuid)

        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize("node_name", ["node2"])
def test_usage(cluster, node_name):
    node1 = cluster.instances["node1"]
    node4 = cluster.instances["node4"]
    node2 = cluster.instances[node_name]
    global uuids
    assert len(uuids) == 3
    for i in range(3):
        node2.query(
            """
            CREATE TABLE test{} UUID '{}'
            (id Int32) ENGINE = MergeTree() ORDER BY id
            SETTINGS storage_policy = 'web';
        """.format(
                i, uuids[i]
            )
        )

        result = node2.query("SELECT * FROM test{} settings max_threads=20".format(i))

        result = node2.query("SELECT count() FROM test{}".format(i))
        assert int(result) == 5000000 * (i + 1)

        result = node2.query(
            "SELECT id FROM test{} WHERE id % 56 = 3 ORDER BY id".format(i)
        )
        node = node1
        if i == 2:
            node = node4

        assert result == node.query(
            "SELECT id FROM data{} WHERE id % 56 = 3 ORDER BY id".format(i)
        )

        result = node2.query(
            "SELECT id FROM test{} WHERE id > 789999 AND id < 999999 ORDER BY id".format(
                i
            )
        )
        assert result == node.query(
            "SELECT id FROM data{} WHERE id > 789999 AND id < 999999 ORDER BY id".format(
                i
            )
        )

        # to check right handling of paths in disk web
        node2.query("SELECT count() FROM system.remote_data_paths")

        node2.query("DROP TABLE test{} SYNC".format(i))
        print(f"Ok {i}")


def test_incorrect_usage(cluster):
    node2 = cluster.instances["node3"]
    global uuids
    node2.query(
        """
        CREATE TABLE test0 UUID '{}'
        (id Int32) ENGINE = MergeTree() ORDER BY id
        SETTINGS storage_policy = 'web';
    """.format(
            uuids[0]
        )
    )

    result = node2.query("SELECT count() FROM test0")
    assert int(result) == 5000000

    result = node2.query_and_get_error("ALTER TABLE test0 ADD COLUMN col1 Int32 first")
    assert "Table is read-only" in result

    result = node2.query_and_get_error("TRUNCATE TABLE test0")
    assert "Table is read-only" in result

    result = node2.query_and_get_error("OPTIMIZE TABLE test0 FINAL")
    assert "Table is in readonly mode due to static storage" in result

    node2.query("DROP TABLE test0 SYNC")


@pytest.mark.parametrize("node_name", ["node2"])
def test_cache(cluster, node_name):
    node1 = cluster.instances["node1"]
    node4 = cluster.instances["node4"]
    node2 = cluster.instances[node_name]
    global uuids
    assert len(uuids) == 3
    for i in range(3):
        node2.query(
            """
            CREATE TABLE test{} UUID '{}'
            (id Int32) ENGINE = MergeTree() ORDER BY id
            SETTINGS storage_policy = 'cached_web';
        """.format(
                i, uuids[i]
            )
        )

        result = node2.query(
            """
            SYSTEM DROP FILESYSTEM CACHE;
            SELECT count() FROM system.filesystem_cache;
        """
        )
        assert int(result) == 0

        result = node2.query("SELECT * FROM test{} settings max_threads=20".format(i))

        result = node2.query(
            """
            SELECT count() FROM system.filesystem_cache;
        """
        )
        assert int(result) > 0

        result = node2.query("SELECT count() FROM test{}".format(i))
        assert int(result) == 5000000 * (i + 1)

        result = node2.query(
            "SELECT id FROM test{} WHERE id % 56 = 3 ORDER BY id".format(i)
        )

        node = node1
        if i == 2:
            node = node4

        assert result == node.query(
            "SELECT id FROM data{} WHERE id % 56 = 3 ORDER BY id".format(i)
        )

        result = node2.query(
            "SELECT id FROM test{} WHERE id > 789999 AND id < 999999 ORDER BY id".format(
                i
            )
        )
        assert result == node.query(
            "SELECT id FROM data{} WHERE id > 789999 AND id < 999999 ORDER BY id".format(
                i
            )
        )

        node2.query("DROP TABLE test{} SYNC".format(i))
        print(f"Ok {i}")


def test_unavailable_server(cluster):
    """
    Regression test for the case when clickhouse-server simply ignore when
    server is unavailable on start and later will simply return 0 rows for
    SELECT from table on web disk.
    """
    node2 = cluster.instances["node2"]
    global uuids
    node2.query(
        """
        CREATE TABLE test0 UUID '{}'
        (id Int32) ENGINE = MergeTree() ORDER BY id
        SETTINGS storage_policy = 'web';
    """.format(
            uuids[0]
        )
    )
    node2.stop_clickhouse()
    try:
        # NOTE: you cannot use separate disk instead, since MergeTree engine will
        # try to lookup parts on all disks (to look unexpected disks with parts)
        # and fail because of unavailable server.
        node2.exec_in_container(
            [
                "bash",
                "-c",
                "sed -i 's#http://nginx:80/test1/#http://nginx:8080/test1/#' /etc/clickhouse-server/config.d/storage_conf_web.xml",
            ]
        )
        with pytest.raises(Exception):
            # HTTP retries with backup can take awhile
            node2.start_clickhouse(start_wait_sec=120, retry_start=False)
        assert node2.contains_in_log(
            "Caught exception while loading metadata.*Connection refused"
        )
        assert node2.contains_in_log(
            "Failed to make request to 'http://nginx:8080/test1/.*'. Error: 'Connection refused'. Failed at try 10/10."
        )
    finally:
        node2.exec_in_container(
            [
                "bash",
                "-c",
                "sed -i 's#http://nginx:8080/test1/#http://nginx:80/test1/#' /etc/clickhouse-server/config.d/storage_conf_web.xml",
            ]
        )
        node2.start_clickhouse()
        node2.query("DROP TABLE test0 SYNC")


def test_replicated_database(cluster):
    node1 = cluster.instances["node3"]
    node1.query(
        "CREATE DATABASE rdb ENGINE=Replicated('/test/rdb', 's1', 'r1')",
    )

    global uuids
    node1.query(
        """
        CREATE TABLE rdb.table0 UUID '{}'
        (id Int32) ENGINE = MergeTree() ORDER BY id
        SETTINGS storage_policy = 'web';
    """.format(
            uuids[0]
        ),
        settings={"database_replicated_allow_explicit_uuid": 3},
    )

    node2 = cluster.instances["node2"]
    node2.query(
        "CREATE DATABASE rdb ENGINE=Replicated('/test/rdb', 's1', 'r2')",
    )
    node2.query("SYSTEM SYNC DATABASE REPLICA rdb")

    assert node1.query("SELECT count() FROM rdb.table0") == "5000000\n"
    assert node2.query("SELECT count() FROM rdb.table0") == "5000000\n"

    node1.query("DROP DATABASE rdb SYNC")
    node2.query("DROP DATABASE rdb SYNC")


def test_page_cache(cluster):
    node = cluster.instances["node2"]
    global uuids
    assert len(uuids) == 3
    for i in range(3):
        node.query(
            """
            CREATE TABLE test{} UUID '{}'
            (id Int32) ENGINE = MergeTree() ORDER BY id
            SETTINGS storage_policy = 'web';
        """.format(
                i, uuids[i]
            )
        )

        result1 = node.query(
            f"SELECT sum(cityHash64(*)) FROM test{i} SETTINGS use_page_cache_for_disks_without_file_cache=1 -- test cold cache"
        )
        result2 = node.query(
            f"SELECT sum(cityHash64(*)) FROM test{i} SETTINGS use_page_cache_for_disks_without_file_cache=1 -- test warm cache"
        )
        result3 = node.query(
            f"SELECT sum(cityHash64(*)) FROM test{i} SETTINGS use_page_cache_for_disks_without_file_cache=0 -- test no cache"
        )

        assert result1 == result3
        assert result2 == result3

        node.query("SYSTEM FLUSH LOGS")

        def get_profile_events(query_name):
            text = node.query(
                f"SELECT ProfileEvents.Names, ProfileEvents.Values FROM system.query_log ARRAY JOIN ProfileEvents WHERE query LIKE '% -- {query_name}' AND type = 'QueryFinish'"
            )
            res = {}
            for line in text.split("\n"):
                if line == "":
                    continue
                name, value = line.split("\t")
                res[name] = int(value)
            return res

        ev1 = get_profile_events("test cold cache")
        assert ev1.get("PageCacheChunkMisses", 0) > 0
        assert (
            ev1.get("DiskConnectionsCreated", 0) + ev1.get("DiskConnectionsReused", 0)
            > 0
        )

        ev2 = get_profile_events("test warm cache")
        assert ev2.get("PageCacheChunkDataHits", 0) > 0
        assert ev2.get("PageCacheChunkMisses", 0) == 0
        assert (
            ev2.get("DiskConnectionsCreated", 0) + ev2.get("DiskConnectionsReused", 0)
            == 0
        )

        ev3 = get_profile_events("test no cache")
        assert ev3.get("PageCacheChunkDataHits", 0) == 0
        assert ev3.get("PageCacheChunkMisses", 0) == 0
        assert (
            ev3.get("DiskConnectionsCreated", 0) + ev3.get("DiskConnectionsReused", 0)
            > 0
        )

        node.query("DROP TABLE test{} SYNC".format(i))
        print(f"Ok {i}")


def test_config_reload(cluster):
    node1 = cluster.instances["node5"]
    table_name = "config_reload"

    global uuids
    node1.query(
        f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} UUID '{uuids[0]}'
        (id Int32) ENGINE = MergeTree() ORDER BY id
        SETTINGS disk = disk(type=web, endpoint='http://nginx:80/test1/');
    """
    )

    node1.query("SYSTEM RELOAD CONFIG")
    node1.query(f"DROP TABLE {table_name} SYNC")
