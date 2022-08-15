import json
import random
import re
import string
import threading
import time
from multiprocessing.dummy import Pool

import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/logs_config.xml",
        "configs/config.d/storage_configuration.xml",
        "configs/config.d/cluster.xml",
    ],
    with_zookeeper=True,
    stay_alive=True,
    tmpfs=["/jbod1:size=40M", "/jbod2:size=40M", "/external:size=200M"],
    macros={"shard": 0, "replica": 1},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/logs_config.xml",
        "configs/config.d/storage_configuration.xml",
        "configs/config.d/cluster.xml",
    ],
    with_zookeeper=True,
    stay_alive=True,
    tmpfs=["/jbod1:size=40M", "/jbod2:size=40M", "/external:size=200M"],
    macros={"shard": 0, "replica": 2},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_system_tables(start_cluster):
    expected_disks_data = [
        {
            "name": "default",
            "path": "/var/lib/clickhouse/",
            "keep_free_space": "1024",
        },
        {
            "name": "jbod1",
            "path": "/jbod1/",
            "keep_free_space": "0",
        },
        {
            "name": "jbod2",
            "path": "/jbod2/",
            "keep_free_space": "10485760",
        },
        {
            "name": "external",
            "path": "/external/",
            "keep_free_space": "0",
        },
    ]

    click_disk_data = json.loads(
        node1.query("SELECT name, path, keep_free_space FROM system.disks FORMAT JSON")
    )["data"]
    assert sorted(click_disk_data, key=lambda x: x["name"]) == sorted(
        expected_disks_data, key=lambda x: x["name"]
    )

    expected_policies_data = [
        {
            "policy_name": "small_jbod_with_external",
            "volume_name": "main",
            "volume_priority": "1",
            "disks": ["jbod1"],
            "volume_type": "JBOD",
            "max_data_part_size": "0",
            "move_factor": 0.1,
            "prefer_not_to_merge": 0,
        },
        {
            "policy_name": "small_jbod_with_external",
            "volume_name": "external",
            "volume_priority": "2",
            "disks": ["external"],
            "volume_type": "JBOD",
            "max_data_part_size": "0",
            "move_factor": 0.1,
            "prefer_not_to_merge": 0,
        },
        {
            "policy_name": "small_jbod_with_external_no_merges",
            "volume_name": "main",
            "volume_priority": "1",
            "disks": ["jbod1"],
            "volume_type": "JBOD",
            "max_data_part_size": "0",
            "move_factor": 0.1,
            "prefer_not_to_merge": 0,
        },
        {
            "policy_name": "small_jbod_with_external_no_merges",
            "volume_name": "external",
            "volume_priority": "2",
            "disks": ["external"],
            "volume_type": "JBOD",
            "max_data_part_size": "0",
            "move_factor": 0.1,
            "prefer_not_to_merge": 1,
        },
        {
            "policy_name": "one_more_small_jbod_with_external",
            "volume_name": "m",
            "volume_priority": "1",
            "disks": ["jbod1"],
            "volume_type": "JBOD",
            "max_data_part_size": "0",
            "move_factor": 0.1,
            "prefer_not_to_merge": 0,
        },
        {
            "policy_name": "one_more_small_jbod_with_external",
            "volume_name": "e",
            "volume_priority": "2",
            "disks": ["external"],
            "volume_type": "JBOD",
            "max_data_part_size": "0",
            "move_factor": 0.1,
            "prefer_not_to_merge": 0,
        },
        {
            "policy_name": "jbods_with_external",
            "volume_name": "main",
            "volume_priority": "1",
            "disks": ["jbod1", "jbod2"],
            "volume_type": "JBOD",
            "max_data_part_size": "10485760",
            "move_factor": 0.1,
            "prefer_not_to_merge": 0,
        },
        {
            "policy_name": "jbods_with_external",
            "volume_name": "external",
            "volume_priority": "2",
            "disks": ["external"],
            "volume_type": "JBOD",
            "max_data_part_size": "0",
            "move_factor": 0.1,
            "prefer_not_to_merge": 0,
        },
        {
            "policy_name": "moving_jbod_with_external",
            "volume_name": "main",
            "volume_priority": "1",
            "disks": ["jbod1"],
            "volume_type": "JBOD",
            "max_data_part_size": "0",
            "move_factor": 0.7,
            "prefer_not_to_merge": 0,
        },
        {
            "policy_name": "moving_jbod_with_external",
            "volume_name": "external",
            "volume_priority": "2",
            "disks": ["external"],
            "volume_type": "JBOD",
            "max_data_part_size": "0",
            "move_factor": 0.7,
            "prefer_not_to_merge": 0,
        },
        {
            "policy_name": "default_disk_with_external",
            "volume_name": "small",
            "volume_priority": "1",
            "disks": ["default"],
            "volume_type": "JBOD",
            "max_data_part_size": "2097152",
            "move_factor": 0.1,
            "prefer_not_to_merge": 0,
        },
        {
            "policy_name": "default_disk_with_external",
            "volume_name": "big",
            "volume_priority": "2",
            "disks": ["external"],
            "volume_type": "JBOD",
            "max_data_part_size": "20971520",
            "move_factor": 0.1,
            "prefer_not_to_merge": 0,
        },
        {
            "policy_name": "special_warning_policy",
            "volume_name": "special_warning_zero_volume",
            "volume_priority": "1",
            "disks": ["default"],
            "volume_type": "JBOD",
            "max_data_part_size": "0",
            "move_factor": 0.1,
            "prefer_not_to_merge": 0,
        },
        {
            "policy_name": "special_warning_policy",
            "volume_name": "special_warning_default_volume",
            "volume_priority": "2",
            "disks": ["external"],
            "volume_type": "JBOD",
            "max_data_part_size": "0",
            "move_factor": 0.1,
            "prefer_not_to_merge": 0,
        },
        {
            "policy_name": "special_warning_policy",
            "volume_name": "special_warning_small_volume",
            "volume_priority": "3",
            "disks": ["jbod1"],
            "volume_type": "JBOD",
            "max_data_part_size": "1024",
            "move_factor": 0.1,
            "prefer_not_to_merge": 0,
        },
        {
            "policy_name": "special_warning_policy",
            "volume_name": "special_warning_big_volume",
            "volume_priority": "4",
            "disks": ["jbod2"],
            "volume_type": "JBOD",
            "max_data_part_size": "1024000000",
            "move_factor": 0.1,
            "prefer_not_to_merge": 0,
        },
    ]

    clickhouse_policies_data = json.loads(
        node1.query(
            "SELECT * FROM system.storage_policies WHERE policy_name != 'default' FORMAT JSON"
        )
    )["data"]

    def key(x):
        return (x["policy_name"], x["volume_name"], x["volume_priority"])

    assert sorted(clickhouse_policies_data, key=key) == sorted(
        expected_policies_data, key=key
    )


def test_query_parser(start_cluster):
    try:
        with pytest.raises(QueryRuntimeException):
            node1.query(
                """
                CREATE TABLE IF NOT EXISTS table_with_absent_policy (
                    d UInt64
                ) ENGINE = MergeTree()
                ORDER BY d
                SETTINGS storage_policy='very_exciting_policy'
            """
            )

        with pytest.raises(QueryRuntimeException):
            node1.query(
                """
                CREATE TABLE IF NOT EXISTS table_with_absent_policy (
                    d UInt64
                ) ENGINE = MergeTree()
                ORDER BY d
                SETTINGS storage_policy='jbod1'
            """
            )

        node1.query(
            """
                CREATE TABLE IF NOT EXISTS table_with_normal_policy (
                    d UInt64
                ) ENGINE = MergeTree()
                ORDER BY d
                SETTINGS storage_policy='default'
        """
        )

        node1.query("INSERT INTO table_with_normal_policy VALUES (5)")

        with pytest.raises(QueryRuntimeException):
            node1.query(
                "ALTER TABLE table_with_normal_policy MOVE PARTITION tuple() TO VOLUME 'some_volume'"
            )

        with pytest.raises(QueryRuntimeException):
            node1.query(
                "ALTER TABLE table_with_normal_policy MOVE PARTITION tuple() TO DISK 'some_volume'"
            )

        with pytest.raises(QueryRuntimeException):
            node1.query(
                "ALTER TABLE table_with_normal_policy MOVE PART 'xxxxx' TO DISK 'jbod1'"
            )

        with pytest.raises(QueryRuntimeException):
            node1.query(
                "ALTER TABLE table_with_normal_policy MOVE PARTITION 'yyyy' TO DISK 'jbod1'"
            )

        with pytest.raises(QueryRuntimeException):
            node1.query(
                "ALTER TABLE table_with_normal_policy MODIFY SETTING storage_policy='moving_jbod_with_external'"
            )
    finally:
        node1.query("DROP TABLE IF EXISTS table_with_normal_policy SYNC")


@pytest.mark.parametrize(
    "name,engine",
    [
        pytest.param("test_alter_policy", "MergeTree()", id="mt"),
        pytest.param(
            "replicated_test_alter_policy",
            "ReplicatedMergeTree('/clickhouse/test_alter_policy', '1')",
            id="replicated",
        ),
    ],
)
def test_alter_policy(start_cluster, name, engine):
    try:
        node1.query_with_retry(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                d UInt64
            ) ENGINE = {engine}
            ORDER BY d
            SETTINGS storage_policy='small_jbod_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        assert (
            node1.query(
                """SELECT storage_policy FROM system.tables WHERE name = '{name}'""".format(
                    name=name
                )
            )
            == "small_jbod_with_external\n"
        )

        with pytest.raises(QueryRuntimeException):
            node1.query(
                """ALTER TABLE {name} MODIFY SETTING storage_policy='one_more_small_jbod_with_external'""".format(
                    name=name
                )
            )

        assert (
            node1.query(
                """SELECT storage_policy FROM system.tables WHERE name = '{name}'""".format(
                    name=name
                )
            )
            == "small_jbod_with_external\n"
        )

        node1.query_with_retry(
            """ALTER TABLE {name} MODIFY SETTING storage_policy='jbods_with_external'""".format(
                name=name
            )
        )

        assert (
            node1.query(
                """SELECT storage_policy FROM system.tables WHERE name = '{name}'""".format(
                    name=name
                )
            )
            == "jbods_with_external\n"
        )

        with pytest.raises(QueryRuntimeException):
            node1.query(
                """ALTER TABLE {name} MODIFY SETTING storage_policy='small_jbod_with_external'""".format(
                    name=name
                )
            )

        assert (
            node1.query(
                """SELECT storage_policy FROM system.tables WHERE name = '{name}'""".format(
                    name=name
                )
            )
            == "jbods_with_external\n"
        )

    finally:
        node1.query_with_retry(f"DROP TABLE IF EXISTS {name} SYNC")


def get_random_string(length):
    return "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(length)
    )


def get_used_disks_for_table(node, table_name):
    return tuple(
        node.query(
            "select disk_name from system.parts where table == '{}' and active=1 order by modification_time".format(
                table_name
            )
        )
        .strip()
        .split("\n")
    )


def get_used_parts_for_table(node, table_name):
    return node.query(
        "SELECT name FROM system.parts WHERE table = '{}' AND active = 1 ORDER BY modification_time".format(
            table_name
        )
    ).splitlines()


def test_no_warning_about_zero_max_data_part_size(start_cluster):
    def get_log(node):
        return node.exec_in_container(
            ["bash", "-c", "cat /var/log/clickhouse-server/clickhouse-server.log"]
        )

    for node in (node1, node2):
        node.query(
            """
            CREATE TABLE IF NOT EXISTS default.test_warning_table (
                s String
            ) ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS storage_policy='small_jbod_with_external'
        """
        )
        node.query("DROP TABLE IF EXISTS default.test_warning_table SYNC")
        log = get_log(node)
        assert not re.search("Warning.*Volume.*special_warning_zero_volume", log)
        assert not re.search("Warning.*Volume.*special_warning_default_volume", log)
        assert re.search("Warning.*Volume.*special_warning_small_volume", log)
        assert not re.search("Warning.*Volume.*special_warning_big_volume", log)


@pytest.mark.parametrize(
    "name,engine",
    [
        pytest.param("mt_on_jbod", "MergeTree()", id="mt"),
        pytest.param(
            "replicated_mt_on_jbod",
            "ReplicatedMergeTree('/clickhouse/replicated_mt_on_jbod', '1')",
            id="replicated",
        ),
    ],
)
def test_round_robin(start_cluster, name, engine):
    try:
        node1.query_with_retry(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                d UInt64
            ) ENGINE = {engine}
            ORDER BY d
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        # first should go to the jbod1
        node1.query_with_retry(
            "insert into {} select * from numbers(10000)".format(name)
        )
        used_disk = get_used_disks_for_table(node1, name)
        assert len(used_disk) == 1, "More than one disk used for single insert"

        # sleep is required because we order disks by their modification time, and if insert will be fast
        # modification time of two disks will be equal, then sort will not provide deterministic results
        time.sleep(5)

        node1.query_with_retry(
            "insert into {} select * from numbers(10000, 10000)".format(name)
        )
        used_disks = get_used_disks_for_table(node1, name)

        assert len(used_disks) == 2, "Two disks should be used for two parts"
        assert used_disks[0] != used_disks[1], "Should write to different disks"

        time.sleep(5)

        node1.query_with_retry(
            "insert into {} select * from numbers(20000, 10000)".format(name)
        )
        used_disks = get_used_disks_for_table(node1, name)

        # jbod1 -> jbod2 -> jbod1 -> jbod2 ... etc
        assert len(used_disks) == 3
        assert used_disks[0] != used_disks[1]
        assert used_disks[2] == used_disks[0]
    finally:
        node1.query_with_retry(f"DROP TABLE IF EXISTS {name} SYNC")


@pytest.mark.parametrize(
    "name,engine",
    [
        pytest.param("mt_with_huge_part", "MergeTree()", id="mt"),
        pytest.param(
            "replicated_mt_with_huge_part",
            "ReplicatedMergeTree('/clickhouse/replicated_mt_with_huge_part', '1')",
            id="replicated",
        ),
    ],
)
def test_max_data_part_size(start_cluster, name, engine):
    try:
        assert (
            int(
                *node1.query(
                    """SELECT max_data_part_size FROM system.storage_policies WHERE policy_name = 'jbods_with_external' AND volume_name = 'main'"""
                ).splitlines()
            )
            == 10 * 1024 * 1024
        )

        node1.query_with_retry(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                s1 String
            ) ENGINE = {engine}
            ORDER BY tuple()
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )
        # 10MB
        node1.query_with_retry(
            """
            INSERT INTO {} SELECT
                randomPrintableASCII(1024 * 1024)
            FROM numbers(10)
        """.format(
                name
            )
        )
        used_disks = get_used_disks_for_table(node1, name)
        assert len(used_disks) == 1
        assert used_disks[0] == "external"
    finally:
        node1.query_with_retry(f"DROP TABLE IF EXISTS {name} SYNC")


@pytest.mark.parametrize(
    "name,engine",
    [
        pytest.param("mt_with_overflow", "MergeTree()", id="mt"),
        pytest.param(
            "replicated_mt_with_overflow",
            "ReplicatedMergeTree('/clickhouse/replicated_mt_with_overflow', '1')",
            id="replicated",
        ),
    ],
)
def test_jbod_overflow(start_cluster, name, engine):
    try:
        node1.query_with_retry(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                s1 String
            ) ENGINE = {engine}
            ORDER BY tuple()
            SETTINGS storage_policy='small_jbod_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        node1.query(f"SYSTEM STOP MERGES {name}")

        # small jbod size is 40MB, so lets insert 35MB
        for _ in range(7):
            node1.query_with_retry(
                """
                INSERT INTO {} SELECT
                    randomPrintableASCII(1024 * 1024)
                FROM numbers(5)
            """.format(
                    name
                )
            )

        used_disks = get_used_disks_for_table(node1, name)
        assert used_disks == tuple("jbod1" for _ in used_disks)

        # should go to the external disk (jbod is overflown)
        # 10MB
        node1.query_with_retry(
            """
            INSERT INTO {} SELECT
                randomPrintableASCII(1024 * 1024)
            FROM numbers(10)
        """.format(
                name
            )
        )

        used_disks = get_used_disks_for_table(node1, name)

        assert used_disks[-1] == "external"

        node1.query(f"SYSTEM START MERGES {name}")
        time.sleep(1)

        node1.query_with_retry("OPTIMIZE TABLE {} FINAL".format(name))
        time.sleep(2)

        disks_for_merges = tuple(
            node1.query(
                "SELECT disk_name FROM system.parts WHERE table == '{}' AND level >= 1 and active = 1 ORDER BY modification_time".format(
                    name
                )
            )
            .strip()
            .split("\n")
        )

        assert disks_for_merges == tuple("external" for _ in disks_for_merges)

    finally:
        node1.query_with_retry(f"DROP TABLE IF EXISTS {name} SYNC")


@pytest.mark.parametrize(
    "name,engine",
    [
        pytest.param("moving_mt", "MergeTree()", id="mt"),
        pytest.param(
            "moving_replicated_mt",
            "ReplicatedMergeTree('/clickhouse/moving_replicated_mt', '1')",
            id="replicated",
        ),
    ],
)
def test_background_move(start_cluster, name, engine):
    try:
        node1.query_with_retry(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                s1 String
            ) ENGINE = {engine}
            ORDER BY tuple()
            SETTINGS storage_policy='moving_jbod_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        node1.query(f"SYSTEM STOP MERGES {name}")

        for _ in range(5):
            # 5MB
            node1.query_with_retry(
                """
                INSERT INTO {} SELECT
                    randomPrintableASCII(1024 * 1024)
                FROM numbers(5)
            """.format(
                    name
                )
            )

        used_disks = get_used_disks_for_table(node1, name)

        retry = 20
        i = 0
        while not sum(1 for x in used_disks if x == "jbod1") <= 2 and i < retry:
            time.sleep(0.5)
            used_disks = get_used_disks_for_table(node1, name)
            i += 1

        assert sum(1 for x in used_disks if x == "jbod1") <= 2

        # first (oldest) part was moved to external
        assert used_disks[0] == "external"

        path = node1.query(
            "SELECT path_on_disk FROM system.part_log WHERE table = '{}' AND event_type='MovePart' ORDER BY event_time LIMIT 1".format(
                name
            )
        )

        # first (oldest) part was moved to external
        assert path.startswith("/external")

        node1.query(f"SYSTEM START MERGES {name}")

    finally:
        node1.query_with_retry(f"DROP TABLE IF EXISTS {name} SYNC")


@pytest.mark.parametrize(
    "name,engine",
    [
        pytest.param("stopped_moving_mt", "MergeTree()", id="mt"),
        pytest.param(
            "stopped_moving_replicated_mt",
            "ReplicatedMergeTree('/clickhouse/stopped_moving_replicated_mt', '1')",
            id="replicated",
        ),
    ],
)
def test_start_stop_moves(start_cluster, name, engine):
    try:
        node1.query_with_retry(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                s1 String
            ) ENGINE = {engine}
            ORDER BY tuple()
            SETTINGS storage_policy='moving_jbod_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        node1.query_with_retry("INSERT INTO {} VALUES ('HELLO')".format(name))
        node1.query_with_retry("INSERT INTO {} VALUES ('WORLD')".format(name))

        used_disks = get_used_disks_for_table(node1, name)
        assert all(d == "jbod1" for d in used_disks), "All writes shoud go to jbods"

        first_part = node1.query(
            "SELECT name FROM system.parts WHERE table = '{}' and active = 1 ORDER BY modification_time LIMIT 1".format(
                name
            )
        ).strip()

        node1.query("SYSTEM STOP MOVES")

        with pytest.raises(QueryRuntimeException):
            node1.query(
                "ALTER TABLE {} MOVE PART '{}' TO VOLUME 'external'".format(
                    name, first_part
                )
            )

        used_disks = get_used_disks_for_table(node1, name)
        assert all(
            d == "jbod1" for d in used_disks
        ), "Blocked moves doesn't actually move something"

        node1.query("SYSTEM START MOVES")

        node1.query(
            "ALTER TABLE {} MOVE PART '{}' TO VOLUME 'external'".format(
                name, first_part
            )
        )

        disk = node1.query(
            "SELECT disk_name FROM system.parts WHERE table = '{}' and name = '{}' and active = 1".format(
                name, first_part
            )
        ).strip()

        assert disk == "external"

        node1.query_with_retry("TRUNCATE TABLE {}".format(name))

        node1.query("SYSTEM STOP MOVES {}".format(name))
        node1.query("SYSTEM STOP MERGES {}".format(name))

        for _ in range(5):
            # 5MB
            node1.query_with_retry(
                """
                INSERT INTO {} SELECT
                    randomPrintableASCII(1024 * 1024)
                FROM numbers(5)
            """.format(
                    name
                )
            )

        used_disks = get_used_disks_for_table(node1, name)

        retry = 5
        i = 0
        while not sum(1 for x in used_disks if x == "jbod1") <= 2 and i < retry:
            time.sleep(0.1)
            used_disks = get_used_disks_for_table(node1, name)
            i += 1

        # first (oldest) part doesn't move anywhere
        assert used_disks[0] == "jbod1"

        node1.query("SYSTEM START MOVES {}".format(name))

        # wait sometime until background backoff finishes
        retry = 30
        i = 0
        while not sum(1 for x in used_disks if x == "jbod1") <= 2 and i < retry:
            time.sleep(1)
            used_disks = get_used_disks_for_table(node1, name)
            i += 1

        node1.query("SYSTEM START MERGES {}".format(name))

        assert sum(1 for x in used_disks if x == "jbod1") <= 2

        # first (oldest) part moved to external
        assert used_disks[0] == "external"

    finally:
        node1.query_with_retry(f"DROP TABLE IF EXISTS {name} SYNC")


def get_path_for_part_from_part_log(node, table, part_name):
    node.query("SYSTEM FLUSH LOGS")
    path = node.query(
        "SELECT path_on_disk FROM system.part_log WHERE table = '{}' and part_name = '{}' ORDER BY event_time DESC LIMIT 1".format(
            table, part_name
        )
    )
    return path.strip()


def get_paths_for_partition_from_part_log(node, table, partition_id):
    node.query("SYSTEM FLUSH LOGS")
    paths = node.query(
        "SELECT path_on_disk FROM system.part_log WHERE table = '{}' and partition_id = '{}' ORDER BY event_time DESC".format(
            table, partition_id
        )
    )
    return paths.strip().split("\n")


@pytest.mark.parametrize(
    "name,engine,use_metadata_cache",
    [
        pytest.param("altering_mt", "MergeTree()", "false", id="mt"),
        pytest.param("altering_mt", "MergeTree()", "true", id="mt_use_metadata_cache"),
        # ("altering_replicated_mt","ReplicatedMergeTree('/clickhouse/altering_replicated_mt', '1')",),
        # SYSTEM STOP MERGES doesn't disable merges assignments
    ],
)
def test_alter_move(start_cluster, name, engine, use_metadata_cache):
    try:
        node1.query(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                EventDate Date,
                number UInt64
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION BY toYYYYMM(EventDate)
            SETTINGS storage_policy='jbods_with_external', use_metadata_cache={use_metadata_cache}
        """.format(
                name=name, engine=engine, use_metadata_cache=use_metadata_cache
            )
        )

        node1.query("SYSTEM STOP MERGES {}".format(name))  # to avoid conflicts

        node1.query("INSERT INTO {} VALUES(toDate('2019-03-15'), 65)".format(name))
        node1.query("INSERT INTO {} VALUES(toDate('2019-03-16'), 66)".format(name))
        node1.query("INSERT INTO {} VALUES(toDate('2019-04-10'), 42)".format(name))
        node1.query("INSERT INTO {} VALUES(toDate('2019-04-11'), 43)".format(name))
        assert node1.query("CHECK TABLE " + name) == "1\n"

        used_disks = get_used_disks_for_table(node1, name)
        assert all(
            d.startswith("jbod") for d in used_disks
        ), "All writes should go to jbods"

        first_part = node1.query(
            "SELECT name FROM system.parts WHERE table = '{}' and active = 1 ORDER BY modification_time LIMIT 1".format(
                name
            )
        ).strip()

        time.sleep(1)
        node1.query(
            "ALTER TABLE {} MOVE PART '{}' TO VOLUME 'external'".format(
                name, first_part
            )
        )
        assert node1.query("CHECK TABLE " + name) == "1\n"
        disk = node1.query(
            "SELECT disk_name FROM system.parts WHERE table = '{}' and name = '{}' and active = 1".format(
                name, first_part
            )
        ).strip()
        assert disk == "external"
        assert get_path_for_part_from_part_log(node1, name, first_part).startswith(
            "/external"
        )

        time.sleep(1)
        node1.query(
            "ALTER TABLE {} MOVE PART '{}' TO DISK 'jbod1'".format(name, first_part)
        )
        assert node1.query("CHECK TABLE " + name) == "1\n"
        disk = node1.query(
            "SELECT disk_name FROM system.parts WHERE table = '{}' and name = '{}' and active = 1".format(
                name, first_part
            )
        ).strip()
        assert disk == "jbod1"
        assert get_path_for_part_from_part_log(node1, name, first_part).startswith(
            "/jbod1"
        )

        time.sleep(1)
        node1.query(
            "ALTER TABLE {} MOVE PARTITION 201904 TO VOLUME 'external'".format(name)
        )
        assert node1.query("CHECK TABLE " + name) == "1\n"
        disks = (
            node1.query(
                "SELECT disk_name FROM system.parts WHERE table = '{}' and partition = '201904' and active = 1".format(
                    name
                )
            )
            .strip()
            .split("\n")
        )
        assert len(disks) == 2
        assert all(d == "external" for d in disks)
        assert all(
            path.startswith("/external")
            for path in get_paths_for_partition_from_part_log(node1, name, "201904")[:2]
        )

        time.sleep(1)
        node1.query("ALTER TABLE {} MOVE PARTITION 201904 TO DISK 'jbod2'".format(name))
        assert node1.query("CHECK TABLE " + name) == "1\n"
        disks = (
            node1.query(
                "SELECT disk_name FROM system.parts WHERE table = '{}' and partition = '201904' and active = 1".format(
                    name
                )
            )
            .strip()
            .split("\n")
        )
        assert len(disks) == 2
        assert all(d == "jbod2" for d in disks)
        assert all(
            path.startswith("/jbod2")
            for path in get_paths_for_partition_from_part_log(node1, name, "201904")[:2]
        )

        assert node1.query("SELECT COUNT() FROM {}".format(name)) == "4\n"

    finally:
        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")


@pytest.mark.parametrize("volume_or_disk", ["DISK", "VOLUME"])
def test_alter_move_half_of_partition(start_cluster, volume_or_disk):
    name = "alter_move_half_of_partition"
    engine = "MergeTree()"
    try:
        node1.query(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                EventDate Date,
                number UInt64
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION BY toYYYYMM(EventDate)
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        node1.query("SYSTEM STOP MERGES {}".format(name))

        node1.query("INSERT INTO {} VALUES(toDate('2019-03-15'), 65)".format(name))
        node1.query("INSERT INTO {} VALUES(toDate('2019-03-16'), 42)".format(name))
        used_disks = get_used_disks_for_table(node1, name)
        assert all(
            d.startswith("jbod") for d in used_disks
        ), "All writes should go to jbods"

        time.sleep(1)
        parts = node1.query(
            "SELECT name FROM system.parts WHERE table = '{}' and active = 1".format(
                name
            )
        ).splitlines()
        assert len(parts) == 2

        node1.query(
            "ALTER TABLE {} MOVE PART '{}' TO VOLUME 'external'".format(name, parts[0])
        )
        disks = node1.query(
            "SELECT disk_name FROM system.parts WHERE table = '{}' and name = '{}' and active = 1".format(
                name, parts[0]
            )
        ).splitlines()
        assert disks == ["external"]

        time.sleep(1)
        node1.query(
            "ALTER TABLE {} MOVE PARTITION 201903 TO {volume_or_disk} 'external'".format(
                name, volume_or_disk=volume_or_disk
            )
        )
        disks = node1.query(
            "SELECT disk_name FROM system.parts WHERE table = '{}' and partition = '201903' and active = 1".format(
                name
            )
        ).splitlines()
        assert disks == ["external"] * 2

        assert node1.query("SELECT COUNT() FROM {}".format(name)) == "2\n"

    finally:
        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")


@pytest.mark.parametrize("volume_or_disk", ["DISK", "VOLUME"])
def test_alter_double_move_partition(start_cluster, volume_or_disk):
    name = "alter_double_move_partition"
    engine = "MergeTree()"
    try:
        node1.query(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                EventDate Date,
                number UInt64
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION BY toYYYYMM(EventDate)
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        node1.query("SYSTEM STOP MERGES {}".format(name))

        node1.query("INSERT INTO {} VALUES(toDate('2019-03-15'), 65)".format(name))
        node1.query("INSERT INTO {} VALUES(toDate('2019-03-16'), 42)".format(name))
        used_disks = get_used_disks_for_table(node1, name)
        assert all(
            d.startswith("jbod") for d in used_disks
        ), "All writes should go to jbods"

        time.sleep(1)
        node1.query(
            "ALTER TABLE {} MOVE PARTITION 201903 TO {volume_or_disk} 'external'".format(
                name, volume_or_disk=volume_or_disk
            )
        )
        disks = node1.query(
            "SELECT disk_name FROM system.parts WHERE table = '{}' and partition = '201903' and active = 1".format(
                name
            )
        ).splitlines()
        assert disks == ["external"] * 2

        assert node1.query("SELECT COUNT() FROM {}".format(name)) == "2\n"

        time.sleep(1)
        with pytest.raises(QueryRuntimeException):
            node1.query(
                "ALTER TABLE {} MOVE PARTITION 201903 TO {volume_or_disk} 'external'".format(
                    name, volume_or_disk=volume_or_disk
                )
            )

    finally:
        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")


def produce_alter_move(node, name):
    move_type = random.choice(["PART", "PARTITION"])
    if move_type == "PART":
        for _ in range(10):
            try:
                parts = (
                    node1.query(
                        "SELECT name from system.parts where table = '{}' and active = 1".format(
                            name
                        )
                    )
                    .strip()
                    .split("\n")
                )
                break
            except QueryRuntimeException:
                pass
        else:
            raise Exception("Cannot select from system.parts")

        move_part = random.choice(["'" + part + "'" for part in parts])
    else:
        move_part = random.choice([201903, 201904])

    move_disk = random.choice(["DISK", "VOLUME"])
    if move_disk == "DISK":
        move_volume = random.choice(["'external'", "'jbod1'", "'jbod2'"])
    else:
        move_volume = random.choice(["'main'", "'external'"])
    try:
        node1.query(
            "ALTER TABLE {} MOVE {mt} {mp} TO {md} {mv}".format(
                name, mt=move_type, mp=move_part, md=move_disk, mv=move_volume
            )
        )
    except QueryRuntimeException as ex:
        pass


@pytest.mark.parametrize(
    "name,engine",
    [
        pytest.param("concurrently_altering_mt", "MergeTree()", id="mt"),
        pytest.param(
            "concurrently_altering_replicated_mt",
            "ReplicatedMergeTree('/clickhouse/concurrently_altering_replicated_mt', '1')",
            id="replicated",
        ),
    ],
)
def test_concurrent_alter_move(start_cluster, name, engine):
    try:
        node1.query_with_retry(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                EventDate Date,
                number UInt64
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION BY toYYYYMM(EventDate)
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        values = list({random.randint(1, 1000000) for _ in range(0, 1000)})

        def insert(num):
            for i in range(num):
                day = random.randint(11, 30)
                value = values.pop()
                month = "0" + str(random.choice([3, 4]))
                node1.query_with_retry(
                    "INSERT INTO {} VALUES(toDate('2019-{m}-{d}'), {v})".format(
                        name, m=month, d=day, v=value
                    )
                )

        def alter_move(num):
            for i in range(num):
                produce_alter_move(node1, name)

        def alter_update(num):
            for i in range(num):
                node1.query(
                    "ALTER TABLE {} UPDATE number = number + 1 WHERE 1".format(name)
                )

        def optimize_table(num):
            for i in range(num):
                node1.query_with_retry("OPTIMIZE TABLE {} FINAL".format(name))

        p = Pool(15)
        tasks = []
        for i in range(5):
            tasks.append(p.apply_async(insert, (100,)))
            tasks.append(p.apply_async(alter_move, (100,)))
            tasks.append(p.apply_async(alter_update, (100,)))
            tasks.append(p.apply_async(optimize_table, (100,)))

        for task in tasks:
            task.get(timeout=240)

        assert node1.query("SELECT 1") == "1\n"
        assert node1.query("SELECT COUNT() FROM {}".format(name)) == "500\n"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")


@pytest.mark.parametrize(
    "name,engine",
    [
        pytest.param("concurrently_dropping_mt", "MergeTree()", id="mt"),
        pytest.param(
            "concurrently_dropping_replicated_mt",
            "ReplicatedMergeTree('/clickhouse/concurrently_dropping_replicated_mt', '1')",
            id="replicated",
        ),
    ],
)
def test_concurrent_alter_move_and_drop(start_cluster, name, engine):
    try:
        node1.query(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                EventDate Date,
                number UInt64
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION BY toYYYYMM(EventDate)
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        values = list({random.randint(1, 1000000) for _ in range(0, 1000)})

        def insert(num):
            for i in range(num):
                day = random.randint(11, 30)
                value = values.pop()
                month = "0" + str(random.choice([3, 4]))
                node1.query_with_retry(
                    "INSERT INTO {} VALUES(toDate('2019-{m}-{d}'), {v})".format(
                        name, m=month, d=day, v=value
                    )
                )

        def alter_move(num):
            for i in range(num):
                produce_alter_move(node1, name)

        def alter_drop(num):
            for i in range(num):
                partition = random.choice([201903, 201904])
                drach = random.choice(["drop", "detach"])
                node1.query(
                    "ALTER TABLE {} {} PARTITION {}".format(name, drach, partition)
                )

        insert(100)
        p = Pool(15)
        tasks = []
        for i in range(5):
            tasks.append(p.apply_async(insert, (100,)))
            tasks.append(p.apply_async(alter_move, (100,)))
            tasks.append(p.apply_async(alter_drop, (100,)))

        for task in tasks:
            task.get(timeout=120)

        assert node1.query("SELECT 1") == "1\n"

    finally:
        node1.query_with_retry(f"DROP TABLE IF EXISTS {name} SYNC")


@pytest.mark.parametrize(
    "name,engine",
    [
        pytest.param("detach_attach_mt", "MergeTree()", id="mt"),
        pytest.param(
            "replicated_detach_attach_mt",
            "ReplicatedMergeTree('/clickhouse/replicated_detach_attach_mt', '1')",
            id="replicated",
        ),
    ],
)
def test_detach_attach(start_cluster, name, engine):
    try:
        node1.query_with_retry(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                s1 String
            ) ENGINE = {engine}
            ORDER BY tuple()
            SETTINGS storage_policy='moving_jbod_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        # 5MB
        node1.query_with_retry(
            """
            INSERT INTO {} SELECT
                randomPrintableASCII(1024 * 1024)
            FROM numbers(5)
        """.format(
                name
            )
        )

        node1.query("ALTER TABLE {} DETACH PARTITION tuple()".format(name))
        assert node1.query("SELECT count() FROM {}".format(name)).strip() == "0"

        assert (
            node1.query(
                "SELECT disk FROM system.detached_parts WHERE table = '{}'".format(name)
            ).strip()
            == "jbod1"
        )

        node1.query("ALTER TABLE {} ATTACH PARTITION tuple()".format(name))
        assert node1.query("SELECT count() FROM {}".format(name)).strip() == "5"

    finally:
        node1.query_with_retry(f"DROP TABLE IF EXISTS {name} SYNC")


@pytest.mark.parametrize(
    "name,engine",
    [
        pytest.param("mutating_mt", "MergeTree()", id="mt"),
        pytest.param(
            "replicated_mutating_mt",
            "ReplicatedMergeTree('/clickhouse/replicated_mutating_mt', '1')",
            id="replicated",
        ),
    ],
)
def test_mutate_to_another_disk(start_cluster, name, engine):
    try:
        node1.query_with_retry(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                s1 String
            ) ENGINE = {engine}
            ORDER BY tuple()
            SETTINGS storage_policy='moving_jbod_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        for _ in range(5):
            # 5MB
            node1.query_with_retry(
                """
                INSERT INTO {} SELECT
                    randomPrintableASCII(1024 * 1024)
                FROM numbers(5)
            """.format(
                    name
                )
            )

        node1.query("ALTER TABLE {} UPDATE s1 = concat(s1, 'x') WHERE 1".format(name))

        retry = 20
        while (
            node1.query("SELECT * FROM system.mutations WHERE is_done = 0") != ""
            and retry > 0
        ):
            retry -= 1
            time.sleep(0.5)

        if (
            node1.query(
                "SELECT latest_fail_reason FROM system.mutations WHERE table = '{}'".format(
                    name
                )
            )
            == ""
        ):
            assert (
                node1.query("SELECT sum(endsWith(s1, 'x')) FROM {}".format(name))
                == "25\n"
            )
        else:  # mutation failed, let's try on another disk
            print("Mutation failed")
            node1.query_with_retry("OPTIMIZE TABLE {} FINAL".format(name))
            node1.query(
                "ALTER TABLE {} UPDATE s1 = concat(s1, 'x') WHERE 1".format(name)
            )
            retry = 20
            while (
                node1.query("SELECT * FROM system.mutations WHERE is_done = 0") != ""
                and retry > 0
            ):
                retry -= 1
                time.sleep(0.5)

            assert (
                node1.query("SELECT sum(endsWith(s1, 'x')) FROM {}".format(name))
                == "25\n"
            )

    finally:
        node1.query_with_retry(f"DROP TABLE IF EXISTS {name} SYNC")


@pytest.mark.parametrize(
    "name,engine",
    [
        pytest.param("alter_modifying_mt", "MergeTree()", id="mt"),
        pytest.param(
            "replicated_alter_modifying_mt",
            "ReplicatedMergeTree('/clickhouse/replicated_alter_modifying_mt', '1')",
            id="replicated",
        ),
    ],
)
def test_concurrent_alter_modify(start_cluster, name, engine):
    try:
        node1.query_with_retry(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                EventDate Date,
                number UInt64
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION BY toYYYYMM(EventDate)
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        values = list({random.randint(1, 1000000) for _ in range(0, 1000)})

        def insert(num):
            for i in range(num):
                day = random.randint(11, 30)
                value = values.pop()
                month = "0" + str(random.choice([3, 4]))
                node1.query_with_retry(
                    "INSERT INTO {} VALUES(toDate('2019-{m}-{d}'), {v})".format(
                        name, m=month, d=day, v=value
                    )
                )

        def alter_move(num):
            for i in range(num):
                produce_alter_move(node1, name)

        def alter_modify(num):
            for i in range(num):
                column_type = random.choice(["UInt64", "String"])
                try:
                    node1.query(
                        "ALTER TABLE {} MODIFY COLUMN number {}".format(
                            name, column_type
                        )
                    )
                except:
                    if "Replicated" not in engine:
                        raise

        insert(100)

        assert node1.query("SELECT COUNT() FROM {}".format(name)) == "100\n"

        p = Pool(50)
        tasks = []
        for i in range(5):
            tasks.append(p.apply_async(alter_move, (100,)))
            tasks.append(p.apply_async(alter_modify, (100,)))

        for task in tasks:
            task.get(timeout=120)

        assert node1.query("SELECT 1") == "1\n"
        assert node1.query("SELECT COUNT() FROM {}".format(name)) == "100\n"

    finally:
        node1.query_with_retry(f"DROP TABLE IF EXISTS {name} SYNC")


def test_simple_replication_and_moves(start_cluster):
    try:
        for i, node in enumerate([node1, node2]):
            node.query_with_retry(
                """
                CREATE TABLE IF NOT EXISTS replicated_table_for_moves (
                    s1 String
                ) ENGINE = ReplicatedMergeTree('/clickhouse/replicated_table_for_moves', '{}')
                ORDER BY tuple()
                SETTINGS storage_policy='moving_jbod_with_external', old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=2
            """.format(
                    i + 1
                )
            )

        def insert(num):
            for i in range(num):
                node = random.choice([node1, node2])
                node.query_with_retry(
                    """
                    INSERT INTO replicated_table_for_moves SELECT
                        randomPrintableASCII(512 * 1024)
                    FROM numbers(2)
                """
                )

        def optimize(num):
            for i in range(num):
                node = random.choice([node1, node2])
                node.query_with_retry("OPTIMIZE TABLE replicated_table_for_moves FINAL")

        p = Pool(60)
        tasks = []
        tasks.append(p.apply_async(insert, (20,)))
        tasks.append(p.apply_async(optimize, (20,)))

        for task in tasks:
            task.get(timeout=60)

        node1.query_with_retry(
            "SYSTEM SYNC REPLICA ON CLUSTER test_cluster replicated_table_for_moves",
            timeout=5,
        )

        node1.query("SELECT COUNT() FROM replicated_table_for_moves") == "40\n"
        node2.query("SELECT COUNT() FROM replicated_table_for_moves") == "40\n"

        time.sleep(3)  # wait until old parts will be deleted
        node1.query("SYSTEM STOP MERGES")
        node2.query("SYSTEM STOP MERGES")

        node1.query_with_retry(
            """
            INSERT INTO replicated_table_for_moves SELECT
                randomPrintableASCII(512 * 1024)
            FROM numbers(2)
        """
        )
        node2.query_with_retry(
            """
            INSERT INTO replicated_table_for_moves SELECT
                randomPrintableASCII(512 * 1024)
            FROM numbers(2)
        """
        )

        time.sleep(3)  # nothing was moved

        disks1 = get_used_disks_for_table(node1, "replicated_table_for_moves")
        disks2 = get_used_disks_for_table(node2, "replicated_table_for_moves")

        node2.query("SYSTEM START MERGES ON CLUSTER test_cluster")

        set(disks1) == set(["jbod1", "external"])
        set(disks2) == set(["jbod1", "external"])
    finally:
        for node in [node1, node2]:
            node.query("DROP TABLE IF EXISTS replicated_table_for_moves SYNC")


def test_download_appropriate_disk(start_cluster):
    try:
        for i, node in enumerate([node1, node2]):
            node.query_with_retry(
                """
                CREATE TABLE IF NOT EXISTS replicated_table_for_download (
                    s1 String
                ) ENGINE = ReplicatedMergeTree('/clickhouse/replicated_table_for_download', '{}')
                ORDER BY tuple()
                SETTINGS storage_policy='moving_jbod_with_external', old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=2
            """.format(
                    i + 1
                )
            )

        # 50MB
        node1.query(
            """
            INSERT INTO replicated_table_for_download SELECT
                randomPrintableASCII(1024 * 1024)
            FROM numbers(50)
        """
        )

        for _ in range(10):
            try:
                print("Syncing replica")
                node2.query_with_retry(
                    "SYSTEM SYNC REPLICA replicated_table_for_download"
                )
                break
            except:
                time.sleep(0.5)

        disks2 = get_used_disks_for_table(node2, "replicated_table_for_download")

        assert set(disks2) == set(["external"])

    finally:
        for node in [node1, node2]:
            node.query_with_retry(
                "DROP TABLE IF EXISTS replicated_table_for_download SYNC"
            )


def test_rename(start_cluster):
    try:
        node1.query(
            """
            CREATE TABLE IF NOT EXISTS default.renaming_table (
                s String
            ) ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS storage_policy='small_jbod_with_external'
        """
        )

        for _ in range(5):
            # 10MB
            node1.query(
                """
                INSERT INTO renaming_table SELECT
                    randomPrintableASCII(1024 * 1024)
                FROM numbers(10)
            """
            )

        disks = get_used_disks_for_table(node1, "renaming_table")
        assert len(disks) > 1
        assert node1.query("SELECT COUNT() FROM default.renaming_table") == "50\n"

        node1.query("RENAME TABLE default.renaming_table TO default.renaming_table1")
        assert node1.query("SELECT COUNT() FROM default.renaming_table1") == "50\n"

        with pytest.raises(QueryRuntimeException):
            node1.query("SELECT COUNT() FROM default.renaming_table")

        node1.query("CREATE DATABASE IF NOT EXISTS test")
        node1.query("RENAME TABLE default.renaming_table1 TO test.renaming_table2")
        assert node1.query("SELECT COUNT() FROM test.renaming_table2") == "50\n"

        with pytest.raises(QueryRuntimeException):
            node1.query("SELECT COUNT() FROM default.renaming_table1")

    finally:
        node1.query("DROP TABLE IF EXISTS default.renaming_table SYNC")
        node1.query("DROP TABLE IF EXISTS default.renaming_table1 SYNC")
        node1.query("DROP TABLE IF EXISTS test.renaming_table2 SYNC")


def test_freeze(start_cluster):
    try:
        node1.query(
            """
            CREATE TABLE IF NOT EXISTS default.freezing_table (
                d Date,
                s String
            ) ENGINE = MergeTree
            ORDER BY tuple()
            PARTITION BY toYYYYMM(d)
            SETTINGS storage_policy='small_jbod_with_external'
        """
        )

        for _ in range(5):
            # 10MB
            node1.query(
                """
                INSERT INTO freezing_table SELECT
                    toDate('2019-03-05'),
                    randomPrintableASCII(1024 * 1024)
                FROM numbers(10)
            """
            )

        disks = get_used_disks_for_table(node1, "freezing_table")
        assert len(disks) > 1
        assert node1.query("SELECT COUNT() FROM default.freezing_table") == "50\n"

        node1.query("ALTER TABLE freezing_table FREEZE PARTITION 201903")
        # check shadow files (backups) exists
        node1.exec_in_container(
            ["bash", "-c", "find /jbod1/shadow -name '*.mrk2' | grep '.*'"]
        )
        node1.exec_in_container(
            ["bash", "-c", "find /external/shadow -name '*.mrk2' | grep '.*'"]
        )

    finally:
        node1.query("DROP TABLE IF EXISTS default.freezing_table SYNC")
        node1.exec_in_container(["rm", "-rf", "/jbod1/shadow", "/external/shadow"])


def test_kill_while_insert(start_cluster):
    try:
        name = "test_kill_while_insert"

        node1.query(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                s String
            ) ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS storage_policy='small_jbod_with_external'
        """.format(
                name=name
            )
        )

        # 10MB
        node1.query(
            """
            INSERT INTO {} SELECT
                randomPrintableASCII(1024 * 1024)
            FROM numbers(10)
        """.format(
                name
            )
        )

        disks = get_used_disks_for_table(node1, name)
        assert set(disks) == {"jbod1"}

        def ignore_exceptions(f, *args):
            try:
                f(*args)
            except:
                """(っಠ‿ಠ)っ"""

        start_time = time.time()
        long_select = threading.Thread(
            target=ignore_exceptions,
            args=(node1.query, "SELECT sleep(3) FROM {name}".format(name=name)),
        )
        long_select.start()

        time.sleep(0.5)

        node1.query(
            "ALTER TABLE {name} MOVE PARTITION tuple() TO DISK 'external'".format(
                name=name
            )
        )
        assert time.time() - start_time < 2
        node1.restart_clickhouse(kill=True)

        try:
            long_select.join()
        except:
            """"""

        assert node1.query(
            "SELECT count() FROM {name}".format(name=name)
        ).splitlines() == ["10"]

    finally:
        try:
            node1.query(f"DROP TABLE IF EXISTS {name} SYNC")
        except:
            """ClickHouse may be inactive at this moment and we don't want to mask a meaningful exception."""


def test_move_while_merge(start_cluster):
    try:
        name = "test_move_while_merge"

        node1.query(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                n Int64
            ) ENGINE = MergeTree
            ORDER BY sleep(2)
            SETTINGS storage_policy='small_jbod_with_external'
        """.format(
                name=name
            )
        )

        node1.query("INSERT INTO {name} VALUES (1)".format(name=name))
        node1.query("INSERT INTO {name} VALUES (2)".format(name=name))

        parts = get_used_parts_for_table(node1, name)
        assert len(parts) == 2

        def optimize():
            node1.query("OPTIMIZE TABLE {name}".format(name=name))

        optimize = threading.Thread(target=optimize)
        optimize.start()

        time.sleep(0.5)

        with pytest.raises(QueryRuntimeException):
            node1.query(
                "ALTER TABLE {name} MOVE PART '{part}' TO DISK 'external'".format(
                    name=name, part=parts[0]
                )
            )

        exiting = False
        no_exception = {}

        def alter():
            while not exiting:
                try:
                    node1.query(
                        "ALTER TABLE {name} MOVE PART '{part}' TO DISK 'external'".format(
                            name=name, part=parts[0]
                        )
                    )
                    no_exception["missing"] = "exception"
                    break
                except QueryRuntimeException:
                    """"""

        alter_thread = threading.Thread(target=alter)
        alter_thread.start()

        optimize.join()

        time.sleep(0.5)

        exiting = True
        alter_thread.join()
        assert len(no_exception) == 0

        assert node1.query(
            "SELECT count() FROM {name}".format(name=name)
        ).splitlines() == ["2"]

    finally:
        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_move_across_policies_does_not_work(start_cluster):
    try:
        name = "test_move_across_policies_does_not_work"

        node1.query(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                n Int64
            ) ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name
            )
        )

        node1.query(
            """
            CREATE TABLE IF NOT EXISTS {name}2 (
                n Int64
            ) ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS storage_policy='small_jbod_with_external'
        """.format(
                name=name
            )
        )

        node1.query("""INSERT INTO {name} VALUES (1)""".format(name=name))
        try:
            node1.query(
                """ALTER TABLE {name} MOVE PARTITION tuple() TO DISK 'jbod2'""".format(
                    name=name
                )
            )
        except QueryRuntimeException:
            """All parts of partition 'all' are already on disk 'jbod2'."""

        with pytest.raises(
            QueryRuntimeException,
            match=".*because disk does not belong to storage policy.*",
        ):
            node1.query(
                """ALTER TABLE {name}2 ATTACH PARTITION tuple() FROM {name}""".format(
                    name=name
                )
            )

        with pytest.raises(
            QueryRuntimeException,
            match=".*because disk does not belong to storage policy.*",
        ):
            node1.query(
                """ALTER TABLE {name}2 REPLACE PARTITION tuple() FROM {name}""".format(
                    name=name
                )
            )

        with pytest.raises(
            QueryRuntimeException,
            match=".*should have the same storage policy of source table.*",
        ):
            node1.query(
                """ALTER TABLE {name} MOVE PARTITION tuple() TO TABLE {name}2""".format(
                    name=name
                )
            )

        assert node1.query(
            """SELECT * FROM {name}""".format(name=name)
        ).splitlines() == ["1"]

    finally:
        node1.query(f"DROP TABLE IF EXISTS {name} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {name}2 SYNC")


def _insert_merge_execute(
    node, name, policy, parts, cmds, parts_before_cmds, parts_after_cmds
):
    try:
        node.query(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                n Int64
            ) ENGINE = MergeTree
            ORDER BY tuple()
            PARTITION BY tuple()
            TTL now()-1 TO VOLUME 'external'
            SETTINGS storage_policy='{policy}'
        """.format(
                name=name, policy=policy
            )
        )

        for i in range(parts):
            node.query("""INSERT INTO {name} VALUES ({n})""".format(name=name, n=i))

        disks = get_used_disks_for_table(node, name)
        assert set(disks) == {"external"}

        node.query("""OPTIMIZE TABLE {name}""".format(name=name))

        parts = get_used_parts_for_table(node, name)
        assert len(parts) == parts_before_cmds

        for cmd in cmds:
            node.query(cmd)

        node.query("""OPTIMIZE TABLE {name}""".format(name=name))

        parts = get_used_parts_for_table(node, name)
        assert len(parts) == parts_after_cmds

    finally:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def _check_merges_are_working(node, storage_policy, volume, shall_work):
    try:
        name = "_check_merges_are_working_{storage_policy}_{volume}".format(
            storage_policy=storage_policy, volume=volume
        )

        node.query(
            """
            CREATE TABLE IF NOT EXISTS {name} (
                n Int64
            ) ENGINE = MergeTree
            ORDER BY tuple()
            PARTITION BY tuple()
            SETTINGS storage_policy='{storage_policy}'
        """.format(
                name=name, storage_policy=storage_policy
            )
        )

        created_parts = 24

        for i in range(created_parts):
            node.query("""INSERT INTO {name} VALUES ({n})""".format(name=name, n=i))
            try:
                node.query(
                    """ALTER TABLE {name} MOVE PARTITION tuple() TO VOLUME '{volume}' """.format(
                        name=name, volume=volume
                    )
                )
            except:
                """Ignore 'nothing to move'."""

        expected_disks = set(
            node.query(
                """
            SELECT disks FROM system.storage_policies ARRAY JOIN disks WHERE volume_name = '{volume_name}'
        """.format(
                    volume_name=volume
                )
            ).splitlines()
        )

        disks = get_used_disks_for_table(node, name)
        assert set(disks) <= expected_disks

        node.query("""OPTIMIZE TABLE {name} FINAL""".format(name=name))

        parts = get_used_parts_for_table(node, name)
        assert len(parts) == 1 if shall_work else created_parts

    finally:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def _get_prefer_not_to_merge_for_storage_policy(node, storage_policy):
    return list(
        map(
            int,
            node.query(
                "SELECT prefer_not_to_merge FROM system.storage_policies WHERE policy_name = '{}' ORDER BY volume_priority".format(
                    storage_policy
                )
            ).splitlines(),
        )
    )


def test_simple_merge_tree_merges_are_disabled(start_cluster):
    _check_merges_are_working(
        node1, "small_jbod_with_external_no_merges", "external", False
    )


def test_no_merges_in_configuration_allow_from_query_without_reload(start_cluster):
    try:
        name = "test_no_merges_in_configuration_allow_from_query_without_reload"
        policy = "small_jbod_with_external_no_merges"
        node1.restart_clickhouse(kill=True)
        assert _get_prefer_not_to_merge_for_storage_policy(node1, policy) == [0, 1]
        _check_merges_are_working(node1, policy, "external", False)

        _insert_merge_execute(
            node1,
            name,
            policy,
            2,
            ["SYSTEM START MERGES ON VOLUME {}.external".format(policy)],
            2,
            1,
        )
        assert _get_prefer_not_to_merge_for_storage_policy(node1, policy) == [0, 0]
        _check_merges_are_working(node1, policy, "external", True)

    finally:
        node1.query("SYSTEM STOP MERGES ON VOLUME {}.external".format(policy))


def test_no_merges_in_configuration_allow_from_query_with_reload(start_cluster):
    try:
        name = "test_no_merges_in_configuration_allow_from_query_with_reload"
        policy = "small_jbod_with_external_no_merges"
        node1.restart_clickhouse(kill=True)
        assert _get_prefer_not_to_merge_for_storage_policy(node1, policy) == [0, 1]
        _check_merges_are_working(node1, policy, "external", False)

        _insert_merge_execute(
            node1,
            name,
            policy,
            2,
            [
                "SYSTEM START MERGES ON VOLUME {}.external".format(policy),
                "SYSTEM RELOAD CONFIG",
            ],
            2,
            1,
        )
        assert _get_prefer_not_to_merge_for_storage_policy(node1, policy) == [0, 0]
        _check_merges_are_working(node1, policy, "external", True)

    finally:
        node1.query("SYSTEM STOP MERGES ON VOLUME {}.external".format(policy))


def test_no_merges_in_configuration_allow_from_query_with_reload_on_cluster(
    start_cluster,
):
    try:
        name = "test_no_merges_in_configuration_allow_from_query_with_reload"
        policy = "small_jbod_with_external_no_merges"
        node1.restart_clickhouse(kill=True)
        assert _get_prefer_not_to_merge_for_storage_policy(node1, policy) == [0, 1]
        _check_merges_are_working(node1, policy, "external", False)

        _insert_merge_execute(
            node1,
            name,
            policy,
            2,
            [
                "SYSTEM START MERGES ON CLUSTER test_cluster ON VOLUME {}.external".format(
                    policy
                ),
                "SYSTEM RELOAD CONFIG ON CLUSTER test_cluster",
            ],
            2,
            1,
        )
        assert _get_prefer_not_to_merge_for_storage_policy(node1, policy) == [0, 0]
        _check_merges_are_working(node1, policy, "external", True)

    finally:
        node1.query(
            "SYSTEM STOP MERGES ON CLUSTER test_cluster ON VOLUME {}.external".format(
                policy
            )
        )


def test_yes_merges_in_configuration_disallow_from_query_without_reload(start_cluster):
    try:
        name = "test_yes_merges_in_configuration_allow_from_query_without_reload"
        policy = "small_jbod_with_external"
        node1.restart_clickhouse(kill=True)
        assert _get_prefer_not_to_merge_for_storage_policy(node1, policy) == [0, 0]
        _check_merges_are_working(node1, policy, "external", True)

        _insert_merge_execute(
            node1,
            name,
            policy,
            2,
            [
                "SYSTEM STOP MERGES ON VOLUME {}.external".format(policy),
                "INSERT INTO {name} VALUES (2)".format(name=name),
            ],
            1,
            2,
        )
        assert _get_prefer_not_to_merge_for_storage_policy(node1, policy) == [0, 1]
        _check_merges_are_working(node1, policy, "external", False)

    finally:
        node1.query("SYSTEM START MERGES ON VOLUME {}.external".format(policy))


def test_yes_merges_in_configuration_disallow_from_query_with_reload(start_cluster):
    try:
        name = "test_yes_merges_in_configuration_allow_from_query_with_reload"
        policy = "small_jbod_with_external"
        node1.restart_clickhouse(kill=True)
        assert _get_prefer_not_to_merge_for_storage_policy(node1, policy) == [0, 0]
        _check_merges_are_working(node1, policy, "external", True)

        _insert_merge_execute(
            node1,
            name,
            policy,
            2,
            [
                "SYSTEM STOP MERGES ON VOLUME {}.external".format(policy),
                "INSERT INTO {name} VALUES (2)".format(name=name),
                "SYSTEM RELOAD CONFIG",
            ],
            1,
            2,
        )
        assert _get_prefer_not_to_merge_for_storage_policy(node1, policy) == [0, 1]
        _check_merges_are_working(node1, policy, "external", False)

    finally:
        node1.query("SYSTEM START MERGES ON VOLUME {}.external".format(policy))
