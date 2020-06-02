import json
import pytest
import random
import re
import string
import threading
import time
from multiprocessing.dummy import Pool
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1',
            config_dir='configs',
            main_configs=['configs/logs_config.xml'],
            with_zookeeper=True,
            tmpfs=['/jbod1:size=40M', '/jbod2:size=40M', '/external:size=200M'],
            macros={"shard": 0, "replica": 1} )

node2 = cluster.add_instance('node2',
            config_dir='configs',
            main_configs=['configs/logs_config.xml'],
            with_zookeeper=True,
            tmpfs=['/jbod1:size=40M', '/jbod2:size=40M', '/external:size=200M'],
            macros={"shard": 0, "replica": 2} )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def get_random_string(length):
    symbols = bytes(string.ascii_uppercase + string.digits)
    result_list = bytearray([0])*length
    for i in range(length):
        result_list[i] = random.choice(symbols)
    return str(result_list)


def get_used_disks_for_table(node, table_name, partition=None):
    if partition is None:
        suffix = ""
    else:
        suffix = "and partition='{}'".format(partition)
    return node.query("""
        SELECT disk_name
        FROM system.parts
        WHERE table == '{name}' AND active=1 {suffix}
        ORDER BY modification_time
    """.format(name=table_name, suffix=suffix)).strip().split('\n')


@pytest.mark.parametrize("name,engine,alter", [
    ("mt_test_rule_with_invalid_destination","MergeTree()",0),
    ("replicated_mt_test_rule_with_invalid_destination","ReplicatedMergeTree('/clickhouse/replicated_test_rule_with_invalid_destination', '1')",0),
    ("mt_test_rule_with_invalid_destination","MergeTree()",1),
    ("replicated_mt_test_rule_with_invalid_destination","ReplicatedMergeTree('/clickhouse/replicated_test_rule_with_invalid_destination', '1')",1),
])
def test_rule_with_invalid_destination(started_cluster, name, engine, alter):
    try:
        def get_command(x, policy):
            x = x or ""
            if alter and x:
                return """
                    ALTER TABLE {name} MODIFY TTL {expression}
                """.format(expression=x, name=name)
            else:
                return """
                    CREATE TABLE {name} (
                        s1 String,
                        d1 DateTime
                    ) ENGINE = {engine}
                    ORDER BY tuple()
                    {expression}
                    SETTINGS storage_policy='{policy}'
                """.format(expression=x, name=name, engine=engine, policy=policy)

        if alter:
            node1.query(get_command(None, "small_jbod_with_external"))

        with pytest.raises(QueryRuntimeException):
            node1.query(get_command("TTL d1 TO DISK 'unknown'", "small_jbod_with_external"))

        node1.query("DROP TABLE IF EXISTS {}".format(name))

        if alter:
            node1.query(get_command(None, "small_jbod_with_external"))

        with pytest.raises(QueryRuntimeException):
            node1.query(get_command("TTL d1 TO VOLUME 'unknown'", "small_jbod_with_external"))

        node1.query("DROP TABLE IF EXISTS {}".format(name))

        if alter:
            node1.query(get_command(None, "only_jbod2"))

        with pytest.raises(QueryRuntimeException):
            node1.query(get_command("TTL d1 TO DISK 'jbod1'", "only_jbod2"))

        node1.query("DROP TABLE IF EXISTS {}".format(name))

        if alter:
            node1.query(get_command(None, "only_jbod2"))

        with pytest.raises(QueryRuntimeException):
            node1.query(get_command("TTL d1 TO VOLUME 'external'", "only_jbod2"))

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


@pytest.mark.parametrize("name,engine,positive", [
    ("mt_test_inserts_to_disk_do_not_work","MergeTree()",0),
    ("replicated_mt_test_inserts_to_disk_do_not_work","ReplicatedMergeTree('/clickhouse/replicated_test_inserts_to_disk_do_not_work', '1')",0),
    ("mt_test_inserts_to_disk_work","MergeTree()",1),
    ("replicated_mt_test_inserts_to_disk_work","ReplicatedMergeTree('/clickhouse/replicated_test_inserts_to_disk_work', '1')",1),
])
def test_inserts_to_disk_work(started_cluster, name, engine, positive):
    try:
        node1.query("""
            CREATE TABLE {name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            TTL d1 TO DISK 'external'
            SETTINGS storage_policy='small_jbod_with_external'
        """.format(name=name, engine=engine))

        data = [] # 10MB in total
        for i in range(10):
            data.append(("'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time.time()-1 if i > 0 or positive else time.time()+300))) # 1MB row

        node1.query("INSERT INTO {} (s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))
        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"external" if positive else "jbod1"}

        assert node1.query("SELECT count() FROM {name}".format(name=name)).strip() == "10"

    finally:
        try:
            node1.query("DROP TABLE IF EXISTS {}".format(name))
        except:
            pass


@pytest.mark.parametrize("name,engine,positive", [
    ("mt_test_moves_to_disk_do_not_work","MergeTree()",0),
    ("replicated_mt_test_moves_to_disk_do_not_work","ReplicatedMergeTree('/clickhouse/replicated_test_moves_to_disk_do_not_work', '1')",0),
    ("mt_test_moves_to_disk_work","MergeTree()",1),
    ("replicated_mt_test_moves_to_disk_work","ReplicatedMergeTree('/clickhouse/replicated_test_moves_to_disk_work', '1')",1),
])
def test_moves_to_disk_work(started_cluster, name, engine, positive):
    try:
        node1.query("""
            CREATE TABLE {name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            TTL d1 TO DISK 'external'
            SETTINGS storage_policy='small_jbod_with_external'
        """.format(name=name, engine=engine))

        wait_expire_1 = 12
        wait_expire_2 = 4
        time_1 = time.time() + wait_expire_1
        time_2 = time.time() + wait_expire_1 + wait_expire_2

        wait_expire_1_thread = threading.Thread(target=time.sleep, args=(wait_expire_1,))
        wait_expire_1_thread.start()

        data = [] # 10MB in total
        for i in range(10):
            data.append(("'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time_1 if i > 0 or positive else time_2))) # 1MB row

        node1.query("INSERT INTO {} (s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))
        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1"}

        wait_expire_1_thread.join()
        time.sleep(wait_expire_2/2)

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"external" if positive else "jbod1"}

        assert node1.query("SELECT count() FROM {name}".format(name=name)).strip() == "10"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


@pytest.mark.parametrize("name,engine", [
    ("mt_test_moves_to_volume_work","MergeTree()"),
    ("replicated_mt_test_moves_to_volume_work","ReplicatedMergeTree('/clickhouse/replicated_test_moves_to_volume_work', '1')"),
])
def test_moves_to_volume_work(started_cluster, name, engine):
    try:
        node1.query("""
            CREATE TABLE {name} (
                p1 Int64,
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION BY p1
            TTL d1 TO VOLUME 'external'
            SETTINGS storage_policy='jbods_with_external'
        """.format(name=name, engine=engine))

        wait_expire_1 = 10
        time_1 = time.time() + wait_expire_1

        wait_expire_1_thread = threading.Thread(target=time.sleep, args=(wait_expire_1,))
        wait_expire_1_thread.start()

        for p in range(2):
            data = [] # 10MB in total
            for i in range(5):
                data.append((str(p), "'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time_1))) # 1MB row

            node1.query("INSERT INTO {} (p1, s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {'jbod1', 'jbod2'}

        wait_expire_1_thread.join()
        time.sleep(1)

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"external"}

        assert node1.query("SELECT count() FROM {name}".format(name=name)).strip() == "10"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


@pytest.mark.parametrize("name,engine,positive", [
    ("mt_test_inserts_to_volume_do_not_work","MergeTree()",0),
    ("replicated_mt_test_inserts_to_volume_do_not_work","ReplicatedMergeTree('/clickhouse/replicated_test_inserts_to_volume_do_not_work', '1')",0),
    ("mt_test_inserts_to_volume_work","MergeTree()",1),
    ("replicated_mt_test_inserts_to_volume_work","ReplicatedMergeTree('/clickhouse/replicated_test_inserts_to_volume_work', '1')",1),
])
def test_inserts_to_volume_work(started_cluster, name, engine, positive):
    try:
        node1.query("""
            CREATE TABLE {name} (
                p1 Int64,
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION BY p1
            TTL d1 TO VOLUME 'external'
            SETTINGS storage_policy='small_jbod_with_external'
        """.format(name=name, engine=engine))

        node1.query("SYSTEM STOP MOVES {name}".format(name=name))

        for p in range(2):
            data = [] # 20MB in total
            for i in range(10):
                data.append((str(p), "'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time.time()-1 if i > 0 or positive else time.time()+300))) # 1MB row

            node1.query("INSERT INTO {} (p1, s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"external" if positive else "jbod1"}

        assert node1.query("SELECT count() FROM {name}".format(name=name)).strip() == "20"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


@pytest.mark.parametrize("name,engine", [
    ("mt_test_moves_to_disk_eventually_work","MergeTree()"),
    ("replicated_mt_test_moves_to_disk_eventually_work","ReplicatedMergeTree('/clickhouse/replicated_test_moves_to_disk_eventually_work', '1')"),
])
def test_moves_to_disk_eventually_work(started_cluster, name, engine):
    try:
        name_temp = name + "_temp"

        node1.query("""
            CREATE TABLE {name} (
                s1 String
            ) ENGINE = MergeTree()
            ORDER BY tuple()
            SETTINGS storage_policy='only_jbod2'
        """.format(name=name_temp))

        data = [] # 35MB in total
        for i in range(35):
            data.append(get_random_string(1024 * 1024)) # 1MB row

        node1.query("INSERT INTO {} VALUES {}".format(name_temp, ",".join(["('" + x + "')" for x in data])))
        used_disks = get_used_disks_for_table(node1, name_temp)
        assert set(used_disks) == {"jbod2"}

        node1.query("""
            CREATE TABLE {name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            TTL d1 TO DISK 'jbod2'
            SETTINGS storage_policy='jbod1_with_jbod2'
        """.format(name=name, engine=engine))

        data = [] # 10MB in total
        for i in range(10):
            data.append(("'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time.time()-1))) # 1MB row

        node1.query("INSERT INTO {} (s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))
        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1"}

        node1.query("DROP TABLE {}".format(name_temp))

        time.sleep(2)
        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod2"}

        assert node1.query("SELECT count() FROM {name}".format(name=name)).strip() == "10"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name_temp))
        node1.query("DROP TABLE IF EXISTS {}".format(name))


def test_replicated_download_ttl_info(started_cluster):
    name = "test_replicated_ttl_info"
    engine = "ReplicatedMergeTree('/clickhouse/test_replicated_download_ttl_info', '{replica}')"
    try:
        for i, node in enumerate((node1, node2), start=1):
            node.query("""
                CREATE TABLE {name} (
                    s1 String,
                    d1 DateTime
                ) ENGINE = {engine}
                ORDER BY tuple()
                TTL d1 TO DISK 'external'
                SETTINGS storage_policy='small_jbod_with_external'
            """.format(name=name, engine=engine))

        node1.query("SYSTEM STOP MOVES {}".format(name))

        node2.query("INSERT INTO {} (s1, d1) VALUES ('{}', toDateTime({}))".format(name, get_random_string(1024 * 1024), time.time()-100))

        assert set(get_used_disks_for_table(node2, name)) == {"external"}
        time.sleep(1)

        assert node1.query("SELECT count() FROM {}".format(name)).splitlines() == ["1"]
        assert set(get_used_disks_for_table(node1, name)) == {"external"}

    finally:
        for node in (node1, node2):
            try:
                node.query("DROP TABLE IF EXISTS {}".format(name))
            except:
                continue


@pytest.mark.parametrize("name,engine,positive", [
    ("mt_test_merges_to_disk_do_not_work","MergeTree()",0),
    ("replicated_mt_test_merges_to_disk_do_not_work","ReplicatedMergeTree('/clickhouse/replicated_test_merges_to_disk_do_not_work', '1')",0),
    ("mt_test_merges_to_disk_work","MergeTree()",1),
    ("replicated_mt_test_merges_to_disk_work","ReplicatedMergeTree('/clickhouse/replicated_test_merges_to_disk_work', '1')",1),
])
def test_merges_to_disk_work(started_cluster, name, engine, positive):
    try:
        node1.query("""
            CREATE TABLE {name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            TTL d1 TO DISK 'external'
            SETTINGS storage_policy='small_jbod_with_external'
        """.format(name=name, engine=engine))

        node1.query("SYSTEM STOP MERGES {}".format(name))
        node1.query("SYSTEM STOP MOVES {}".format(name))

        wait_expire_1 = 16
        wait_expire_2 = 4
        time_1 = time.time() + wait_expire_1
        time_2 = time.time() + wait_expire_1 + wait_expire_2

        wait_expire_1_thread = threading.Thread(target=time.sleep, args=(wait_expire_1,))
        wait_expire_1_thread.start()

        for _ in range(2):
            data = [] # 16MB in total
            for i in range(8):
                data.append(("'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time_1 if i > 0 or positive else time_2))) # 1MB row

            node1.query("INSERT INTO {} (s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1"}
        assert "2" == node1.query("SELECT count() FROM system.parts WHERE table = '{}' AND active = 1".format(name)).strip()

        wait_expire_1_thread.join()
        time.sleep(wait_expire_2/2)

        node1.query("SYSTEM START MERGES {}".format(name))
        node1.query("OPTIMIZE TABLE {}".format(name))

        time.sleep(1)
        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"external" if positive else "jbod1"}
        assert "1" == node1.query("SELECT count() FROM system.parts WHERE table = '{}' AND active = 1".format(name)).strip()

        assert node1.query("SELECT count() FROM {name}".format(name=name)).strip() == "16"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


@pytest.mark.parametrize("name,engine", [
    ("mt_test_merges_with_full_disk_work","MergeTree()"),
    ("replicated_mt_test_merges_with_full_disk_work","ReplicatedMergeTree('/clickhouse/replicated_test_merges_with_full_disk_work', '1')"),
])
def test_merges_with_full_disk_work(started_cluster, name, engine):
    try:
        name_temp = name + "_temp"

        node1.query("""
            CREATE TABLE {name} (
                s1 String
            ) ENGINE = MergeTree()
            ORDER BY tuple()
            SETTINGS storage_policy='only_jbod2'
        """.format(name=name_temp))

        data = [] # 35MB in total
        for i in range(35):
            data.append(get_random_string(1024 * 1024)) # 1MB row

        node1.query("INSERT INTO {} VALUES {}".format(name_temp, ",".join(["('" + x + "')" for x in data])))
        used_disks = get_used_disks_for_table(node1, name_temp)
        assert set(used_disks) == {"jbod2"}

        node1.query("""
            CREATE TABLE {name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            TTL d1 TO DISK 'jbod2'
            SETTINGS storage_policy='jbod1_with_jbod2'
        """.format(name=name, engine=engine))

        wait_expire_1 = 10
        time_1 = time.time() + wait_expire_1

        wait_expire_1_thread = threading.Thread(target=time.sleep, args=(wait_expire_1,))
        wait_expire_1_thread.start()

        for _ in range(2):
            data = [] # 12MB in total
            for i in range(6):
                data.append(("'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time_1))) # 1MB row
            node1.query("INSERT INTO {} (s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1"}
        assert "2" == node1.query("SELECT count() FROM system.parts WHERE table = '{}' AND active = 1".format(name)).strip()

        wait_expire_1_thread.join()

        node1.query("OPTIMIZE TABLE {}".format(name))
        time.sleep(1)

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1"} # Merged to the same disk against the rule.
        assert "1" == node1.query("SELECT count() FROM system.parts WHERE table = '{}' AND active = 1".format(name)).strip()

        assert node1.query("SELECT count() FROM {name}".format(name=name)).strip() == "12"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name_temp))
        node1.query("DROP TABLE IF EXISTS {}".format(name))


@pytest.mark.parametrize("name,engine,positive", [
    ("mt_test_moves_after_merges_do_not_work","MergeTree()",0),
    ("replicated_mt_test_moves_after_merges_do_not_work","ReplicatedMergeTree('/clickhouse/replicated_test_moves_after_merges_do_not_work', '1')",0),
    ("mt_test_moves_after_merges_work","MergeTree()",1),
    ("replicated_mt_test_moves_after_merges_work","ReplicatedMergeTree('/clickhouse/replicated_test_moves_after_merges_work', '1')",1),
])
def test_moves_after_merges_work(started_cluster, name, engine, positive):
    try:
        node1.query("""
            CREATE TABLE {name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            TTL d1 TO DISK 'external'
            SETTINGS storage_policy='small_jbod_with_external'
        """.format(name=name, engine=engine))

        wait_expire_1 = 16
        wait_expire_2 = 4
        time_1 = time.time() + wait_expire_1
        time_2 = time.time() + wait_expire_1 + wait_expire_2

        wait_expire_1_thread = threading.Thread(target=time.sleep, args=(wait_expire_1,))
        wait_expire_1_thread.start()

        for _ in range(2):
            data = [] # 14MB in total
            for i in range(7):
                data.append(("'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time_1 if i > 0 or positive else time_2))) # 1MB row

            node1.query("INSERT INTO {} (s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))

        node1.query("OPTIMIZE TABLE {}".format(name))
        time.sleep(1)

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1"}
        assert "1" == node1.query("SELECT count() FROM system.parts WHERE table = '{}' AND active = 1".format(name)).strip()

        wait_expire_1_thread.join()
        time.sleep(wait_expire_2/2)

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"external" if positive else "jbod1"}

        assert node1.query("SELECT count() FROM {name}".format(name=name)).strip() == "14"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


@pytest.mark.parametrize("name,engine,positive,bar", [
    ("mt_test_moves_after_alter_do_not_work","MergeTree()",0,"DELETE"),
    ("replicated_mt_test_moves_after_alter_do_not_work","ReplicatedMergeTree('/clickhouse/replicated_test_moves_after_alter_do_not_work', '1')",0,"DELETE"),
    ("mt_test_moves_after_alter_work","MergeTree()",1,"DELETE"),
    ("replicated_mt_test_moves_after_alter_work","ReplicatedMergeTree('/clickhouse/replicated_test_moves_after_alter_work', '1')",1,"DELETE"),
    ("mt_test_moves_after_alter_do_not_work","MergeTree()",0,"TO DISK 'external'"),
    ("replicated_mt_test_moves_after_alter_do_not_work","ReplicatedMergeTree('/clickhouse/replicated_test_moves_after_alter_do_not_work', '1')",0,"TO DISK 'external'"),
    ("mt_test_moves_after_alter_work","MergeTree()",1,"TO DISK 'external'"),
    ("replicated_mt_test_moves_after_alter_work","ReplicatedMergeTree('/clickhouse/replicated_test_moves_after_alter_work', '1')",1,"TO DISK 'external'"),
])
def test_ttls_do_not_work_after_alter(started_cluster, name, engine, positive, bar):
    try:
        node1.query("""
            CREATE TABLE {name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            TTL d1 TO DISK 'external'
            SETTINGS storage_policy='small_jbod_with_external'
        """.format(name=name, engine=engine))

        if positive:
            node1.query("""
                ALTER TABLE {name}
                    MODIFY TTL
                    d1 + INTERVAL 15 MINUTE {bar}
            """.format(name=name, bar=bar)) # That shall disable TTL.

        data = [] # 10MB in total
        for i in range(10):
            data.append(("'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time.time()-1))) # 1MB row
        node1.query("INSERT INTO {} (s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1" if positive else "external"}

        assert node1.query("SELECT count() FROM {name}".format(name=name)).strip() == "10"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


@pytest.mark.parametrize("name,engine", [
    ("mt_test_materialize_ttl_in_partition","MergeTree()"),
    ("replicated_mt_test_materialize_ttl_in_partition","ReplicatedMergeTree('/clickhouse/test_materialize_ttl_in_partition', '1')"),
])
def test_materialize_ttl_in_partition(started_cluster, name, engine):
    try:
        node1.query("""
            CREATE TABLE {name} (
                p1 Int8,
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY p1
            PARTITION BY p1
            SETTINGS storage_policy='small_jbod_with_external'
        """.format(name=name, engine=engine))

        data = [] # 5MB in total
        for i in range(5):
            data.append((str(i), "'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time.time()-1))) # 1MB row
        node1.query("INSERT INTO {} (p1, s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))

        time.sleep(0.5)

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1"}

        node1.query("""
                ALTER TABLE {name}
                    MODIFY TTL
                    d1 TO DISK 'external' SETTINGS materialize_ttl_after_modify = 0
            """.format(name=name))

        time.sleep(0.5)

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1"}

        node1.query("""
                ALTER TABLE {name}
                    MATERIALIZE TTL IN PARTITION 2
        """.format(name=name))

        node1.query("""
                ALTER TABLE {name}
                    MATERIALIZE TTL IN PARTITION 4
        """.format(name=name))

        time.sleep(0.5)

        used_disks_sets = []
        for i in range(len(data)):
            used_disks_sets.append(set(get_used_disks_for_table(node1, name, partition=i)))

        assert used_disks_sets == [{"jbod1"}, {"jbod1"}, {"external"}, {"jbod1"}, {"external"}]

        assert node1.query("SELECT count() FROM {name}".format(name=name)).strip() == str(len(data))

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


def start_thread(*args, **kwargs):
    thread = threading.Thread(*args, **kwargs)
    thread.start()
    return thread


@pytest.mark.parametrize("name,engine,positive", [
    ("mt_test_alter_multiple_ttls_positive", "MergeTree()", True),
    ("mt_replicated_test_alter_multiple_ttls_positive", "ReplicatedMergeTree('/clickhouse/replicated_test_alter_multiple_ttls_positive', '1')", True),
    ("mt_test_alter_multiple_ttls_negative", "MergeTree()", False),
    ("mt_replicated_test_alter_multiple_ttls_negative", "ReplicatedMergeTree('/clickhouse/replicated_test_alter_multiple_ttls_negative', '1')", False),
])
def test_alter_multiple_ttls(started_cluster, name, engine, positive):
    """Copyright 2019, Altinity LTD

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""
    """Check that when multiple TTL expressions are set
    and before any parts are inserted the TTL expressions
    are changed with ALTER command then all old
    TTL expressions are removed and the
    the parts are moved to the specified disk or volume or
    deleted if the new TTL expression is triggered
    and are not moved or deleted when it is not.
    """
    now = time.time()
    try:
        sleeps = { delay : start_thread(target=time.sleep, args=(delay,)) for delay in [16, 26] }

        node1.query("""
            CREATE TABLE {name} (
                p1 Int64,
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION BY p1
            TTL d1 + INTERVAL 34 SECOND TO DISK 'jbod2',
                d1 + INTERVAL 64 SECOND TO VOLUME 'external'
            SETTINGS storage_policy='jbods_with_external', merge_with_ttl_timeout=0
        """.format(name=name, engine=engine))

        node1.query("""
            ALTER TABLE {name} MODIFY
            TTL d1 + INTERVAL 0 SECOND TO DISK 'jbod2',
                d1 + INTERVAL 14 SECOND TO VOLUME 'external',
                d1 + INTERVAL 24 SECOND DELETE
        """.format(name=name))

        for p in range(3):
            data = [] # 6MB in total
            now = time.time()
            for i in range(2):
                p1 = p
                s1 = get_random_string(1024 * 1024) # 1MB
                d1 = now - 1 if i > 0 or positive else now + 300
                data.append("({}, '{}', toDateTime({}))".format(p1, s1, d1))
            node1.query("INSERT INTO {name} (p1, s1, d1) VALUES {values}".format(name=name, values=",".join(data)))

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod2"} if positive else {"jbod1", "jbod2"}

        assert node1.query("SELECT count() FROM {name}".format(name=name)).splitlines() == ["6"]

        sleeps[16].join()

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"external"} if positive else {"jbod1", "jbod2"}

        assert node1.query("SELECT count() FROM {name}".format(name=name)).splitlines() == ["6"]

        sleeps[26].join()

        node1.query("OPTIMIZE TABLE {name} FINAL".format(name=name))

        assert node1.query("SELECT count() FROM {name}".format(name=name)).splitlines() == ["0"] if positive else ["3"]

    finally:
        node1.query("DROP TABLE IF EXISTS {name}".format(name=name))


@pytest.mark.parametrize("name,engine", [
    ("concurrently_altering_ttl_mt","MergeTree()"),
    ("concurrently_altering_ttl_replicated_mt","ReplicatedMergeTree('/clickhouse/concurrently_altering_ttl_replicated_mt', '1')",),
])
def test_concurrent_alter_with_ttl_move(started_cluster, name, engine):
    try:
        node1.query("""
            CREATE TABLE {name} (
                EventDate Date,
                number UInt64
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION BY toYYYYMM(EventDate)
            SETTINGS storage_policy='jbods_with_external'
        """.format(name=name, engine=engine))

        values = list({ random.randint(1, 1000000) for _ in range(0, 1000) })

        def insert(num):
            for i in range(num):
                day = random.randint(11, 30)
                value = values.pop()
                month = '0' + str(random.choice([3, 4]))
                node1.query("INSERT INTO {} VALUES(toDate('2019-{m}-{d}'), {v})".format(name, m=month, d=day, v=value))

        def alter_move(num):
            def produce_alter_move(node, name):
                move_type = random.choice(["PART", "PARTITION"])
                if move_type == "PART":
                    for _ in range(10):
                        try:
                            parts = node1.query("SELECT name from system.parts where table = '{}' and active = 1".format(name)).strip().split('\n')
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
                    node1.query("ALTER TABLE {} MOVE {mt} {mp} TO {md} {mv}".format(
                        name, mt=move_type, mp=move_part, md=move_disk, mv=move_volume))
                except QueryRuntimeException:
                    pass

            for i in range(num):
                produce_alter_move(node1, name)

        def alter_update(num):
            for i in range(num):
                node1.query("ALTER TABLE {} UPDATE number = number + 1 WHERE 1".format(name))

        def alter_modify_ttl(num):
            for i in range(num):
                ttls = []
                for j in range(random.randint(1, 10)):
                    what = random.choice(["TO VOLUME 'main'", "TO VOLUME 'external'", "TO DISK 'jbod1'", "TO DISK 'jbod2'", "TO DISK 'external'"])
                    when = "now()+{}".format(random.randint(-1, 5))
                    ttls.append("{} {}".format(when, what))
                try:
                    node1.query("ALTER TABLE {} MODIFY TTL {}".format(name, ", ".join(ttls)))
                except QueryRuntimeException:
                    pass

        def optimize_table(num):
            for i in range(num):
                node1.query("OPTIMIZE TABLE {} FINAL".format(name))

        p = Pool(15)
        tasks = []
        for i in range(5):
            tasks.append(p.apply_async(insert, (100,)))
            tasks.append(p.apply_async(alter_move, (100,)))
            tasks.append(p.apply_async(alter_update, (100,)))
            tasks.append(p.apply_async(alter_modify_ttl, (100,)))
            tasks.append(p.apply_async(optimize_table, (100,)))

        for task in tasks:
            task.get(timeout=120)

        assert node1.query("SELECT 1") == "1\n"
        assert node1.query("SELECT COUNT() FROM {}".format(name)) == "500\n"
    finally:
        node1.query("DROP TABLE IF EXISTS {name}".format(name=name))

@pytest.mark.skip(reason="Flacky test")
@pytest.mark.parametrize("name,positive", [
    ("test_double_move_while_select_negative", 0),
    ("test_double_move_while_select_positive", 1),
])
def test_double_move_while_select(started_cluster, name, positive):
    try:
        node1.query("""
            CREATE TABLE {name} (
                n Int64,
                s String
            ) ENGINE = MergeTree
            ORDER BY tuple()
            PARTITION BY n
            SETTINGS storage_policy='small_jbod_with_external'
        """.format(name=name))

        node1.query("INSERT INTO {name} VALUES (1, '{string}')".format(name=name, string=get_random_string(10 * 1024 * 1024)))

        parts = node1.query("SELECT name FROM system.parts WHERE table = '{name}' AND active = 1".format(name=name)).splitlines()
        assert len(parts) == 1

        node1.query("ALTER TABLE {name} MOVE PART '{part}' TO DISK 'external'".format(name=name, part=parts[0]))

        def long_select():
            if positive:
                node1.query("SELECT sleep(3), sleep(2), sleep(1), n FROM {name}".format(name=name))

        thread = threading.Thread(target=long_select)
        thread.start()

        time.sleep(1)

        node1.query("ALTER TABLE {name} MOVE PART '{part}' TO DISK 'jbod1'".format(name=name, part=parts[0]))

        # Fill jbod1 to force ClickHouse to make move of partition 1 to external.
        node1.query("INSERT INTO {name} VALUES (2, '{string}')".format(name=name, string=get_random_string(9 * 1024 * 1024)))
        node1.query("INSERT INTO {name} VALUES (3, '{string}')".format(name=name, string=get_random_string(9 * 1024 * 1024)))
        node1.query("INSERT INTO {name} VALUES (4, '{string}')".format(name=name, string=get_random_string(9 * 1024 * 1024)))

        time.sleep(1)

        # If SELECT locked old part on external, move shall fail.
        assert node1.query("SELECT disk_name FROM system.parts WHERE table = '{name}' AND active = 1 AND name = '{part}'"
                .format(name=name, part=parts[0])).splitlines() == ["jbod1" if positive else "external"]

        thread.join()

        assert node1.query("SELECT n FROM {name} ORDER BY n".format(name=name)).splitlines() == ["1", "2", "3", "4"]

    finally:
        node1.query("DROP TABLE IF EXISTS {name}".format(name=name))
