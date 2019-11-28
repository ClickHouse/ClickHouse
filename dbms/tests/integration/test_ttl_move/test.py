import json
import pytest
import random
import re
import string
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
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))


def get_used_disks_for_table(node, table_name):
    return node.query("select disk_name from system.parts where table == '{}' and active=1 order by modification_time".format(table_name)).strip().split('\n')


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
            data.append(("'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time.time()-2 if i > 0 or positive else time.time()+2))) # 1MB row

        node1.query("INSERT INTO {} (s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))
        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"external" if positive else "jbod1"}

        assert node1.query("SELECT count() FROM {}".format(name=name)).strip() == "10"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


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

        data = [] # 10MB in total
        for i in range(10):
            data.append(("'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time.time()+2 if i > 0 or positive else time.time()+6))) # 1MB row

        node1.query("INSERT INTO {} (s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))
        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1"}

        time.sleep(4)
        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"external" if positive else "jbod1"}

        assert node1.query("SELECT count() FROM {}".format(name=name)).strip() == "10"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


@pytest.mark.parametrize("name,engine", [
    ("mt_test_moves_to_volume_work","MergeTree()"),
    ("replicated_mt_test_moves_to_volume_work","ReplicatedMergeTree('/clickhouse/replicated_test_moves_to_volume_work', '1')",),
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
            TTL d1 TO VOLUME 'main'
            SETTINGS storage_policy='external_with_jbods'
        """.format(name=name, engine=engine))

        for p in range(2):
            data = [] # 20MB in total
            for i in range(10):
                data.append((p, "'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time.time()+2))) # 1MB row

            node1.query("INSERT INTO {} (p1, s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {'jbod1', 'jbod2'}

        time.sleep(4)
        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"external"}

        assert node1.query("SELECT count() FROM {}".format(name=name)).strip() == "20"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


@pytest.mark.parametrize("name,engine", [
    ("mt_test_inserts_to_volume_work","MergeTree()"),
    ("replicated_mt_test_inserts_to_volume_work","ReplicatedMergeTree('/clickhouse/replicated_test_inserts_to_volume_work', '1')",),
])
def test_inserts_to_volume_work(started_cluster, name, engine):
    try:
        node1.query("""
            CREATE TABLE {name} (
                p1 Int64,
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION BY p1
            TTL d1 TO VOLUME 'main'
            SETTINGS storage_policy='external_with_jbods'
        """.format(name=name, engine=engine))

        for p in range(2):
            data = [] # 20MB in total
            for i in range(10):
                data.append((p, "'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time.time()-2))) # 1MB row

            node1.query("INSERT INTO {} (p1, s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"external"}

        assert node1.query("SELECT count() FROM {}".format(name=name)).strip() == "20"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


@pytest.mark.parametrize("name,engine", [
    ("mt_test_moves_to_disk_eventually_work","MergeTree()"),
    ("replicated_mt_test_moves_to_disk_eventually_work","ReplicatedMergeTree('/clickhouse/replicated_test_moves_to_disk_eventually_work', '1')",),
])
def test_moves_to_disk_eventually_work(started_cluster, name, engine):
    try:
        name_temp = name + "_temp"

        node1.query("""
            CREATE TABLE {name} (
                s1 String
            ) ENGINE = {engine}
            ORDER BY tuple()
            SETTINGS storage_policy='only_jbod2'
        """.format(name=name_temp, engine=engine))

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
            data.append(("'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time.time()-2))) # 1MB row

        node1.query("INSERT INTO {} (s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))
        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1"}

        node1.query("DROP TABLE {}".format(name_temp))

        time.sleep(2)
        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod2"}

        assert node1.query("SELECT count() FROM {}".format(name=name)).strip() == "10"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name_temp))
        node1.query("DROP TABLE IF EXISTS {}".format(name))


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

        for _ in range(2):
            data = [] # 16MB in total
            for i in range(8):
                data.append(("'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time.time()+2 if i > 0 or positive else time.time()+7))) # 1MB row

            node1.query("INSERT INTO {} (s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))

        time.sleep(4)
        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1"}
        assert "2" == node1.query("SELECT count() FROM system.parts WHERE table = '{}' AND active = 1".format(name)).strip()

        node1.query("SYSTEM START MERGES {}".format(name))
        node1.query("OPTIMIZE TABLE {}".format(name))

        time.sleep(1)
        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"external" if positive else "jbod1"}
        assert "1" == node1.query("SELECT count() FROM system.parts WHERE table = '{}' AND active = 1".format(name)).strip()

        assert node1.query("SELECT count() FROM {}".format(name=name)).strip() == "16"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


@pytest.mark.parametrize("name,engine", [
    ("mt_test_merges_to_full_disk_work","MergeTree()"),
    ("replicated_mt_test_merges_to_full_disk_work","ReplicatedMergeTree('/clickhouse/replicated_test_merges_to_full_disk_work', '1')",),
])
def test_merges_to_full_disk_work(started_cluster, name, engine):
    try:
        name_temp = name + "_temp"

        node1.query("""
            CREATE TABLE {name} (
                s1 String
            ) ENGINE = {engine}
            ORDER BY tuple()
            SETTINGS storage_policy='only_jbod2'
        """.format(name=name_temp, engine=engine))

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
            TTL d1 TO DISK 'external'
            SETTINGS storage_policy='small_jbod_with_external'
        """.format(name=name, engine=engine))

        node1.query("SYSTEM STOP MOVES {}".format(name))

        for _ in range(2):
            data = [] # 16MB in total
            for i in range(8):
                data.append(("'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time.time()+2))) # 1MB row

            node1.query("INSERT INTO {} (s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1"}
        assert "2" == node1.query("SELECT count() FROM system.parts WHERE table = '{}' AND active = 1".format(name)).strip()

        time.sleep(4)
        node1.query("OPTIMIZE TABLE {}".format(name))

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1"} # Merged to the same disk against the rule.
        assert "1" == node1.query("SELECT count() FROM system.parts WHERE table = '{}' AND active = 1".format(name)).strip()

        assert node1.query("SELECT count() FROM {}".format(name=name)).strip() == "16"

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

        for _ in range(2):
            data = [] # 16MB in total
            for i in range(8):
                data.append(("'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time.time()+2 if i > 0 or positive else time.time()+6))) # 1MB row

            node1.query("INSERT INTO {} (s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))

        node1.query("OPTIMIZE TABLE {}".format(name))
        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1"}
        assert "1" == node1.query("SELECT count() FROM system.parts WHERE table = '{}' AND active = 1".format(name)).strip()

        time.sleep(4)

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"external" if positive else "jbod1"}

        assert node1.query("SELECT count() FROM {}".format(name=name)).strip() == "16"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))
