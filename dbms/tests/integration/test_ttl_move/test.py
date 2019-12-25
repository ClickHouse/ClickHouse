from __future__ import print_function

import json
import pytest
import random
import re
import string
import sys
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
            data.append(("'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time.time()-1 if i > 0 or positive else time.time()+300))) # 1MB row

        node1.query("INSERT INTO {} (s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))
        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"external" if positive else "jbod1"}

        assert node1.query("SELECT count() FROM {name}".format(name=name)).strip() == "10"

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

        wait_expire_1 = 6
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

        wait_expire_1 = 10
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

        wait_expire_1 = 10
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


@pytest.mark.parametrize("name,engine,positive", [
    ("mt_test_moves_after_merges_do_not_work","MergeTree()",0),
    ("replicated_mt_test_moves_after_merges_do_not_work","ReplicatedMergeTree('/clickhouse/replicated_test_moves_after_merges_do_not_work', '1')",0),
    ("mt_test_moves_after_merges_work","MergeTree()",1),
    ("replicated_mt_test_moves_after_merges_work","ReplicatedMergeTree('/clickhouse/replicated_test_moves_after_merges_work', '1')",1),
])
def test_ttls_do_not_work_after_alter(started_cluster, name, engine, positive):
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
                    d1 + INTERVAL 15 MINUTE
            """.format(name=name)) # That shall disable TTL.

        data = [] # 10MB in total
        for i in range(10):
            data.append(("'{}'".format(get_random_string(1024 * 1024)), "toDateTime({})".format(time.time()-1))) # 1MB row
        node1.query("INSERT INTO {} (s1, d1) VALUES {}".format(name, ",".join(["(" + ",".join(x) + ")" for x in data])))

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1" if positive else "external"}

        assert node1.query("SELECT count() FROM {name}".format(name=name)).strip() == "10"

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))


@pytest.mark.parametrize("name,engine,positive", [
    ("mt_test_alter_with_merge_do_not_work","MergeTree()",0),
    ("replicated_mt_test_alter_with_merge_do_not_work","ReplicatedMergeTree('/clickhouse/replicated_test_alter_with_merge_do_not_work', '1')",0),
    ("mt_test_alter_with_merge_work","MergeTree()",1),
    ("replicated_mt_test_alter_with_merge_work","ReplicatedMergeTree('/clickhouse/replicated_test_alter_with_merge_work', '1')",1),
])
def test_alter_with_merge_work(started_cluster, name, engine, positive):
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
    """Check that TTL expressions are re-evaluated for
    existing parts after ALTER command changes TTL expressions
    and parts are merged.
    """
    try:
        node1.query("""
            CREATE TABLE {name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            TTL d1 + INTERVAL 3000 SECOND TO DISK 'jbod2',
                d1 + INTERVAL 6000 SECOND TO VOLUME 'external'
            SETTINGS storage_policy='jbods_with_external', merge_with_ttl_timeout=0
        """.format(name=name, engine=engine))

        node1.query("SYSTEM STOP MOVES {name}".format(name=name))
        node1.query("SYSTEM STOP MERGES {name}".format(name=name))

        for p in range(3):
            data = [] # 6MB in total
            now = time.time()
            for i in range(2):
                s1 = get_random_string(1024 * 1024) # 1MB
                d1 = now - 1 if positive else now + 300
                data.append("('{}', toDateTime({}))".format(s1, d1))
            values = ",".join(data)
            node1.query("INSERT INTO {name} (s1, d1) VALUES {values}".format(name=name, values=values))

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) == {"jbod1", "jbod2"}

        node1.query("SELECT count() FROM {name}".format(name=name)).splitlines() == ["6"]

        node1.query("""
            ALTER TABLE {name} MODIFY
            TTL d1 + INTERVAL 0 SECOND TO DISK 'jbod2', 
                d1 + INTERVAL 5 SECOND TO VOLUME 'external',
                d1 + INTERVAL 10 SECOND DELETE
        """.format(name=name))

        node1.query("SYSTEM START MOVES {name}".format(name=name))
        node1.query("SYSTEM START MERGES {name}".format(name=name))

        node1.query("OPTIMIZE TABLE {name}".format(name=name))

        time.sleep(1)

        assert node1.query("SELECT count() FROM system.parts WHERE table = '{name}' AND active = 1".format(name=name)).splitlines() == ["1"]

        time.sleep(6)

        print(node1.exec_in_container("bash -c 'cat /jbod*/data/default/{name}/*/ttl.txt'".format(name=name)), file=sys.stderr)

        used_disks = get_used_disks_for_table(node1, name)
        assert set(used_disks) in (({"external"},) if positive else ({"jbod1"}, {"jbod2"}))

        assert node1.query("SELECT count() FROM {name}").splitlines() == ["6"]

        time.sleep(5)

        node1.query("OPTIMIZE TABLE {name} FINAL".format(name=name))

        assert node1.query("SELECT count() FROM {name}".format(name=name)).splitlines() == ["0" if positive else "6"]

    finally:
        node1.query("DROP TABLE IF EXISTS {name}".format(name=name))
