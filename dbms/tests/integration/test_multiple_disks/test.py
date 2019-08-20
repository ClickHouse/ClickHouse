import time
import pytest
import os
import random
import string

from helpers.cluster import ClickHouseCluster

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
def start_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_random_string(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))

def get_used_disks_for_table(node, table_name):
    return node.query("select disk_name from system.parts where table == '{}' order by modification_time".format(table_name)).strip().split('\n')

@pytest.mark.parametrize("name,engine", [
    ("mt_on_jbod","MergeTree()"),
    ("replicated_mt_on_jbod","ReplicatedMergeTree('/clickhouse/replicated_mt_on_jbod', '1')",),
])
def test_round_robin(start_cluster, name, engine):
    try:
        node1.query("""
            CREATE TABLE {name} (
                d UInt64
            ) ENGINE = {engine}
            ORDER BY d
            SETTINGS storage_policy_name='jbods_with_external'
        """.format(name=name, engine=engine))

        # first should go to the jbod1
        node1.query("insert into {} select * from numbers(10000)".format(name))
        used_disk = get_used_disks_for_table(node1, name)
        assert len(used_disk) == 1, 'More than one disk used for single insert'

        node1.query("insert into {} select * from numbers(10000, 10000)".format(name))
        used_disks = get_used_disks_for_table(node1, name)

        assert len(used_disks) == 2, 'Two disks should be used for two parts'
        assert used_disks[0] != used_disks[1], "Should write to different disks"

        node1.query("insert into {} select * from numbers(20000, 10000)".format(name))
        used_disks = get_used_disks_for_table(node1, name)

        # jbod1 -> jbod2 -> jbod1 -> jbod2 ... etc
        assert len(used_disks) == 3
        assert used_disks[0] != used_disks[1]
        assert used_disks[2] == used_disks[0]
    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))

@pytest.mark.parametrize("name,engine", [
    ("mt_with_huge_part","MergeTree()"),
    ("replicated_mt_with_huge_part","ReplicatedMergeTree('/clickhouse/replicated_mt_with_huge_part', '1')",),
])
def test_max_data_part_size(start_cluster, name, engine):
    try:
        node1.query("""
            CREATE TABLE {name} (
                s1 String
            ) ENGINE = {engine}
            ORDER BY tuple()
            SETTINGS storage_policy_name='jbods_with_external'
        """.format(name=name, engine=engine))
        data = [] # 10MB in total
        for i in range(10):
            data.append(get_random_string(1024 * 1024)) # 1MB row

        node1.query("INSERT INTO {} VALUES {}".format(name, ','.join(["('" + x + "')" for x in data])))
        used_disks = get_used_disks_for_table(node1, name)
        assert len(used_disks) == 1
        assert used_disks[0] == 'external'
    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))

@pytest.mark.parametrize("name,engine", [
    ("mt_with_overflow","MergeTree()"),
    ("replicated_mt_with_overflow","ReplicatedMergeTree('/clickhouse/replicated_mt_with_overflow', '1')",),
])
def test_jbod_overflow(start_cluster, name, engine):
    try:
        node1.query("""
            CREATE TABLE {name} (
                s1 String
            ) ENGINE = {engine}
            ORDER BY tuple()
            SETTINGS storage_policy_name='small_jbod_with_external'
        """.format(name=name, engine=engine))

        node1.query("SYSTEM STOP MERGES")

        # small jbod size is 40MB, so lets insert 5MB batch 7 times
        for i in range(7):
            data = [] # 5MB in total
            for i in range(5):
                data.append(get_random_string(1024 * 1024)) # 1MB row
            node1.query("INSERT INTO {} VALUES {}".format(name, ','.join(["('" + x + "')" for x in data])))

        used_disks = get_used_disks_for_table(node1, name)
        assert all(disk == 'jbod1' for disk in used_disks)

        # should go to the external disk (jbod is overflown)
        data = [] # 10MB in total
        for i in range(10):
            data.append(get_random_string(1024 * 1024)) # 1MB row

        node1.query("INSERT INTO {} VALUES {}".format(name, ','.join(["('" + x + "')" for x in data])))

        used_disks = get_used_disks_for_table(node1, name)

        print used_disks
        assert used_disks[-1] == 'external'

        node1.query("SYSTEM START MERGES")
        time.sleep(1)

        node1.query("OPTIMIZE TABLE {} FINAL".format(name))
        time.sleep(2)

        disks_for_merges = node1.query("SELECT disk_name FROM system.parts WHERE table == '{}' AND level >= 1 ORDER BY modification_time".format(name)).strip().split('\n')
        print disks_for_merges

        assert all(disk == 'external' for disk in disks_for_merges)

    finally:
        node1.query("DROP TABLE IF EXISTS {}".format(name))

@pytest.mark.parametrize("name,engine", [
    ("moving_mt","MergeTree()"),
    ("moving_replicated_mt","ReplicatedMergeTree('/clickhouse/moving_replicated_mt', '1')",),
])
def test_background_move(start_cluster, name, engine):
    try:
        node1.query("""
            CREATE TABLE {name} (
                s1 String
            ) ENGINE = {engine}
            ORDER BY tuple()
            SETTINGS storage_policy_name='moving_jbod_with_external'
        """.format(name=name, engine=engine))

        for i in range(5):
            data = [] # 5MB in total
            for i in range(5):
                data.append(get_random_string(1024 * 1024)) # 1MB row
            # small jbod size is 40MB, so lets insert 5MB batch 2 times (less than 70%)
            node1.query("INSERT INTO {} VALUES {}".format(name, ','.join(["('" + x + "')" for x in data])))


        time.sleep(5)
        used_disks = get_used_disks_for_table(node1, name)

        # Maximum two parts on jbod1
        assert sum(1 for x in used_disks if x == 'jbod1') <= 2

        # first (oldest) part was moved to external
        assert used_disks[0] == 'external'

    finally:
        node1.query("DROP TABLE IF EXISTS {name}".format(name=name))


#def test_default(start_cluster):
#    assert node1.query("create table node1_default_mt ( d UInt64 )\n ENGINE = MergeTree\n ORDER BY d") == ""
#    assert node1.query("select storage_policy from system.tables where name == 'node1_default_mt'") == "default\n"
#    assert node1.query("insert into node1_default_mt values (1)") == ""
#    assert node1.query("select disk_name from system.parts where table == 'node1_default_mt'") == "default\n"
#
#
#def test_move(start_cluster):
#    node2.query("create table move_mt ( d UInt64 )\n ENGINE = MergeTree\n ORDER BY d\n SETTINGS storage_policy_name='default_disk_with_external'")
#    node2.query("insert into move_mt values (1)")
#    assert node2.query("select disk_name from system.parts where table == 'move_mt'") == "default\n"
#
#    # move from default to external
#    node2.query("alter table move_mt move PART 'all_1_1_0' to disk 'external'")
#    assert node2.query("select disk_name from system.parts where table == 'move_mt'") == "external\n"
#    time.sleep(5)
#    # Check that it really moved
#    node2.query("detach table move_mt")
#    node2.query("attach table move_mt")
#    assert node2.query("select disk_name from system.parts where table == 'move_mt'") == "external\n"
#
#    # move back by volume small, that contains only 'default' disk
#    node2.query("alter table move_mt move PART 'all_1_1_0' to volume 'small'")
#    assert node2.query("select disk_name from system.parts where table == 'move_mt'") == "default\n"
#    time.sleep(5)
#    # Check that it really moved
#    node2.query("detach table move_mt")
#    node2.query("attach table move_mt")
#    assert node2.query("select disk_name from system.parts where table == 'move_mt'") == "default\n"
#
#
#def test_no_policy(start_cluster):
#    try:
#        node1.query("create table node1_move_mt ( d UInt64 )\n ENGINE = MergeTree\n ORDER BY d\n SETTINGS storage_policy_name='name_that_does_not_exists'")
#    except Exception as e:
#        assert str(e).strip().split("\n")[1].find("Unknown StoragePolicy name_that_does_not_exists") != -1


'''
## Test stand for multiple disks feature

Currently for manual tests, can be easily scripted to be the part of integration tests.

To run you need to have docker & docker-compose.

```
(Check makefile)
make run
make ch1_shell
 > clickhouse-client

make logs # Ctrl+C
make cleup
```

### basic

* allows to configure multiple disks & folumes & shemas
* clickhouse check that all disks are write-accessible
* clickhouse can create a table with provided storagepolicy

### one volume-one disk custom storagepolicy

* clickhouse puts data to correct folder when storagepolicy is used
* clickhouse can do merges / detach / attach / freeze on that folder

### one volume-multiple disks storagepolicy (JBOD scenario)

* clickhouse uses round-robin to place new parts
* clickhouse can do merges / detach / attach / freeze on that folder

### two volumes-one disk per volume (fast expensive / slow cheap storage)

* clickhouse uses round-robin to place new parts
* clickhouse can do merges / detach / attach / freeze on that folder
* clickhouse put parts to different volumes depending on part size

### use 'default' storagepolicy for tables created without storagepolicy provided.


# ReplicatedMergeTree

....

For all above:
clickhouse respect free space limitation setting.
ClickHouse writes important disk-related information to logs.

## Queries

```
CREATE TABLE table_with_storage_policy_default (id UInt64) Engine=MergeTree() ORDER BY (id);

select name, data_paths, storage_policy from system.tables where name='table_with_storage_policy_default';
"table_with_storage_policy_default","['/mainstorage/default/table_with_storage_policy_default/']","default"

    INSERT INTO table_with_storage_policy_default SELECT rand64() FROM numbers(100);
CREATE TABLE table_with_storage_policy_default_explicit           (id UInt64) Engine=MergeTree() ORDER BY (id) SETTINGS storage_table_with_storage_policy_name='default';
CREATE TABLE table_with_storage_policy_default_disk_with_external (id UInt64) Engine=MergeTree() ORDER BY (id) SETTINGS storage_table_with_storage_policy_name='default_disk_with_external';
CREATE TABLE table_with_storage_policy_jbod_with_external         (id UInt64) Engine=MergeTree() ORDER BY (id) SETTINGS storage_table_with_storage_policy_name='jbods_with_external';

CREATE TABLE replicated_table_with_storage_policy_default                    (id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}') ORDER BY (id);
CREATE TABLE replicated_table_with_storage_policy_default_explicit           (id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}') ORDER BY (id) SETTINGS storage_table_with_storage_policy_name='default';
CREATE TABLE replicated_table_with_storage_policy_default_disk_with_external (id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}') ORDER BY (id) SETTINGS storage_table_with_storage_policy_name='default_disk_with_external';
CREATE TABLE replicated_table_with_storage_policy_jbod_with_external         (id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}') ORDER BY (id) SETTINGS storage_table_with_storage_policy_name='jbods_with_external';
```


## Extra acceptance criterias

* hardlinks problems. Thouse stetements should be able to work properly (or give a proper feedback) on multidisk scenarios
  * ALTER TABLE ... UPDATE
  * ALTER TABLE ... TABLE
  * ALTER TABLE ... MODIFY COLUMN ...
  * ALTER TABLE ... CLEAR COLUMN
  * ALTER TABLE ... REPLACE PARTITION ...
* Maintainance - system tables show proper values:
  * system.parts
  * system.tables
  * system.part_log (target disk?)
* New system table
  * system.volumes
  * system.disks
  * system.storagepolicys
* chown / create needed disk folders in docker
'''
