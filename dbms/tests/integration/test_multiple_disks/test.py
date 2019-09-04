import time
import pytest
import random
import string
from multiprocessing.dummy import Pool
from helpers.client import QueryRuntimeException

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
    return node.query("select disk_name from system.parts where table == '{}' and active=1 order by modification_time".format(table_name)).strip().split('\n')

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

        assert used_disks[-1] == 'external'

        node1.query("SYSTEM START MERGES")
        time.sleep(1)

        node1.query("OPTIMIZE TABLE {} FINAL".format(name))
        time.sleep(2)

        disks_for_merges = node1.query("SELECT disk_name FROM system.parts WHERE table == '{}' AND level >= 1 and active = 1 ORDER BY modification_time".format(name)).strip().split('\n')

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
            # small jbod size is 40MB, so lets insert 5MB batch 5 times
            node1.query("INSERT INTO {} VALUES {}".format(name, ','.join(["('" + x + "')" for x in data])))


        used_disks = get_used_disks_for_table(node1, name)

        retry = 20
        i = 0
        while not sum(1 for x in used_disks if x == 'jbod1') <= 2 and i < retry:
            time.sleep(0.5)
            used_disks = get_used_disks_for_table(node1, name)
            i += 1

        assert sum(1 for x in used_disks if x == 'jbod1') <= 2

        # first (oldest) part was moved to external
        assert used_disks[0] == 'external'

        path = node1.query("SELECT path_on_disk FROM system.part_log WHERE table = '{}' AND event_type='MovePart' ORDER BY event_time LIMIT 1".format(name))

        # first (oldest) part was moved to external
        assert path.startswith("/external")

    finally:
        node1.query("DROP TABLE IF EXISTS {name}".format(name=name))

@pytest.mark.parametrize("name,engine", [
    ("stopped_moving_mt","MergeTree()"),
    ("stopped_moving_replicated_mt","ReplicatedMergeTree('/clickhouse/stopped_moving_replicated_mt', '1')",),
])
def test_start_stop_moves(start_cluster, name, engine):
    try:
        node1.query("""
            CREATE TABLE {name} (
                s1 String
            ) ENGINE = {engine}
            ORDER BY tuple()
            SETTINGS storage_policy_name='moving_jbod_with_external'
        """.format(name=name, engine=engine))

        node1.query("INSERT INTO {} VALUES ('HELLO')".format(name))
        node1.query("INSERT INTO {} VALUES ('WORLD')".format(name))

        used_disks = get_used_disks_for_table(node1, name)
        assert all(d == "jbod1" for d in used_disks), "All writes shoud go to jbods"

        first_part = node1.query("SELECT name FROM system.parts WHERE table = '{}' and active = 1 ORDER BY modification_time LIMIT 1".format(name)).strip()

        node1.query("SYSTEM STOP MOVES")

        with pytest.raises(QueryRuntimeException):
            node1.query("ALTER TABLE {} MOVE PART '{}' TO VOLUME 'external'".format(name, first_part))

        used_disks = get_used_disks_for_table(node1, name)
        assert all(d == "jbod1" for d in used_disks), "Blocked moves doesn't actually move something"

        node1.query("SYSTEM START MOVES")

        node1.query("ALTER TABLE {} MOVE PART '{}' TO VOLUME 'external'".format(name, first_part))

        disk = node1.query("SELECT disk_name FROM system.parts WHERE table = '{}' and name = '{}' and active = 1".format(name, first_part)).strip()

        assert disk == "external"

        node1.query("TRUNCATE TABLE {}".format(name))

        node1.query("SYSTEM STOP MOVES {}".format(name))
        node1.query("SYSTEM STOP MERGES {}".format(name))

        for i in range(5):
            data = [] # 5MB in total
            for i in range(5):
                data.append(get_random_string(1024 * 1024)) # 1MB row
            # jbod size is 40MB, so lets insert 5MB batch 7 times
            node1.query("INSERT INTO {} VALUES {}".format(name, ','.join(["('" + x + "')" for x in data])))

        used_disks = get_used_disks_for_table(node1, name)

        retry = 5
        i = 0
        while not sum(1 for x in used_disks if x == 'jbod1') <= 2 and i < retry:
            time.sleep(0.1)
            used_disks = get_used_disks_for_table(node1, name)
            i += 1

        # first (oldest) part doesn't move anywhere
        assert used_disks[0] == 'jbod1'

        node1.query("SYSTEM START MOVES {}".format(name))
        node1.query("SYSTEM START MERGES {}".format(name))

        # wait sometime until background backoff finishes
        retry = 30
        i = 0
        while not sum(1 for x in used_disks if x == 'jbod1') <= 2 and i < retry:
            time.sleep(1)
            used_disks = get_used_disks_for_table(node1, name)
            i += 1

        assert sum(1 for x in used_disks if x == 'jbod1') <= 2

        # first (oldest) part moved to external
        assert used_disks[0] == 'external'

    finally:
        node1.query("DROP TABLE IF EXISTS {name}".format(name=name))

def get_path_for_part_from_part_log(node, table, part_name):
    node.query("SYSTEM FLUSH LOGS")
    path = node.query("SELECT path_on_disk FROM system.part_log WHERE table = '{}' and part_name = '{}' ORDER BY event_time DESC LIMIT 1".format(table, part_name))
    return path.strip()

def get_paths_for_partition_from_part_log(node, table, partition_id):
    node.query("SYSTEM FLUSH LOGS")
    paths = node.query("SELECT path_on_disk FROM system.part_log WHERE table = '{}' and partition_id = '{}' ORDER BY event_time DESC".format(table, partition_id))
    return paths.strip().split('\n')

@pytest.mark.parametrize("name,engine", [
    ("altering_mt","MergeTree()"),
    ("altering_replicated_mt","ReplicatedMergeTree('/clickhouse/altering_replicated_mt', '1')",),
])
def test_alter_move(start_cluster, name, engine):
    try:
        node1.query("""
            CREATE TABLE {name} (
                EventDate Date,
                number UInt64
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION BY toYYYYMM(EventDate)
            SETTINGS storage_policy_name='jbods_with_external'
        """.format(name=name, engine=engine))

        node1.query("INSERT INTO {} VALUES(toDate('2019-03-15'), 65)".format(name))
        node1.query("INSERT INTO {} VALUES(toDate('2019-03-16'), 66)".format(name))
        node1.query("INSERT INTO {} VALUES(toDate('2019-04-10'), 42)".format(name))
        node1.query("INSERT INTO {} VALUES(toDate('2019-04-11'), 43)".format(name))
        used_disks = get_used_disks_for_table(node1, name)
        assert all(d.startswith("jbod") for d in used_disks), "All writes shoud go to jbods"

        first_part = node1.query("SELECT name FROM system.parts WHERE table = '{}' and active = 1 ORDER BY modification_time LIMIT 1".format(name)).strip()

        time.sleep(1)
        node1.query("ALTER TABLE {} MOVE PART '{}' TO VOLUME 'external'".format(name, first_part))
        disk = node1.query("SELECT disk_name FROM system.parts WHERE table = '{}' and name = '{}' and active = 1".format(name, first_part)).strip()
        assert disk == 'external'
        assert get_path_for_part_from_part_log(node1, name, first_part).startswith("/external")


        time.sleep(1)
        node1.query("ALTER TABLE {} MOVE PART '{}' TO DISK 'jbod1'".format(name, first_part))
        disk = node1.query("SELECT disk_name FROM system.parts WHERE table = '{}' and name = '{}' and active = 1".format(name, first_part)).strip()
        assert disk == 'jbod1'
        assert get_path_for_part_from_part_log(node1, name, first_part).startswith("/jbod1")

        time.sleep(1)
        node1.query("ALTER TABLE {} MOVE PARTITION 201904 TO VOLUME 'external'".format(name))
        disks = node1.query("SELECT disk_name FROM system.parts WHERE table = '{}' and partition = '201904' and active = 1".format(name)).strip().split('\n')
        assert len(disks) == 2
        assert all(d == "external" for d in disks)
        assert all(path.startswith("/external") for path in get_paths_for_partition_from_part_log(node1, name, '201904')[:2])


        time.sleep(1)
        node1.query("ALTER TABLE {} MOVE PARTITION 201904 TO DISK 'jbod2'".format(name))
        disks = node1.query("SELECT disk_name FROM system.parts WHERE table = '{}' and partition = '201904' and active = 1".format(name)).strip().split('\n')
        assert len(disks) == 2
        assert all(d == "jbod2" for d in disks)
        assert all(path.startswith("/jbod2") for path in get_paths_for_partition_from_part_log(node1, name, '201904')[:2])

        assert node1.query("SELECT COUNT() FROM {}".format(name)) == "4\n"

    finally:
        node1.query("DROP TABLE IF EXISTS {name}".format(name=name))

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
    except QueryRuntimeException as ex:
        pass


@pytest.mark.parametrize("name,engine", [
    ("concurrently_altering_mt","MergeTree()"),
    ("concurrently_altering_replicated_mt","ReplicatedMergeTree('/clickhouse/concurrently_altering_replicated_mt', '1')",),
])
def test_concurrent_alter_move(start_cluster, name, engine):
    try:
        node1.query("""
            CREATE TABLE {name} (
                EventDate Date,
                number UInt64
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION BY toYYYYMM(EventDate)
            SETTINGS storage_policy_name='jbods_with_external'
        """.format(name=name, engine=engine))

        def insert(num):
            for i in range(num):
                day = random.randint(11, 30)
                value = random.randint(1, 1000000)
                month = '0' + str(random.choice([3, 4]))
                node1.query("INSERT INTO {} VALUES(toDate('2019-{m}-{d}'), {v})".format(name, m=month, d=day, v=value))

        def alter_move(num):
            for i in range(num):
                produce_alter_move(node1, name)

        def alter_update(num):
            for i in range(num):
                node1.query("ALTER TABLE {} UPDATE number = number + 1 WHERE 1".format(name))

        def optimize_table(num):
            for i in range(num):
                node1.query("OPTIMIZE TABLE {} FINAL".format(name))

        p = Pool(15)
        tasks = []
        for i in range(5):
            tasks.append(p.apply_async(insert, (100,)))
            tasks.append(p.apply_async(alter_move, (100,)))
            tasks.append(p.apply_async(alter_update, (100,)))
            tasks.append(p.apply_async(optimize_table, (100,)))

        for task in tasks:
            task.get(timeout=60)

        assert node1.query("SELECT 1") == "1\n"
        assert node1.query("SELECT COUNT() FROM {}".format(name)) == "500\n"
    finally:
        node1.query("DROP TABLE IF EXISTS {name}".format(name=name))

@pytest.mark.parametrize("name,engine", [
    ("concurrently_dropping_mt","MergeTree()"),
    ("concurrently_dropping_replicated_mt","ReplicatedMergeTree('/clickhouse/concurrently_dropping_replicated_mt', '1')",),
])
def test_concurrent_alter_move_and_drop(start_cluster, name, engine):
    try:
        node1.query("""
            CREATE TABLE {name} (
                EventDate Date,
                number UInt64
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION BY toYYYYMM(EventDate)
            SETTINGS storage_policy_name='jbods_with_external'
        """.format(name=name, engine=engine))

        def insert(num):
            for i in range(num):
                day = random.randint(11, 30)
                value = random.randint(1, 1000000)
                month = '0' + str(random.choice([3, 4]))
                node1.query("INSERT INTO {} VALUES(toDate('2019-{m}-{d}'), {v})".format(name, m=month, d=day, v=value))

        def alter_move(num):
            for i in range(num):
                produce_alter_move(node1, name)

        def alter_drop(num):
            for i in range(num):
                partition = random.choice([201903, 201904])
                drach = random.choice(["drop", "detach"])
                node1.query("ALTER TABLE {} {} PARTITION {}".format(name, drach, partition))

        insert(100)
        p = Pool(15)
        tasks = []
        for i in range(5):
            tasks.append(p.apply_async(insert, (100,)))
            tasks.append(p.apply_async(alter_move, (100,)))
            tasks.append(p.apply_async(alter_drop, (100,)))

        for task in tasks:
            task.get(timeout=60)

        assert node1.query("SELECT 1") == "1\n"

    finally:
        node1.query("DROP TABLE IF EXISTS {name}".format(name=name))


@pytest.mark.parametrize("name,engine", [
    ("mutating_mt","MergeTree()"),
    ("replicated_mutating_mt","ReplicatedMergeTree('/clickhouse/replicated_mutating_mt', '1')",),
])
def test_mutate_to_another_disk(start_cluster, name, engine):

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
            node1.query("INSERT INTO {} VALUES {}".format(name, ','.join(["('" + x + "')" for x in data])))

        node1.query("ALTER TABLE {} UPDATE s1 = concat(s1, 'x') WHERE 1".format(name))

        retry = 10
        while node1.query("SELECT * FROM system.mutations WHERE is_done = 0") != "" and retry > 0:
            retry -= 1
            time.sleep(0.5)

        assert node1.query("SELECT sum(endsWith(s1, 'x')) FROM {}".format(name)) == "25\n"
    finally:
        node1.query("DROP TABLE IF EXISTS {name}".format(name=name))



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
