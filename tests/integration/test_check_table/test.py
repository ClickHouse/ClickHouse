import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', with_zookeeper=True)
node2 = cluster.add_instance('node2', with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def corrupt_data_part_on_disk(node, table, part_name):
    part_path = node.query(
        "SELECT path FROM system.parts WHERE table = '{}' and name = '{}'".format(table, part_name)).strip()
    node.exec_in_container(['bash', '-c',
                            'cd {p} && ls *.bin | head -n 1 | xargs -I{{}} sh -c \'echo "1" >> $1\' -- {{}}'.format(
                                p=part_path)], privileged=True)


def remove_checksums_on_disk(node, table, part_name):
    part_path = node.query(
        "SELECT path FROM system.parts WHERE table = '{}' and name = '{}'".format(table, part_name)).strip()
    node.exec_in_container(['bash', '-c', 'rm -r {p}/checksums.txt'.format(p=part_path)], privileged=True)


def remove_part_from_disk(node, table, part_name):
    part_path = node.query(
        "SELECT path FROM system.parts WHERE table = '{}' and name = '{}'".format(table, part_name)).strip()
    if not part_path:
        raise Exception("Part " + part_name + "doesn't exist")
    node.exec_in_container(['bash', '-c', 'rm -r {p}/*'.format(p=part_path)], privileged=True)


def test_check_normal_table_corruption(started_cluster):
    node1.query("DROP TABLE IF EXISTS non_replicated_mt")

    node1.query('''
        CREATE TABLE non_replicated_mt(date Date, id UInt32, value Int32)
        ENGINE = MergeTree() PARTITION BY toYYYYMM(date) ORDER BY id
        SETTINGS min_bytes_for_wide_part=0;
    ''')

    node1.query("INSERT INTO non_replicated_mt VALUES (toDate('2019-02-01'), 1, 10), (toDate('2019-02-01'), 2, 12)")
    assert node1.query("CHECK TABLE non_replicated_mt PARTITION 201902",
                       settings={"check_query_single_value_result": 0}) == "201902_1_1_0\t1\t\n"

    remove_checksums_on_disk(node1, "non_replicated_mt", "201902_1_1_0")

    assert node1.query("CHECK TABLE non_replicated_mt", settings={
        "check_query_single_value_result": 0}).strip() == "201902_1_1_0\t1\tChecksums recounted and written to disk."

    assert node1.query("SELECT COUNT() FROM non_replicated_mt") == "2\n"

    remove_checksums_on_disk(node1, "non_replicated_mt", "201902_1_1_0")

    assert node1.query("CHECK TABLE non_replicated_mt PARTITION 201902", settings={
        "check_query_single_value_result": 0}).strip() == "201902_1_1_0\t1\tChecksums recounted and written to disk."

    assert node1.query("SELECT COUNT() FROM non_replicated_mt") == "2\n"

    corrupt_data_part_on_disk(node1, "non_replicated_mt", "201902_1_1_0")

    assert node1.query("CHECK TABLE non_replicated_mt", settings={
        "check_query_single_value_result": 0}).strip() == "201902_1_1_0\t0\tCannot read all data. Bytes read: 2. Bytes expected: 25."

    assert node1.query("CHECK TABLE non_replicated_mt", settings={
        "check_query_single_value_result": 0}).strip() == "201902_1_1_0\t0\tCannot read all data. Bytes read: 2. Bytes expected: 25."

    node1.query("INSERT INTO non_replicated_mt VALUES (toDate('2019-01-01'), 1, 10), (toDate('2019-01-01'), 2, 12)")

    assert node1.query("CHECK TABLE non_replicated_mt PARTITION 201901",
                       settings={"check_query_single_value_result": 0}) == "201901_2_2_0\t1\t\n"

    corrupt_data_part_on_disk(node1, "non_replicated_mt", "201901_2_2_0")

    remove_checksums_on_disk(node1, "non_replicated_mt", "201901_2_2_0")

    assert node1.query("CHECK TABLE non_replicated_mt PARTITION 201901", settings={
        "check_query_single_value_result": 0}) == "201901_2_2_0\t0\tCheck of part finished with error: \\'Cannot read all data. Bytes read: 2. Bytes expected: 25.\\'\n"


def test_check_replicated_table_simple(started_cluster):
    for node in [node1, node2]:
        node.query("DROP TABLE IF EXISTS replicated_mt")

        node.query('''
        CREATE TABLE replicated_mt(date Date, id UInt32, value Int32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/replicated_mt', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY id;
            '''.format(replica=node.name))

    node1.query("INSERT INTO replicated_mt VALUES (toDate('2019-02-01'), 1, 10), (toDate('2019-02-01'), 2, 12)")
    node2.query("SYSTEM SYNC REPLICA replicated_mt")

    assert node1.query("SELECT count() from replicated_mt") == "2\n"
    assert node2.query("SELECT count() from replicated_mt") == "2\n"

    assert node1.query("CHECK TABLE replicated_mt",
                       settings={"check_query_single_value_result": 0}) == "201902_0_0_0\t1\t\n"
    assert node2.query("CHECK TABLE replicated_mt",
                       settings={"check_query_single_value_result": 0}) == "201902_0_0_0\t1\t\n"

    node2.query("INSERT INTO replicated_mt VALUES (toDate('2019-01-02'), 3, 10), (toDate('2019-01-02'), 4, 12)")
    node1.query("SYSTEM SYNC REPLICA replicated_mt")
    assert node1.query("SELECT count() from replicated_mt") == "4\n"
    assert node2.query("SELECT count() from replicated_mt") == "4\n"

    assert node1.query("CHECK TABLE replicated_mt PARTITION 201901",
                       settings={"check_query_single_value_result": 0}) == "201901_0_0_0\t1\t\n"
    assert node2.query("CHECK TABLE replicated_mt PARTITION 201901",
                       settings={"check_query_single_value_result": 0}) == "201901_0_0_0\t1\t\n"


def test_check_replicated_table_corruption(started_cluster):
    for node in [node1, node2]:
        node.query_with_retry("DROP TABLE IF EXISTS replicated_mt_1")

        node.query_with_retry('''
        CREATE TABLE replicated_mt_1(date Date, id UInt32, value Int32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/replicated_mt_1', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY id;
            '''.format(replica=node.name))

    node1.query("INSERT INTO replicated_mt_1 VALUES (toDate('2019-02-01'), 1, 10), (toDate('2019-02-01'), 2, 12)")
    node1.query("INSERT INTO replicated_mt_1 VALUES (toDate('2019-01-02'), 3, 10), (toDate('2019-01-02'), 4, 12)")
    node2.query("SYSTEM SYNC REPLICA replicated_mt_1")

    assert node1.query("SELECT count() from replicated_mt_1") == "4\n"
    assert node2.query("SELECT count() from replicated_mt_1") == "4\n"

    part_name = node1.query_with_retry(
        "SELECT name from system.parts where table = 'replicated_mt_1' and partition_id = '201901' and active = 1").strip()

    corrupt_data_part_on_disk(node1, "replicated_mt_1", part_name)
    assert node1.query("CHECK TABLE replicated_mt_1 PARTITION 201901", settings={
        "check_query_single_value_result": 0}) == "{p}\t0\tPart {p} looks broken. Removing it and will try to fetch.\n".format(
        p=part_name)

    node1.query_with_retry("SYSTEM SYNC REPLICA replicated_mt_1")
    assert node1.query("CHECK TABLE replicated_mt_1 PARTITION 201901",
                       settings={"check_query_single_value_result": 0}) == "{}\t1\t\n".format(part_name)
    assert node1.query("SELECT count() from replicated_mt_1") == "4\n"

    remove_part_from_disk(node2, "replicated_mt_1", part_name)
    assert node2.query("CHECK TABLE replicated_mt_1 PARTITION 201901", settings={
        "check_query_single_value_result": 0}) == "{p}\t0\tPart {p} looks broken. Removing it and will try to fetch.\n".format(
        p=part_name)

    node1.query("SYSTEM SYNC REPLICA replicated_mt_1")
    assert node1.query("CHECK TABLE replicated_mt_1 PARTITION 201901",
                       settings={"check_query_single_value_result": 0}) == "{}\t1\t\n".format(part_name)
    assert node1.query("SELECT count() from replicated_mt_1") == "4\n"
