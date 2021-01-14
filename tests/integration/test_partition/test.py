import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance')
q = instance.query
path_to_data = '/var/lib/clickhouse/'


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        q('CREATE DATABASE test ENGINE = Ordinary')     # Different path in shadow/ with Atomic

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture
def partition_table_simple(started_cluster):
    q("DROP TABLE IF EXISTS test.partition")
    q("CREATE TABLE test.partition (date MATERIALIZED toDate(0), x UInt64, sample_key MATERIALIZED intHash64(x)) "
      "ENGINE=MergeTree PARTITION BY date SAMPLE BY sample_key ORDER BY (date,x,sample_key) "
      "SETTINGS index_granularity=8192, index_granularity_bytes=0")
    q("INSERT INTO test.partition ( x ) VALUES ( now() )")
    q("INSERT INTO test.partition ( x ) VALUES ( now()+1 )")

    yield

    q('DROP TABLE test.partition')


def test_partition_simple(partition_table_simple):
    q("ALTER TABLE test.partition DETACH PARTITION 197001")
    q("ALTER TABLE test.partition ATTACH PARTITION 197001")
    q("OPTIMIZE TABLE test.partition")


def exec_bash(cmd):
    cmd = '/bin/bash -c "{}"'.format(cmd.replace('"', '\\"'))
    return instance.exec_in_container(cmd)


def partition_complex_assert_columns_txt():
    path_to_parts = path_to_data + 'data/test/partition/'
    parts = TSV(q("SELECT name FROM system.parts WHERE database='test' AND table='partition'"))
    for part_name in parts.lines:
        path_to_columns = path_to_parts + part_name + '/columns.txt'
        # 2 header lines + 3 columns
        assert exec_bash('cat {} | wc -l'.format(path_to_columns)) == '5\n'


def partition_complex_assert_checksums():
    # Do `cd` for consistent output for reference
    # Do not check increment.txt - it can be changed by other tests with FREEZE
    cmd = 'cd ' + path_to_data + " && find shadow -type f -exec md5sum {} \\;" \
                                 "  | grep partition" \
                                 "  | sed 's!shadow/[0-9]*/data/[a-z0-9_-]*/!shadow/1/data/test/!g'" \
                                 "  | sort" \
                                 "  | uniq"

    checksums = "082814b5aa5109160d5c0c5aff10d4df\tshadow/1/data/test/partition/19700102_2_2_0/k.bin\n" \
                "082814b5aa5109160d5c0c5aff10d4df\tshadow/1/data/test/partition/19700201_1_1_0/v1.bin\n" \
                "13cae8e658e0ca4f75c56b1fc424e150\tshadow/1/data/test/partition/19700102_2_2_0/minmax_p.idx\n" \
                "25daad3d9e60b45043a70c4ab7d3b1c6\tshadow/1/data/test/partition/19700102_2_2_0/partition.dat\n" \
                "3726312af62aec86b64a7708d5751787\tshadow/1/data/test/partition/19700201_1_1_0/partition.dat\n" \
                "37855b06a39b79a67ea4e86e4a3299aa\tshadow/1/data/test/partition/19700102_2_2_0/checksums.txt\n" \
                "38e62ff37e1e5064e9a3f605dfe09d13\tshadow/1/data/test/partition/19700102_2_2_0/v1.bin\n" \
                "4ae71336e44bf9bf79d2752e234818a5\tshadow/1/data/test/partition/19700102_2_2_0/k.mrk\n" \
                "4ae71336e44bf9bf79d2752e234818a5\tshadow/1/data/test/partition/19700102_2_2_0/p.mrk\n" \
                "4ae71336e44bf9bf79d2752e234818a5\tshadow/1/data/test/partition/19700102_2_2_0/v1.mrk\n" \
                "4ae71336e44bf9bf79d2752e234818a5\tshadow/1/data/test/partition/19700201_1_1_0/k.mrk\n" \
                "4ae71336e44bf9bf79d2752e234818a5\tshadow/1/data/test/partition/19700201_1_1_0/p.mrk\n" \
                "4ae71336e44bf9bf79d2752e234818a5\tshadow/1/data/test/partition/19700201_1_1_0/v1.mrk\n" \
                "55a54008ad1ba589aa210d2629c1df41\tshadow/1/data/test/partition/19700201_1_1_0/primary.idx\n" \
                "5f087cb3e7071bf9407e095821e2af8f\tshadow/1/data/test/partition/19700201_1_1_0/checksums.txt\n" \
                "77d5af402ada101574f4da114f242e02\tshadow/1/data/test/partition/19700102_2_2_0/columns.txt\n" \
                "77d5af402ada101574f4da114f242e02\tshadow/1/data/test/partition/19700201_1_1_0/columns.txt\n" \
                "88cdc31ded355e7572d68d8cde525d3a\tshadow/1/data/test/partition/19700201_1_1_0/p.bin\n" \
                "9e688c58a5487b8eaf69c9e1005ad0bf\tshadow/1/data/test/partition/19700102_2_2_0/primary.idx\n" \
                "c0904274faa8f3f06f35666cc9c5bd2f\tshadow/1/data/test/partition/19700102_2_2_0/default_compression_codec.txt\n" \
                "c0904274faa8f3f06f35666cc9c5bd2f\tshadow/1/data/test/partition/19700201_1_1_0/default_compression_codec.txt\n" \
                "c4ca4238a0b923820dcc509a6f75849b\tshadow/1/data/test/partition/19700102_2_2_0/count.txt\n" \
                "c4ca4238a0b923820dcc509a6f75849b\tshadow/1/data/test/partition/19700201_1_1_0/count.txt\n" \
                "cfcb770c3ecd0990dcceb1bde129e6c6\tshadow/1/data/test/partition/19700102_2_2_0/p.bin\n" \
                "e2af3bef1fd129aea73a890ede1e7a30\tshadow/1/data/test/partition/19700201_1_1_0/k.bin\n" \
                "f2312862cc01adf34a93151377be2ddf\tshadow/1/data/test/partition/19700201_1_1_0/minmax_p.idx\n"

    assert TSV(exec_bash(cmd).replace('  ', '\t')) == TSV(checksums)


@pytest.fixture
def partition_table_complex(started_cluster):
    q("DROP TABLE IF EXISTS test.partition")
    q("CREATE TABLE test.partition (p Date, k Int8, v1 Int8 MATERIALIZED k + 1) "
      "ENGINE = MergeTree PARTITION BY p ORDER BY k SETTINGS index_granularity=1, index_granularity_bytes=0")
    q("INSERT INTO test.partition (p, k) VALUES(toDate(31), 1)")
    q("INSERT INTO test.partition (p, k) VALUES(toDate(1), 2)")

    yield

    q("DROP TABLE test.partition")


def test_partition_complex(partition_table_complex):
    partition_complex_assert_columns_txt()

    q("ALTER TABLE test.partition FREEZE")

    partition_complex_assert_checksums()

    q("ALTER TABLE test.partition DETACH PARTITION 197001")
    q("ALTER TABLE test.partition ATTACH PARTITION 197001")

    partition_complex_assert_columns_txt()

    q("ALTER TABLE test.partition MODIFY COLUMN v1 Int8")

    # Check the backup hasn't changed
    partition_complex_assert_checksums()

    q("OPTIMIZE TABLE test.partition")

    expected = TSV('31\t1\t2\n'
                   '1\t2\t3')
    res = q("SELECT toUInt16(p), k, v1 FROM test.partition ORDER BY k")
    assert (TSV(res) == expected)


@pytest.fixture
def cannot_attach_active_part_table(started_cluster):
    q("DROP TABLE IF EXISTS test.attach_active")
    q("CREATE TABLE test.attach_active (n UInt64) ENGINE = MergeTree() PARTITION BY intDiv(n, 4) ORDER BY n")
    q("INSERT INTO test.attach_active SELECT number FROM system.numbers LIMIT 16")

    yield

    q("DROP TABLE test.attach_active")


def test_cannot_attach_active_part(cannot_attach_active_part_table):
    error = instance.client.query_and_get_error("ALTER TABLE test.attach_active ATTACH PART '../1_2_2_0'")
    print(error)
    assert 0 <= error.find('Invalid part name')

    res = q("SElECT name FROM system.parts WHERE table='attach_active' AND database='test' ORDER BY name")
    assert TSV(res) == TSV('0_1_1_0\n1_2_2_0\n2_3_3_0\n3_4_4_0')
    assert TSV(q("SElECT count(), sum(n) FROM test.attach_active")) == TSV('16\t120')


@pytest.fixture
def attach_check_all_parts_table(started_cluster):
    q("SYSTEM STOP MERGES")
    q("DROP TABLE IF EXISTS test.attach_partition")
    q("CREATE TABLE test.attach_partition (n UInt64) ENGINE = MergeTree() PARTITION BY intDiv(n, 8) ORDER BY n")
    q("INSERT INTO test.attach_partition SELECT number FROM system.numbers WHERE number % 2 = 0 LIMIT 8")
    q("INSERT INTO test.attach_partition SELECT number FROM system.numbers WHERE number % 2 = 1 LIMIT 8")

    yield

    q("DROP TABLE test.attach_partition")
    q("SYSTEM START MERGES")


def test_attach_check_all_parts(attach_check_all_parts_table):
    q("ALTER TABLE test.attach_partition DETACH PARTITION 0")

    path_to_detached = path_to_data + 'data/test/attach_partition/detached/'
    exec_bash('mkdir {}'.format(path_to_detached + '0_5_5_0'))
    exec_bash('cp -pr {} {}'.format(path_to_detached + '0_1_1_0', path_to_detached + 'attaching_0_6_6_0'))
    exec_bash('cp -pr {} {}'.format(path_to_detached + '0_3_3_0', path_to_detached + 'deleting_0_7_7_0'))

    error = instance.client.query_and_get_error("ALTER TABLE test.attach_partition ATTACH PARTITION 0")
    assert 0 <= error.find('No columns in part 0_5_5_0') or 0 <= error.find('No columns.txt in part 0_5_5_0')

    parts = q("SElECT name FROM system.parts WHERE table='attach_partition' AND database='test' ORDER BY name")
    assert TSV(parts) == TSV('1_2_2_0\n1_4_4_0')
    detached = q("SELECT name FROM system.detached_parts "
                 "WHERE table='attach_partition' AND database='test' ORDER BY name")
    assert TSV(detached) == TSV('0_1_1_0\n0_3_3_0\n0_5_5_0\nattaching_0_6_6_0\ndeleting_0_7_7_0')

    exec_bash('rm -r {}'.format(path_to_detached + '0_5_5_0'))

    q("ALTER TABLE test.attach_partition ATTACH PARTITION 0")
    parts = q("SElECT name FROM system.parts WHERE table='attach_partition' AND database='test' ORDER BY name")
    expected = '0_5_5_0\n0_6_6_0\n1_2_2_0\n1_4_4_0'
    assert TSV(parts) == TSV(expected)
    assert TSV(q("SElECT count(), sum(n) FROM test.attach_partition")) == TSV('16\t120')

    detached = q("SELECT name FROM system.detached_parts "
                 "WHERE table='attach_partition' AND database='test' ORDER BY name")
    assert TSV(detached) == TSV('attaching_0_6_6_0\ndeleting_0_7_7_0')


@pytest.fixture
def drop_detached_parts_table(started_cluster):
    q("SYSTEM STOP MERGES")
    q("DROP TABLE IF EXISTS test.drop_detached")
    q("CREATE TABLE test.drop_detached (n UInt64) ENGINE = MergeTree() PARTITION BY intDiv(n, 8) ORDER BY n")
    q("INSERT INTO test.drop_detached SELECT number FROM system.numbers WHERE number % 2 = 0 LIMIT 8")
    q("INSERT INTO test.drop_detached SELECT number FROM system.numbers WHERE number % 2 = 1 LIMIT 8")

    yield

    q("DROP TABLE test.drop_detached")
    q("SYSTEM START MERGES")


def test_drop_detached_parts(drop_detached_parts_table):
    s = {"allow_drop_detached": 1}
    q("ALTER TABLE test.drop_detached DETACH PARTITION 0")
    q("ALTER TABLE test.drop_detached DETACH PARTITION 1")

    path_to_detached = path_to_data + 'data/test/drop_detached/detached/'
    exec_bash('mkdir {}'.format(path_to_detached + 'attaching_0_6_6_0'))
    exec_bash('mkdir {}'.format(path_to_detached + 'deleting_0_7_7_0'))
    exec_bash('mkdir {}'.format(path_to_detached + 'any_other_name'))
    exec_bash('mkdir {}'.format(path_to_detached + 'prefix_1_2_2_0_0'))

    error = instance.client.query_and_get_error("ALTER TABLE test.drop_detached DROP DETACHED PART '../1_2_2_0'",
                                                settings=s)
    assert 0 <= error.find('Invalid part name')

    q("ALTER TABLE test.drop_detached DROP DETACHED PART '0_1_1_0'", settings=s)

    error = instance.client.query_and_get_error("ALTER TABLE test.drop_detached DROP DETACHED PART 'attaching_0_6_6_0'",
                                                settings=s)
    assert 0 <= error.find('Cannot drop part')

    error = instance.client.query_and_get_error("ALTER TABLE test.drop_detached DROP DETACHED PART 'deleting_0_7_7_0'",
                                                settings=s)
    assert 0 <= error.find('Cannot drop part')

    q("ALTER TABLE test.drop_detached DROP DETACHED PART 'any_other_name'", settings=s)

    detached = q("SElECT name FROM system.detached_parts WHERE table='drop_detached' AND database='test' ORDER BY name")
    assert TSV(detached) == TSV('0_3_3_0\n1_2_2_0\n1_4_4_0\nattaching_0_6_6_0\ndeleting_0_7_7_0\nprefix_1_2_2_0_0')

    q("ALTER TABLE test.drop_detached DROP DETACHED PARTITION 1", settings=s)
    detached = q("SElECT name FROM system.detached_parts WHERE table='drop_detached' AND database='test' ORDER BY name")
    assert TSV(detached) == TSV('0_3_3_0\nattaching_0_6_6_0\ndeleting_0_7_7_0')
