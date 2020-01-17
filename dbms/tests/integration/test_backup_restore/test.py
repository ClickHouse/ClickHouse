import os.path
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
        q('CREATE DATABASE test ENGINE = Ordinary')

        yield cluster

    finally:
        cluster.shutdown()


def exec_bash(cmd):
    cmd = '/bin/bash -c "{}"'.format(cmd.replace('"', '\\"'))
    return instance.exec_in_container(cmd)


def copy_backup_to_detached(database, src_table, dst_table):
    fp_increment = os.path.join(path_to_data, 'shadow/increment.txt')
    increment = exec_bash('cat ' + fp_increment).strip()
    fp_backup = os.path.join(path_to_data, 'shadow', increment, 'data', database, src_table)
    fp_detached = os.path.join(path_to_data, 'data', database, dst_table, 'detached')
    exec_bash('cp -r {}/* {}/'.format(fp_backup, fp_detached))


@pytest.fixture
def backup_restore(started_cluster):
    q("DROP TABLE IF EXISTS test.tbl")
    q("CREATE TABLE test.tbl (p Date, k Int8) ENGINE = MergeTree PARTITION BY toYYYYMM(p) ORDER BY p")
    for i in range(1, 4):
        q('INSERT INTO test.tbl (p, k) VALUES(toDate({}), {})'.format(i, i))
    for i in range(31, 34):
        q('INSERT INTO test.tbl (p, k) VALUES(toDate({}), {})'.format(i, i))

    expected = TSV('1970-01-02\t1\n1970-01-03\t2\n1970-01-04\t3\n1970-02-01\t31\n1970-02-02\t32\n1970-02-03\t33')
    res = q("SELECT * FROM test.tbl ORDER BY p")
    assert(TSV(res) == expected)

    q("ALTER TABLE test.tbl FREEZE")

    yield

    q("DROP TABLE IF EXISTS test.tbl")


def test_restore(backup_restore):
    q("CREATE TABLE test.tbl1 AS test.tbl")

    copy_backup_to_detached('test', 'tbl', 'tbl1')

    # The data_version of parts to be attached are larger than the newly created table's data_version.
    q("ALTER TABLE test.tbl1 ATTACH PARTITION 197001")
    q("ALTER TABLE test.tbl1 ATTACH PARTITION 197002")
    q("SELECT sleep(2)")

    # Validate the attached parts are identical to the backup.
    expected = TSV('1970-01-02\t1\n1970-01-03\t2\n1970-01-04\t3\n1970-02-01\t31\n1970-02-02\t32\n1970-02-03\t33')
    res = q("SELECT * FROM test.tbl1 ORDER BY p")
    assert(TSV(res) == expected)

    q("ALTER TABLE test.tbl1 UPDATE k=10 WHERE 1")
    q("SELECT sleep(2)")

    # Validate mutation has been applied to all attached parts.
    expected = TSV('1970-01-02\t10\n1970-01-03\t10\n1970-01-04\t10\n1970-02-01\t10\n1970-02-02\t10\n1970-02-03\t10')
    res = q("SELECT * FROM test.tbl1 ORDER BY p")
    assert(TSV(res) == expected)

    q("DROP TABLE IF EXISTS test.tbl1")


def test_attach_partition(backup_restore):
    q("CREATE TABLE test.tbl2 AS test.tbl")
    for i in range(3, 5):
        q('INSERT INTO test.tbl2(p, k) VALUES(toDate({}), {})'.format(i, i))
    for i in range(33, 35):
        q('INSERT INTO test.tbl2(p, k) VALUES(toDate({}), {})'.format(i, i))

    expected = TSV('1970-01-04\t3\n1970-01-05\t4\n1970-02-03\t33\n1970-02-04\t34')
    res = q("SELECT * FROM test.tbl2 ORDER BY p")
    assert(TSV(res) == expected)

    copy_backup_to_detached('test', 'tbl', 'tbl2')

    # The data_version of parts to be attached
    # - may be less than, equal to or larger than the current table's data_version.
    # - may intersect with the existing parts of a partition.
    q("ALTER TABLE test.tbl2 ATTACH PARTITION 197001")
    q("ALTER TABLE test.tbl2 ATTACH PARTITION 197002")
    q("SELECT sleep(2)")

    expected = TSV('1970-01-02\t1\n1970-01-03\t2\n1970-01-04\t3\n1970-01-04\t3\n1970-01-05\t4\n1970-02-01\t31\n1970-02-02\t32\n1970-02-03\t33\n1970-02-03\t33\n1970-02-04\t34')
    res = q("SELECT * FROM test.tbl2 ORDER BY p")
    assert(TSV(res) == expected)

    q("ALTER TABLE test.tbl2 UPDATE k=10 WHERE 1")
    q("SELECT sleep(2)")

    # Validate mutation has been applied to all attached parts.
    expected = TSV('1970-01-02\t10\n1970-01-03\t10\n1970-01-04\t10\n1970-01-04\t10\n1970-01-05\t10\n1970-02-01\t10\n1970-02-02\t10\n1970-02-03\t10\n1970-02-03\t10\n1970-02-04\t10')
    res = q("SELECT * FROM test.tbl2 ORDER BY p")
    assert(TSV(res) == expected)

    q("DROP TABLE IF EXISTS test.tbl2")


def test_replace_partition(backup_restore):
    q("CREATE TABLE test.tbl3 AS test.tbl")
    for i in range(3, 5):
        q('INSERT INTO test.tbl3(p, k) VALUES(toDate({}), {})'.format(i, i))
    for i in range(33, 35):
        q('INSERT INTO test.tbl3(p, k) VALUES(toDate({}), {})'.format(i, i))

    expected = TSV('1970-01-04\t3\n1970-01-05\t4\n1970-02-03\t33\n1970-02-04\t34')
    res = q("SELECT * FROM test.tbl3 ORDER BY p")
    assert(TSV(res) == expected)

    copy_backup_to_detached('test', 'tbl', 'tbl3')

    # The data_version of parts to be copied
    # - may be less than, equal to or larger than the current table data_version.
    # - may intersect with the existing parts of a partition.
    q("ALTER TABLE test.tbl3 REPLACE PARTITION 197002 FROM test.tbl")
    q("SELECT sleep(2)")

    expected = TSV('1970-01-04\t3\n1970-01-05\t4\n1970-02-01\t31\n1970-02-02\t32\n1970-02-03\t33')
    res = q("SELECT * FROM test.tbl3 ORDER BY p")
    assert(TSV(res) == expected)

    q("ALTER TABLE test.tbl3 UPDATE k=10 WHERE 1")
    q("SELECT sleep(2)")

    # Validate mutation has been applied to all copied parts.
    expected = TSV('1970-01-04\t10\n1970-01-05\t10\n1970-02-01\t10\n1970-02-02\t10\n1970-02-03\t10')
    res = q("SELECT * FROM test.tbl3 ORDER BY p")
    assert(TSV(res) == expected)

    q("DROP TABLE IF EXISTS test.tbl3")
