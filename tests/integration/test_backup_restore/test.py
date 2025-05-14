import logging
import os.path

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("node")
path_to_data = "/var/lib/clickhouse/"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        instance.query(
            "CREATE DATABASE test ENGINE = Ordinary",
            settings={"allow_deprecated_database_ordinary": 1},
        )  # Different path in shadow/ with Atomic
        instance.query("DROP TABLE IF EXISTS test.tbl")
        instance.query(
            "CREATE TABLE test.tbl (p Date, k Int8) ENGINE = MergeTree PARTITION BY toYYYYMM(p) ORDER BY p"
        )
        for i in range(1, 4):
            instance.query(
                "INSERT INTO test.tbl (p, k) VALUES(toDate({}), {})".format(i, i)
            )
        for i in range(31, 34):
            instance.query(
                "INSERT INTO test.tbl (p, k) VALUES(toDate({}), {})".format(i, i)
            )

        expected = TSV(
            "1970-01-02\t1\n1970-01-03\t2\n1970-01-04\t3\n1970-02-01\t31\n1970-02-02\t32\n1970-02-03\t33"
        )
        res = instance.query("SELECT * FROM test.tbl ORDER BY p")
        assert TSV(res) == expected

        instance.query("ALTER TABLE test.tbl FREEZE")

        yield cluster

    finally:
        cluster.shutdown()


def get_last_backup_path(instance, database, table):
    fp_increment = os.path.join(path_to_data, "shadow/increment.txt")
    increment = instance.exec_in_container(["cat", fp_increment]).strip()
    return os.path.join(path_to_data, "shadow", increment, "data", database, table)


def copy_backup_to_detached(instance, database, src_table, dst_table):
    fp_backup = os.path.join(path_to_data, "shadow", "*", "data", database, src_table)
    fp_detached = os.path.join(path_to_data, "data", database, dst_table, "detached")
    logging.debug(f"copy from {fp_backup} to {fp_detached}")
    instance.exec_in_container(["bash", "-c", f"cp -r {fp_backup} -T {fp_detached}"])


def test_restore(started_cluster):
    instance.query("CREATE TABLE test.tbl1 AS test.tbl")

    copy_backup_to_detached(started_cluster.instances["node"], "test", "tbl", "tbl1")

    # The data_version of parts to be attached are larger than the newly created table's data_version.
    instance.query("ALTER TABLE test.tbl1 ATTACH PARTITION 197001")
    instance.query("ALTER TABLE test.tbl1 ATTACH PARTITION 197002")
    instance.query("SELECT sleep(2)")

    # Validate the attached parts are identical to the backup.
    expected = TSV(
        "1970-01-02\t1\n1970-01-03\t2\n1970-01-04\t3\n1970-02-01\t31\n1970-02-02\t32\n1970-02-03\t33"
    )
    res = instance.query("SELECT * FROM test.tbl1 ORDER BY p")
    assert TSV(res) == expected

    instance.query("ALTER TABLE test.tbl1 UPDATE k=10 WHERE 1")
    instance.query("SELECT sleep(2)")

    # Validate mutation has been applied to all attached parts.
    expected = TSV(
        "1970-01-02\t10\n1970-01-03\t10\n1970-01-04\t10\n1970-02-01\t10\n1970-02-02\t10\n1970-02-03\t10"
    )
    res = instance.query("SELECT * FROM test.tbl1 ORDER BY p")
    assert TSV(res) == expected

    instance.query("DROP TABLE IF EXISTS test.tbl1")


def test_attach_partition(started_cluster):
    instance.query("CREATE TABLE test.tbl2 AS test.tbl")
    for i in range(3, 5):
        instance.query(
            "INSERT INTO test.tbl2(p, k) VALUES(toDate({}), {})".format(i, i)
        )
    for i in range(33, 35):
        instance.query(
            "INSERT INTO test.tbl2(p, k) VALUES(toDate({}), {})".format(i, i)
        )

    expected = TSV("1970-01-04\t3\n1970-01-05\t4\n1970-02-03\t33\n1970-02-04\t34")
    res = instance.query("SELECT * FROM test.tbl2 ORDER BY p")
    assert TSV(res) == expected

    copy_backup_to_detached(started_cluster.instances["node"], "test", "tbl", "tbl2")

    # The data_version of parts to be attached
    # - may be less than, equal to or larger than the current table's data_version.
    # - may intersect with the existing parts of a partition.
    instance.query("ALTER TABLE test.tbl2 ATTACH PARTITION 197001")
    instance.query("ALTER TABLE test.tbl2 ATTACH PARTITION 197002")
    instance.query("SELECT sleep(2)")

    expected = TSV(
        "1970-01-02\t1\n1970-01-03\t2\n1970-01-04\t3\n1970-01-04\t3\n1970-01-05\t4\n1970-02-01\t31\n1970-02-02\t32\n1970-02-03\t33\n1970-02-03\t33\n1970-02-04\t34"
    )
    res = instance.query("SELECT * FROM test.tbl2 ORDER BY p")
    assert TSV(res) == expected

    instance.query("ALTER TABLE test.tbl2 UPDATE k=10 WHERE 1")
    instance.query("SELECT sleep(2)")

    # Validate mutation has been applied to all attached parts.
    expected = TSV(
        "1970-01-02\t10\n1970-01-03\t10\n1970-01-04\t10\n1970-01-04\t10\n1970-01-05\t10\n1970-02-01\t10\n1970-02-02\t10\n1970-02-03\t10\n1970-02-03\t10\n1970-02-04\t10"
    )
    res = instance.query("SELECT * FROM test.tbl2 ORDER BY p")
    assert TSV(res) == expected

    instance.query("DROP TABLE IF EXISTS test.tbl2")


def test_replace_partition(started_cluster):
    instance.query("CREATE TABLE test.tbl3 AS test.tbl")
    for i in range(3, 5):
        instance.query(
            "INSERT INTO test.tbl3(p, k) VALUES(toDate({}), {})".format(i, i)
        )
    for i in range(33, 35):
        instance.query(
            "INSERT INTO test.tbl3(p, k) VALUES(toDate({}), {})".format(i, i)
        )

    expected = TSV("1970-01-04\t3\n1970-01-05\t4\n1970-02-03\t33\n1970-02-04\t34")
    res = instance.query("SELECT * FROM test.tbl3 ORDER BY p")
    assert TSV(res) == expected

    copy_backup_to_detached(started_cluster.instances["node"], "test", "tbl", "tbl3")

    # The data_version of parts to be copied
    # - may be less than, equal to or larger than the current table data_version.
    # - may intersect with the existing parts of a partition.
    instance.query("ALTER TABLE test.tbl3 REPLACE PARTITION 197002 FROM test.tbl")
    instance.query("SELECT sleep(2)")

    expected = TSV(
        "1970-01-04\t3\n1970-01-05\t4\n1970-02-01\t31\n1970-02-02\t32\n1970-02-03\t33"
    )
    res = instance.query("SELECT * FROM test.tbl3 ORDER BY p")
    assert TSV(res) == expected

    instance.query("ALTER TABLE test.tbl3 UPDATE k=10 WHERE 1")
    instance.query("SELECT sleep(2)")

    # Validate mutation has been applied to all copied parts.
    expected = TSV(
        "1970-01-04\t10\n1970-01-05\t10\n1970-02-01\t10\n1970-02-02\t10\n1970-02-03\t10"
    )
    res = instance.query("SELECT * FROM test.tbl3 ORDER BY p")
    assert TSV(res) == expected

    instance.query("DROP TABLE IF EXISTS test.tbl3")
