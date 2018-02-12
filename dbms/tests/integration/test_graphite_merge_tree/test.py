import os.path as p
import time
import datetime
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance', main_configs=['configs/graphite_rollup.xml'])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        instance.query('CREATE DATABASE test')

        yield cluster

    finally:
        cluster.shutdown()

@pytest.fixture
def graphite_table(started_cluster):
    instance.query('''
DROP TABLE IF EXISTS test.graphite;
CREATE TABLE test.graphite
    (metric String, value Float64, timestamp UInt32, date Date, updated UInt32)
    ENGINE = GraphiteMergeTree(date, (metric, timestamp), 8192, 'graphite_rollup');
''')

    yield

    instance.query('DROP TABLE test.graphite')


def test_rollup_versions(graphite_table):
    timestamp = int(time.time())
    rounded_timestamp = timestamp - timestamp % 60
    date = datetime.date.today().isoformat()

    q = instance.query

    # Insert rows with timestamps relative to the current time so that the first retention clause is active.
    # Two parts are created.
    q('''
INSERT INTO test.graphite (metric, value, timestamp, date, updated) VALUES ('one_min.x1', 100, {timestamp}, '{date}', 1);
INSERT INTO test.graphite (metric, value, timestamp, date, updated) VALUES ('one_min.x1', 200, {timestamp}, '{date}', 2);
'''.format(timestamp=timestamp, date=date))

    expected1 = '''\
one_min.x1	100	{timestamp}	{date}	1
one_min.x1	200	{timestamp}	{date}	2
'''.format(timestamp=timestamp, date=date)

    assert TSV(q('SELECT * FROM test.graphite ORDER BY updated')) == TSV(expected1)

    q('OPTIMIZE TABLE test.graphite')

    # After rollup only the row with max version is retained.
    expected2 = '''\
one_min.x1	200	{timestamp}	{date}	2
'''.format(timestamp=rounded_timestamp, date=date)

    assert TSV(q('SELECT * FROM test.graphite')) == TSV(expected2)


def test_rollup_aggregation(graphite_table):
    q = instance.query

    # This query essentially emulates what rollup does.
    result1 = q('''
SELECT avg(v), max(upd)
FROM (SELECT timestamp,
            argMax(value, (updated, number)) AS v,
            max(updated) AS upd
      FROM (SELECT 'one_min.x5' AS metric,
                   toFloat64(number) AS value,
                   toUInt32(1111111111 + intDiv(number, 3)) AS timestamp,
                   toDate('2017-02-02') AS date,
                   toUInt32(intDiv(number, 2)) AS updated,
                   number
            FROM system.numbers LIMIT 1000000)
      WHERE intDiv(timestamp, 600) * 600 = 1111444200
      GROUP BY timestamp)
''')

    expected1 = '''\
999634.9918367347	499999
'''
    assert TSV(result1) == TSV(expected1)

    # Timestamp 1111111111 is in sufficiently distant past so that the last retention clause is active.
    result2 = q('''
INSERT INTO test.graphite
    SELECT 'one_min.x' AS metric,
           toFloat64(number) AS value,
           toUInt32(1111111111 + intDiv(number, 3)) AS timestamp,
           toDate('2017-02-02') AS date, toUInt32(intDiv(number, 2)) AS updated
    FROM (SELECT * FROM system.numbers LIMIT 1000000)
    WHERE intDiv(timestamp, 600) * 600 = 1111444200;

OPTIMIZE TABLE test.graphite PARTITION 201702 FINAL;

SELECT * FROM test.graphite;
''')

    expected2 = '''\
one_min.x	999634.9918367347	1111444200	2017-02-02	499999
'''

    assert TSV(result2) == TSV(expected2)


def test_rollup_aggregation_2(graphite_table):
    result = instance.query('''
INSERT INTO test.graphite
    SELECT 'one_min.x' AS metric,
           toFloat64(number) AS value,
           toUInt32(1111111111 - intDiv(number, 3)) AS timestamp,
           toDate('2017-02-02') AS date,
           toUInt32(100 - number) AS updated
    FROM (SELECT * FROM system.numbers LIMIT 50);

OPTIMIZE TABLE test.graphite PARTITION 201702 FINAL;

SELECT * FROM test.graphite;
''')

    expected = '''\
one_min.x	24	1111110600	2017-02-02	100
'''

    assert TSV(result) == TSV(expected)


def test_multiple_paths_and_versions(graphite_table):
    result = instance.query('''
INSERT INTO test.graphite
    SELECT 'one_min.x' AS metric,
           toFloat64(number) AS value,
           toUInt32(1111111111 + intDiv(number, 3) * 600) AS timestamp,
           toDate('2017-02-02') AS date,
           toUInt32(100 - number) AS updated
    FROM (SELECT * FROM system.numbers LIMIT 50);

OPTIMIZE TABLE test.graphite PARTITION 201702 FINAL;

SELECT * FROM test.graphite;


INSERT INTO test.graphite
    SELECT 'one_min.y' AS metric,
           toFloat64(number) AS value,
           toUInt32(1111111111 + number * 600) AS timestamp,
           toDate('2017-02-02') AS date,
           toUInt32(100 - number) AS updated
    FROM (SELECT * FROM system.numbers LIMIT 50);

OPTIMIZE TABLE test.graphite PARTITION 201702 FINAL;

SELECT * FROM test.graphite;
''')

    with open(p.join(p.dirname(__file__), 'test_multiple_paths_and_versions.reference')) as reference:
        assert TSV(result) == TSV(reference)


def test_multiple_output_blocks(graphite_table):
    MERGED_BLOCK_SIZE = 8192

    to_insert = ''
    expected = ''
    for i in range(2 * MERGED_BLOCK_SIZE + 1):
        rolled_up_time = 1000000200 + 600 * i

        for j in range(3):
            cur_time = rolled_up_time + 100 * j
            to_insert += 'one_min.x1	{}	{}	2001-09-09	1\n'.format(10 * j, cur_time)
            to_insert += 'one_min.x1	{}	{}	2001-09-09	2\n'.format(10 * (j + 1), cur_time)

        expected += 'one_min.x1	20	{}	2001-09-09	2\n'.format(rolled_up_time)

    instance.query('INSERT INTO test.graphite FORMAT TSV', to_insert)

    result = instance.query('''
OPTIMIZE TABLE test.graphite PARTITION 200109 FINAL;

SELECT * FROM test.graphite;
''')

    assert TSV(result) == TSV(expected)


def test_paths_not_matching_any_pattern(graphite_table):
    to_insert = '''\
one_min.x1	100	1000000000	2001-09-09	1
zzzzzzzz	100	1000000001	2001-09-09	1
zzzzzzzz	200	1000000001	2001-09-09	2
'''

    instance.query('INSERT INTO test.graphite FORMAT TSV', to_insert)

    expected = '''\
one_min.x1	100	999999600	2001-09-09	1
zzzzzzzz	200	1000000001	2001-09-09	2
'''

    result = instance.query('''
OPTIMIZE TABLE test.graphite PARTITION 200109 FINAL;

SELECT * FROM test.graphite;
''')

    assert TSV(result) == TSV(expected)

def test_path_dangling_pointer(graphite_table):
    instance.query('''
DROP TABLE IF EXISTS test.graphite2;
CREATE TABLE test.graphite2
  (metric String, value Float64, timestamp UInt32, date Date, updated UInt32)
  ENGINE = GraphiteMergeTree(date, (metric, timestamp), 1, 'graphite_rollup');
  ''')

    path = 'abcd' * 4000000 # 16MB
    instance.query('INSERT INTO test.graphite2 FORMAT TSV', "{}\t0.0\t0\t2018-01-01\t100\n".format(path))
    instance.query('INSERT INTO test.graphite2 FORMAT TSV', "{}\t0.0\t0\t2018-01-01\t101\n".format(path))
    for version in range(10):
        instance.query('INSERT INTO test.graphite2 FORMAT TSV', "{}\t0.0\t0\t2018-01-01\t{}\n".format(path, version))

    while True:
      instance.query('OPTIMIZE TABLE test.graphite2 PARTITION 201801 FINAL')
      parts = int(instance.query("SELECT count() FROM system.parts WHERE active AND database='test' AND table='graphite2'"))
      if parts == 1:
        break
      print "Parts", parts

    assert TSV(instance.query("SELECT value, timestamp, date, updated FROM test.graphite2")) == TSV("0\t0\t2018-01-01\t101\n")

    instance.query('DROP TABLE test.graphite2')