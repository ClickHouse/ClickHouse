import datetime
import os.path as p
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, csv_compare

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/graphite_rollup.xml"],
    user_configs=["configs/users.xml"],
)
q = instance.query


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        q("CREATE DATABASE test")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture
def graphite_table(started_cluster):
    q(
        """
DROP TABLE IF EXISTS test.graphite;
CREATE TABLE test.graphite
    (metric String, value Float64, timestamp UInt32, date Date, updated UInt32)
    ENGINE = GraphiteMergeTree('graphite_rollup')
    PARTITION BY toYYYYMM(date)
    ORDER BY (metric, timestamp)
    SETTINGS index_granularity=8192;
"""
    )

    yield

    q("DROP TABLE test.graphite")


def test_rollup_versions(graphite_table):
    timestamp = int(time.time())
    rounded_timestamp = timestamp - timestamp % 60
    date = datetime.date.today().isoformat()

    # Insert rows with timestamps relative to the current time so that the
    # first retention clause is active.
    # Two parts are created.
    q(
        """
INSERT INTO test.graphite (metric, value, timestamp, date, updated)
      VALUES ('one_min.x1', 100, {timestamp}, '{date}', 1);
INSERT INTO test.graphite (metric, value, timestamp, date, updated)
      VALUES ('one_min.x1', 200, {timestamp}, '{date}', 2);
""".format(
            timestamp=timestamp, date=date
        )
    )

    expected1 = """\
one_min.x1	100	{timestamp}	{date}	1
one_min.x1	200	{timestamp}	{date}	2
""".format(
        timestamp=timestamp, date=date
    )

    assert TSV(q("SELECT * FROM test.graphite ORDER BY updated")) == TSV(expected1)

    q("OPTIMIZE TABLE test.graphite")

    # After rollup only the row with max version is retained.
    expected2 = """\
one_min.x1	200	{timestamp}	{date}	2
""".format(
        timestamp=rounded_timestamp, date=date
    )

    assert TSV(q("SELECT * FROM test.graphite")) == TSV(expected2)


def test_rollup_aggregation(graphite_table):
    # This query essentially emulates what rollup does.
    result1 = q(
        """
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
"""
    )

    expected1 = """\
999634.9918367347	499999
"""
    assert TSV(result1) == TSV(expected1)

    # Timestamp 1111111111 is in sufficiently distant past
    # so that the last retention clause is active.
    result2 = q(
        """
INSERT INTO test.graphite
    SELECT 'one_min.x' AS metric,
           toFloat64(number) AS value,
           toUInt32(1111111111 + intDiv(number, 3)) AS timestamp,
           toDate('2017-02-02') AS date, toUInt32(intDiv(number, 2)) AS updated
    FROM (SELECT * FROM system.numbers LIMIT 1000000)
    WHERE intDiv(timestamp, 600) * 600 = 1111444200;

OPTIMIZE TABLE test.graphite PARTITION 201702 FINAL;

SELECT * FROM test.graphite;
"""
    )

    expected2 = """\
one_min.x	999634.9918367347	1111444200	2017-02-02	499999
"""

    assert TSV(result2) == TSV(expected2)


def test_rollup_aggregation_2(graphite_table):
    result = q(
        """
INSERT INTO test.graphite
    SELECT 'one_min.x' AS metric,
           toFloat64(number) AS value,
           toUInt32(1111111111 - intDiv(number, 3)) AS timestamp,
           toDate('2017-02-02') AS date,
           toUInt32(100 - number) AS updated
    FROM (SELECT * FROM system.numbers LIMIT 50);

OPTIMIZE TABLE test.graphite PARTITION 201702 FINAL;

SELECT * FROM test.graphite;
"""
    )

    expected = """\
one_min.x	24	1111110600	2017-02-02	100
"""

    assert TSV(result) == TSV(expected)


def test_multiple_paths_and_versions(graphite_table):
    result = q(
        """
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
"""
    )

    with open(
        p.join(p.dirname(__file__), "test_multiple_paths_and_versions.reference")
    ) as reference:
        assert TSV(result) == TSV(reference)


def test_multiple_output_blocks(graphite_table):
    MERGED_BLOCK_SIZE = 8192

    to_insert = ""
    expected = ""
    for i in range(2 * MERGED_BLOCK_SIZE + 1):
        rolled_up_time = 1000000200 + 600 * i

        for j in range(3):
            cur_time = rolled_up_time + 100 * j
            to_insert += "one_min.x1	{}	{}	2001-09-09	1\n".format(10 * j, cur_time)
            to_insert += "one_min.x1	{}	{}	2001-09-09	2\n".format(
                10 * (j + 1), cur_time
            )

        expected += "one_min.x1	20	{}	2001-09-09	2\n".format(rolled_up_time)

    q("INSERT INTO test.graphite FORMAT TSV", to_insert)

    result = q(
        """
OPTIMIZE TABLE test.graphite PARTITION 200109 FINAL;

SELECT * FROM test.graphite;
"""
    )

    assert TSV(result) == TSV(expected)


def test_paths_not_matching_any_pattern(graphite_table):
    to_insert = """\
one_min.x1	100	1000000000	2001-09-09	1
zzzzzzzz	100	1000000001	2001-09-09	1
zzzzzzzz	200	1000000001	2001-09-09	2
"""

    q("INSERT INTO test.graphite FORMAT TSV", to_insert)

    expected = """\
one_min.x1	100	999999600	2001-09-09	1
zzzzzzzz	200	1000000001	2001-09-09	2
"""

    result = q(
        """
OPTIMIZE TABLE test.graphite PARTITION 200109 FINAL;

SELECT * FROM test.graphite;
"""
    )

    assert TSV(result) == TSV(expected)


def test_system_graphite_retentions(graphite_table):
    expected = """
graphite_rollup	all	\\\\.count$	sum	0	0	1	0	['test']	['graphite']
graphite_rollup	all	\\\\.max$	max	0	0	2	0	['test']	['graphite']
graphite_rollup	all	^five_min\\\\.		31536000	14400	3	0	['test']	['graphite']
graphite_rollup	all	^five_min\\\\.		5184000	3600	3	0	['test']	['graphite']
graphite_rollup	all	^five_min\\\\.		0	300	3	0	['test']	['graphite']
graphite_rollup	all	^one_min	avg	31536000	600	4	0	['test']	['graphite']
graphite_rollup	all	^one_min	avg	7776000	300	4	0	['test']	['graphite']
graphite_rollup	all	^one_min	avg	0	60	4	0	['test']	['graphite']
    """
    result = q("SELECT * from system.graphite_retentions")

    mismatch = csv_compare(result, expected)
    assert len(mismatch) == 0, f"got\n{result}\nwant\n{expected}\ndiff\n{mismatch}\n"

    q(
        """
DROP TABLE IF EXISTS test.graphite2;
CREATE TABLE test.graphite2
    (metric String, value Float64, timestamp UInt32, date Date, updated UInt32)
    ENGINE = GraphiteMergeTree('graphite_rollup')
    PARTITION BY toYYYYMM(date)
    ORDER BY (metric, timestamp)
    SETTINGS index_granularity=8192;
    """
    )
    expected = """
graphite_rollup	['test','test']	['graphite','graphite2']
graphite_rollup	['test','test']	['graphite','graphite2']
graphite_rollup	['test','test']	['graphite','graphite2']
graphite_rollup	['test','test']	['graphite','graphite2']
graphite_rollup	['test','test']	['graphite','graphite2']
graphite_rollup	['test','test']	['graphite','graphite2']
graphite_rollup	['test','test']	['graphite','graphite2']
graphite_rollup	['test','test']	['graphite','graphite2']
    """
    result = q(
        """
    SELECT
        config_name,
        Tables.database,
        Tables.table
    FROM system.graphite_retentions
    """
    )
    assert TSV(result) == TSV(expected)


def test_path_dangling_pointer(graphite_table):
    q(
        """
DROP TABLE IF EXISTS test.graphite2;
CREATE TABLE test.graphite2
    (metric String, value Float64, timestamp UInt32, date Date, updated UInt32)
    ENGINE = GraphiteMergeTree('graphite_rollup')
    PARTITION BY toYYYYMM(date)
    ORDER BY (metric, timestamp)
    SETTINGS index_granularity=1;
    """
    )

    path = "abcd" * 4000000  # 16MB
    q(
        "INSERT INTO test.graphite2 FORMAT TSV",
        "{}\t0.0\t0\t2018-01-01\t100\n".format(path),
    )
    q(
        "INSERT INTO test.graphite2 FORMAT TSV",
        "{}\t0.0\t0\t2018-01-01\t101\n".format(path),
    )
    for version in range(10):
        q(
            "INSERT INTO test.graphite2 FORMAT TSV",
            "{}\t0.0\t0\t2018-01-01\t{}\n".format(path, version),
        )

    while True:
        q("OPTIMIZE TABLE test.graphite2 PARTITION 201801 FINAL")
        parts = int(
            q(
                "SELECT count() FROM system.parts "
                "WHERE active AND database='test' "
                "AND table='graphite2'"
            )
        )
        if parts == 1:
            break
        print(("Parts", parts))

    assert TSV(q("SELECT value, timestamp, date, updated FROM test.graphite2")) == TSV(
        "0\t0\t2018-01-01\t101\n"
    )

    q("DROP TABLE test.graphite2")


def test_combined_rules(graphite_table):
    # 1487970000 ~ Sat 25 Feb 00:00:00 MSK 2017
    to_insert = "INSERT INTO test.graphite VALUES "
    expected_unmerged = ""
    for i in range(384):
        to_insert += "('five_min.count', {v}, {t}, toDate({t}), 1), ".format(
            v=1, t=1487970000 + (i * 300)
        )
        to_insert += "('five_min.max', {v}, {t}, toDate({t}), 1), ".format(
            v=i, t=1487970000 + (i * 300)
        )
        expected_unmerged += (
            "five_min.count\t{v1}\t{t}\n" "five_min.max\t{v2}\t{t}\n"
        ).format(v1=1, v2=i, t=1487970000 + (i * 300))

    q(to_insert)
    assert TSV(
        q(
            "SELECT metric, value, timestamp FROM test.graphite"
            " ORDER BY (timestamp, metric)"
        )
    ) == TSV(expected_unmerged)

    q("OPTIMIZE TABLE test.graphite PARTITION 201702 FINAL")
    expected_merged = """
        five_min.count	48	1487970000	2017-02-25	1
        five_min.count	48	1487984400	2017-02-25	1
        five_min.count	48	1487998800	2017-02-25	1
        five_min.count	48	1488013200	2017-02-25	1
        five_min.count	48	1488027600	2017-02-25	1
        five_min.count	48	1488042000	2017-02-25	1
        five_min.count	48	1488056400	2017-02-26	1
        five_min.count	48	1488070800	2017-02-26	1
        five_min.max	47	1487970000	2017-02-25	1
        five_min.max	95	1487984400	2017-02-25	1
        five_min.max	143	1487998800	2017-02-25	1
        five_min.max	191	1488013200	2017-02-25	1
        five_min.max	239	1488027600	2017-02-25	1
        five_min.max	287	1488042000	2017-02-25	1
        five_min.max	335	1488056400	2017-02-26	1
        five_min.max	383	1488070800	2017-02-26	1
    """
    assert TSV(q("SELECT * FROM test.graphite" " ORDER BY (metric, timestamp)")) == TSV(
        expected_merged
    )


def test_combined_rules_with_default(graphite_table):
    q(
        """
DROP TABLE IF EXISTS test.graphite;
CREATE TABLE test.graphite
    (metric String, value Float64, timestamp UInt32, date Date, updated UInt32)
    ENGINE = GraphiteMergeTree('graphite_rollup_with_default')
    PARTITION BY toYYYYMM(date)
    ORDER BY (metric, timestamp)
    SETTINGS index_granularity=1;
      """
    )
    # 1487970000 ~ Sat 25 Feb 00:00:00 MSK 2017
    to_insert = "INSERT INTO test.graphite VALUES "
    expected_unmerged = ""
    for i in range(100):
        to_insert += "('top_level.count', {v}, {t}, toDate({t}), 1), ".format(
            v=1, t=1487970000 + (i * 60)
        )
        to_insert += "('top_level.max', {v}, {t}, toDate({t}), 1), ".format(
            v=i, t=1487970000 + (i * 60)
        )
        expected_unmerged += (
            "top_level.count\t{v1}\t{t}\n" "top_level.max\t{v2}\t{t}\n"
        ).format(v1=1, v2=i, t=1487970000 + (i * 60))

    q(to_insert)
    assert TSV(
        q(
            "SELECT metric, value, timestamp FROM test.graphite"
            " ORDER BY (timestamp, metric)"
        )
    ) == TSV(expected_unmerged)

    q("OPTIMIZE TABLE test.graphite PARTITION 201702 FINAL")
    expected_merged = """
        top_level.count	10	1487970000	2017-02-25	1
        top_level.count	10	1487970600	2017-02-25	1
        top_level.count	10	1487971200	2017-02-25	1
        top_level.count	10	1487971800	2017-02-25	1
        top_level.count	10	1487972400	2017-02-25	1
        top_level.count	10	1487973000	2017-02-25	1
        top_level.count	10	1487973600	2017-02-25	1
        top_level.count	10	1487974200	2017-02-25	1
        top_level.count	10	1487974800	2017-02-25	1
        top_level.count	10	1487975400	2017-02-25	1
        top_level.max	9	1487970000	2017-02-25	1
        top_level.max	19	1487970600	2017-02-25	1
        top_level.max	29	1487971200	2017-02-25	1
        top_level.max	39	1487971800	2017-02-25	1
        top_level.max	49	1487972400	2017-02-25	1
        top_level.max	59	1487973000	2017-02-25	1
        top_level.max	69	1487973600	2017-02-25	1
        top_level.max	79	1487974200	2017-02-25	1
        top_level.max	89	1487974800	2017-02-25	1
        top_level.max	99	1487975400	2017-02-25	1
    """
    assert TSV(q("SELECT * FROM test.graphite" " ORDER BY (metric, timestamp)")) == TSV(
        expected_merged
    )


def test_broken_partial_rollup(graphite_table):
    q(
        """
DROP TABLE IF EXISTS test.graphite;
CREATE TABLE test.graphite
    (metric String, value Float64, timestamp UInt32, date Date, updated UInt32)
    ENGINE = GraphiteMergeTree('graphite_rollup_broken')
    PARTITION BY toYYYYMM(date)
    ORDER BY (metric, timestamp)
    SETTINGS index_granularity=1;
      """
    )
    to_insert = """\
one_min.x1	100	1000000000	2001-09-09	1
zzzzzzzz	100	1000000001	2001-09-09	1
zzzzzzzz	200	1000000001	2001-09-09	2
"""

    q("INSERT INTO test.graphite FORMAT TSV", to_insert)

    expected = """\
one_min.x1	100	1000000000	2001-09-09	1
zzzzzzzz	200	1000000001	2001-09-09	2
"""

    result = q(
        """
OPTIMIZE TABLE test.graphite PARTITION 200109 FINAL;

SELECT * FROM test.graphite;
"""
    )

    assert TSV(result) == TSV(expected)


def test_wrong_rollup_config(graphite_table):
    with pytest.raises(QueryRuntimeException) as exc:
        q(
            """
CREATE TABLE test.graphite_not_created
    (metric String, value Float64, timestamp UInt32, date Date, updated UInt32)
    ENGINE = GraphiteMergeTree('graphite_rollup_wrong_age_precision')
    PARTITION BY toYYYYMM(date)
    ORDER BY (metric, timestamp)
    SETTINGS index_granularity=1;
        """
        )

    # The order of retentions is not guaranteed
    assert "Age and precision should only grow up: " in str(exc.value)
    assert "36000:600" in str(exc.value)
    assert "72000:300" in str(exc.value)
