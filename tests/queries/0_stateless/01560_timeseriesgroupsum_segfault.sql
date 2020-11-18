DROP TABLE IF EXISTS tsgs_local;
DROP TABLE IF EXISTS tsgs;

CREATE TABLE tsgs_local ENGINE = MergeTree ORDER BY tuple() AS
SELECT
    toUInt64(13820745146630357293) AS a,
    toInt64(1604422500000000000) AS b,
    toFloat64(0) AS c
FROM numbers(100);

-- the issue (https://github.com/ClickHouse/ClickHouse/issues/16862) happens during serialization of the state
-- so happens only when Distributed tables are used or with -State modifier.

CREATE TABLE tsgs AS tsgs_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), tsgs_local);

SELECT timeSeriesGroupSum(a, b, c) FROM tsgs;

SELECT count() FROM ( SELECT timeSeriesGroupSumState(a, b, c) as x FROM tsgs_local) WHERE NOT ignore(*);

SELECT 'server is still alive';

DROP TABLE tsgs_local;
DROP TABLE tsgs;
