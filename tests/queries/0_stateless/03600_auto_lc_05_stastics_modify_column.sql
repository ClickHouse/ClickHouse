DROP TABLE IF EXISTS t_stats_modify;
SET allow_experimental_statistics = 1;

CREATE TABLE t_stats_modify
(
    a UInt64,
    b UInt64 STATISTICS(tdigest)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS statistics_types = 'uniq, minmax';

INSERT INTO t_stats_modify SELECT number, number FROM numbers(1000);

SELECT column, type, statistics, estimated_cardinality FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_modify' AND active
ORDER BY column;

ALTER TABLE t_stats_modify MODIFY COLUMN a String;
ALTER TABLE t_stats_modify MODIFY COLUMN b String; -- { serverError ILLEGAL_STATISTICS }

SELECT column, type, statistics, estimated_cardinality FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_modify' AND active
ORDER BY column;

DROP TABLE IF EXISTS t_stats_modify;
