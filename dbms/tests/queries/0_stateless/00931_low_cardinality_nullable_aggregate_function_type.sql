drop table if exists test.lc;

CREATE TABLE test.lc (`date` Date, `name` LowCardinality(Nullable(String)), `clicks` Nullable(Int32)) ENGINE = MergeTree() ORDER BY date SETTINGS index_granularity = 8192;
INSERT INTO test.lc SELECT '2019-01-01', null, 0 FROM numbers(1000000);
SELECT date, argMax(name, clicks) FROM test.lc GROUP BY date;

drop table if exists test.lc;

