-- With `group_by_use_nulls` the keys of grouping sets are `Nullable` in the output of the aggregation,
-- so `optimize_aggregators_of_group_by_keys` must not replace `any` of a key with the key itself:
-- the outer `sum` read the `Nullable` column as a plain one, which gave a wrong result or an exception.

DROP TABLE IF EXISTS t_aggregators_of_keys_use_nulls;
CREATE TABLE t_aggregators_of_keys_use_nulls (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_aggregators_of_keys_use_nulls SELECT number, number % 100 FROM numbers(10000);

SET group_by_use_nulls = 1, optimize_aggregators_of_group_by_keys = 1;

SELECT sum(s), count() FROM (SELECT b AS k, anyHeavy(b) AS s FROM t_aggregators_of_keys_use_nulls GROUP BY GROUPING SETS ((k)));
SELECT sum(s), count() FROM (SELECT b AS k, any(b) AS s FROM t_aggregators_of_keys_use_nulls GROUP BY GROUPING SETS ((k)));
SELECT sum(s), count() FROM (SELECT b AS k, max(b) AS s FROM t_aggregators_of_keys_use_nulls GROUP BY GROUPING SETS ((k), ()));

DROP TABLE t_aggregators_of_keys_use_nulls;
