-- Ported from DuckDB test/sql/join/iejoin/merge_join_switch.test: switching between join
-- algorithms. ClickHouse has no size thresholds for IEJoin yet, so the ported part checks
-- switching by the setting and that a null-safe equality keeps the join on the hash path.

SET allow_experimental_ie_join = 1;

DROP TABLE IF EXISTS bigtbl;
DROP TABLE IF EXISTS smalltbl;

CREATE TABLE bigtbl ENGINE = MergeTree ORDER BY tuple() AS SELECT toInt64(number) AS i FROM numbers(1000);
CREATE TABLE smalltbl ENGINE = MergeTree ORDER BY tuple() AS SELECT toInt64(number) AS low, toInt64(number + 1) AS high FROM numbers(100);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM bigtbl JOIN smalltbl ON bigtbl.i BETWEEN low AND high) WHERE explain LIKE '%IEJoin%';
SELECT count() FROM bigtbl JOIN smalltbl ON bigtbl.i BETWEEN low AND high;

-- With the setting disabled the same query runs as a cross join with a filter
SELECT count() FROM (EXPLAIN actions = 1 SELECT count() FROM bigtbl JOIN smalltbl ON bigtbl.i BETWEEN low AND high SETTINGS allow_experimental_ie_join = 0) WHERE explain LIKE '%IEJoin%';
SELECT count() FROM bigtbl JOIN smalltbl ON bigtbl.i BETWEEN low AND high SETTINGS allow_experimental_ie_join = 0;

-- A null-safe equality condition makes it a hash join even with the setting enabled
SELECT count() FROM (EXPLAIN actions = 1 SELECT count() FROM bigtbl JOIN smalltbl ON (bigtbl.i BETWEEN low AND high) AND (bigtbl.i IS NOT DISTINCT FROM high - low)) WHERE explain LIKE '%IEJoin%';
SELECT count() FROM bigtbl JOIN smalltbl ON (bigtbl.i BETWEEN low AND high) AND (bigtbl.i IS NOT DISTINCT FROM high - low);

DROP TABLE bigtbl;
DROP TABLE smalltbl;
