-- Tags: no-old-analyzer

-- Switching between join algorithms: there are no size thresholds for IEJoin yet, so the
-- switch is controlled by the `join_algorithm` list; with `ie_join` listed last, a null-safe
-- equality keeps the join on the hash path.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

DROP TABLE IF EXISTS bigtbl;
DROP TABLE IF EXISTS smalltbl;

CREATE TABLE bigtbl ENGINE = MergeTree ORDER BY tuple() AS SELECT toInt64(number) AS i FROM numbers(1000);
CREATE TABLE smalltbl ENGINE = MergeTree ORDER BY tuple() AS SELECT toInt64(number) AS low, toInt64(number + 1) AS high FROM numbers(100);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM bigtbl JOIN smalltbl ON bigtbl.i BETWEEN low AND high) WHERE explain LIKE '%IEJoin%';
SELECT count() FROM bigtbl JOIN smalltbl ON bigtbl.i BETWEEN low AND high;

-- Without `ie_join` in the list the same query runs as a cross join with a filter
SELECT count() FROM (EXPLAIN actions = 1 SELECT count() FROM bigtbl JOIN smalltbl ON bigtbl.i BETWEEN low AND high SETTINGS join_algorithm = 'direct,parallel_hash,hash') WHERE explain LIKE '%IEJoin%';
SELECT count() FROM bigtbl JOIN smalltbl ON bigtbl.i BETWEEN low AND high SETTINGS join_algorithm = 'direct,parallel_hash,hash';

-- A null-safe equality condition makes it a hash join because `ie_join` is listed last
SELECT count() FROM (EXPLAIN actions = 1 SELECT count() FROM bigtbl JOIN smalltbl ON (bigtbl.i BETWEEN low AND high) AND (bigtbl.i IS NOT DISTINCT FROM high - low)) WHERE explain LIKE '%IEJoin%';
SELECT count() FROM bigtbl JOIN smalltbl ON (bigtbl.i BETWEEN low AND high) AND (bigtbl.i IS NOT DISTINCT FROM high - low);

DROP TABLE bigtbl;
DROP TABLE smalltbl;
