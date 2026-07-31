-- Tags: no-ordinary-database, no-fasttest, use-rocksdb

-- A key-value storage is looked up by its primary key, so a `WHERE` equality on any other column
-- must stay a filter. Promoting it to a second join key makes `join_algorithm = 'direct'` fail and
-- silently replaces the lookup with a full hash join under the default algorithms.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_merge_filter_into_join_condition = 1; -- CI may inject False

CREATE TABLE kv (k UInt64, payload Int32) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
CREATE TABLE lk (k UInt64, payload Nullable(Int32)) ENGINE = Memory;

INSERT INTO kv VALUES (1, 5), (2, 9);
INSERT INTO lk VALUES (1, 5), (2, 7);

SELECT '-- the lookup algorithm is kept';
SELECT extract(arrayStringConcat(groupArray(explain), '\n'), 'Algorithm: ([^\n]*)') AS algorithm
FROM (EXPLAIN SELECT * FROM lk ALL INNER JOIN kv ON lk.k = kv.k WHERE lk.payload = kv.payload);

SELECT extract(arrayStringConcat(groupArray(explain), '\n'), 'Algorithm: ([^\n]*)') AS algorithm
FROM (EXPLAIN SELECT * FROM lk, kv WHERE lk.k = kv.k AND lk.payload = kv.payload);

SELECT '-- the non-key equality is still applied';
SELECT * FROM lk ALL INNER JOIN kv ON lk.k = kv.k WHERE lk.payload = kv.payload ORDER BY ALL;

SELECT '-- joining on the primary key alone is unaffected';
SELECT extract(arrayStringConcat(groupArray(explain), '\n'), 'Algorithm: ([^\n]*)') AS algorithm
FROM (EXPLAIN SELECT * FROM lk ALL INNER JOIN kv ON lk.k = kv.k);

SELECT '-- a join on no primary key falls back to a hash join';
SELECT extract(arrayStringConcat(groupArray(explain), '\n'), 'Type: (\\w+)') AS join_kind
FROM (EXPLAIN SELECT * FROM lk ALL INNER JOIN kv ON lk.payload = kv.payload SETTINGS join_algorithm = 'direct,hash');

SELECT * FROM lk ALL INNER JOIN kv ON lk.payload = kv.payload ORDER BY ALL SETTINGS join_algorithm = 'direct,hash';
