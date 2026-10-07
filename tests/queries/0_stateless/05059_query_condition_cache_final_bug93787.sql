-- Tags: no-parallel, no-ordinary-database, no-parallel-replicas
-- no-parallel: clears and inspects the instance-wide query condition cache
-- no-ordinary-database: a database without table UUIDs never populates the cache
-- no-parallel-replicas: the cache is populated per replica, so this needs a single replica

-- `PREWHERE` runs before `FINAL`, so it can record marks that hold only losing versions. A `FINAL`
-- read must never reuse those marks: a row that loses the filter can still be the row that wins
-- deduplication. See https://github.com/ClickHouse/ClickHouse/issues/93787

SET enable_analyzer = 1; -- the query condition cache is analyzer-only
SET async_insert = 0; -- the two versions must land in two separate parts

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id UInt64, c1 Int8) ENGINE = ReplacingMergeTree ORDER BY id;
SYSTEM STOP MERGES tab;
INSERT INTO tab VALUES (1, 0);
INSERT INTO tab VALUES (1, 1);
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active;

SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT count() FROM tab FINAL WHERE c1 = 0 SETTINGS use_query_condition_cache = 1;
SELECT count() FROM tab FINAL PREWHERE c1 = 0 SETTINGS use_query_condition_cache = 1 FORMAT Null;
SELECT countIf(startsWith(matching_marks, '0')) FROM system.query_condition_cache;
SELECT count() FROM tab FINAL WHERE c1 = 0 SETTINGS use_query_condition_cache = 1;

DROP TABLE tab;
