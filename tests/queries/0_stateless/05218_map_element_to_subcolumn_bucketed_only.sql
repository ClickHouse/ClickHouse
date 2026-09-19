-- `m['key']` is rewritten to the Map key subcolumn `m.key_<key>` only when the source table
-- serializes Maps with buckets, because only then can a single key be read on its own.
-- It is a read optimization, so results must not depend on it.
-- Both serialization versions are pinned in every DDL below, they are randomized in CI.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_map_bucketed;
DROP TABLE IF EXISTS t_map_basic;
DROP TABLE IF EXISTS t_map_memory;
DROP TABLE IF EXISTS t_map_merge;
DROP TABLE IF EXISTS t_map_mv;
DROP TABLE IF EXISTS t_map_buffer;

CREATE TABLE t_map_bucketed (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';

CREATE TABLE t_map_basic (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';

CREATE TABLE t_map_memory (id UInt64, m Map(String, UInt64)) ENGINE = Memory;

INSERT INTO t_map_bucketed SELECT number, map('k', number) FROM numbers(10);
INSERT INTO t_map_basic SELECT number, map('k', number) FROM numbers(10);
INSERT INTO t_map_memory SELECT number, map('k', number) FROM numbers(10);

SELECT '-- bucketed MergeTree: rewritten';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_bucketed WHERE m['k'] > 7) WHERE explain LIKE '%m.key_k%';

SELECT '-- basic MergeTree: not rewritten';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_basic WHERE m['k'] > 7) WHERE explain LIKE '%m.key_k%';

SELECT '-- Memory: not rewritten';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_memory WHERE m['k'] > 7) WHERE explain LIKE '%m.key_k%';

SELECT '-- results are identical';
SELECT id, m['k'] FROM t_map_bucketed WHERE m['k'] > 7 ORDER BY id;
SELECT id, m['k'] FROM t_map_basic WHERE m['k'] > 7 ORDER BY id;
SELECT id, m['k'] FROM t_map_memory WHERE m['k'] > 7 ORDER BY id;

-- Engines that forward the already-analyzed query to another table answer for that table.
CREATE TABLE t_map_merge (id UInt64, m Map(String, UInt64)) ENGINE = Merge(currentDatabase(), 't_map_bucketed');
CREATE MATERIALIZED VIEW t_map_mv (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets'
AS SELECT id, m FROM t_map_bucketed;
CREATE TABLE t_map_buffer (id UInt64, m Map(String, UInt64))
ENGINE = Buffer(currentDatabase(), t_map_bucketed, 1, 10, 100, 10000, 1000000, 10000000, 100000000);

SELECT '-- Merge over a bucketed table: rewritten';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_merge WHERE m['k'] > 7) WHERE explain LIKE '%m.key_k%';

SELECT '-- MaterializedView over a bucketed target: rewritten';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_mv WHERE m['k'] > 7) WHERE explain LIKE '%m.key_k%';

SELECT '-- Buffer over a bucketed destination: rewritten';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_buffer WHERE m['k'] > 7) WHERE explain LIKE '%m.key_k%';

-- A Merge table is rewritten only when every underlying table benefits from it.
DROP TABLE t_map_merge;
CREATE TABLE t_map_merge (id UInt64, m Map(String, UInt64)) ENGINE = Merge(currentDatabase(), 't_map_(bucketed|basic)');

SELECT '-- Merge over a bucketed and a basic table: not rewritten';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_merge WHERE m['k'] > 7) WHERE explain LIKE '%m.key_k%';

SELECT '-- Merge results';
SELECT id, m['k'] FROM t_map_merge WHERE m['k'] > 7 ORDER BY id;

DROP TABLE t_map_buffer;
DROP TABLE t_map_mv;
DROP TABLE t_map_merge;
DROP TABLE t_map_memory;
DROP TABLE t_map_basic;
DROP TABLE t_map_bucketed;
