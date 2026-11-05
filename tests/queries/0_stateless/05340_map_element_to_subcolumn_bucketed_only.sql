-- `m['key']` is rewritten to the Map key subcolumn `m.key_<key>` only when the table serializes Maps
-- with buckets, because only then can a single key be read without the whole Map.
-- Both serialization settings are named in every table, clickhouse-test randomizes them.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_map_buffer;
DROP TABLE IF EXISTS t_map_mv;
DROP TABLE IF EXISTS t_map_merge;
DROP TABLE IF EXISTS t_map_memory;
DROP TABLE IF EXISTS t_map_basic;
DROP TABLE IF EXISTS t_map_zero_level_bucketed;
DROP TABLE IF EXISTS t_map_bucketed;

CREATE TABLE t_map_bucketed (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
CREATE TABLE t_map_zero_level_bucketed (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'with_buckets';
CREATE TABLE t_map_basic (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';
CREATE TABLE t_map_memory (id UInt64, m Map(String, UInt64)) ENGINE = Memory;

INSERT INTO t_map_bucketed SELECT number, map('k', number) FROM numbers(10);
INSERT INTO t_map_zero_level_bucketed SELECT number, map('k', number) FROM numbers(10);
INSERT INTO t_map_basic SELECT number, map('k', number) FROM numbers(10);
INSERT INTO t_map_memory SELECT number, map('k', number) FROM numbers(10);

SELECT 'bucketed MergeTree';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_bucketed WHERE m['k'] > 7) WHERE explain LIKE '%m.key_k%';
SELECT 'MergeTree with bucketed zero-level parts';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_zero_level_bucketed WHERE m['k'] > 7) WHERE explain LIKE '%m.key_k%';
SELECT 'basic MergeTree';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_basic WHERE m['k'] > 7) WHERE explain LIKE '%m.key_k%';
SELECT 'Memory';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_memory WHERE m['k'] > 7) WHERE explain LIKE '%m.key_k%';

SELECT 'explicit key subcolumn of a basic Map';
SELECT id, m.key_k FROM t_map_basic WHERE m.key_k > 7 ORDER BY id;

SELECT 'results';
SELECT id, m['k'] FROM t_map_bucketed WHERE m['k'] > 7 ORDER BY id;
SELECT id, m['k'] FROM t_map_basic WHERE m['k'] > 7 ORDER BY id;
SELECT id, m['k'] FROM t_map_memory WHERE m['k'] > 7 ORDER BY id;

-- Engines that pass the analyzed query to other tables answer for those tables.
CREATE TABLE t_map_merge (id UInt64, m Map(String, UInt64)) ENGINE = Merge(currentDatabase(), '^t_map_bucketed$');
CREATE MATERIALIZED VIEW t_map_mv (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets'
AS SELECT id, m FROM t_map_bucketed;
CREATE TABLE t_map_buffer (id UInt64, m Map(String, UInt64))
ENGINE = Buffer(currentDatabase(), t_map_bucketed, 1, 10, 100, 10000, 1000000, 10000000, 100000000);

SELECT 'Merge over a bucketed table';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_merge WHERE m['k'] > 7) WHERE explain LIKE '%m.key_k%';
SELECT 'MaterializedView with a bucketed target';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_mv WHERE m['k'] > 7) WHERE explain LIKE '%m.key_k%';
SELECT 'Buffer with a bucketed destination';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_buffer WHERE m['k'] > 7) WHERE explain LIKE '%m.key_k%';

-- The query is sent to every table of a Merge, so all of them must be bucketed.
DROP TABLE t_map_merge;
CREATE TABLE t_map_merge (id UInt64, m Map(String, UInt64)) ENGINE = Merge(currentDatabase(), '^t_map_(bucketed|basic)$');

SELECT 'Merge over a bucketed and a basic table';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_merge WHERE m['k'] > 7) WHERE explain LIKE '%m.key_k%';
SELECT id, m['k'] FROM t_map_merge WHERE m['k'] > 7 ORDER BY id;

DROP TABLE t_map_buffer;
DROP TABLE t_map_mv;
DROP TABLE t_map_merge;
DROP TABLE t_map_memory;
DROP TABLE t_map_basic;
DROP TABLE t_map_zero_level_bucketed;
DROP TABLE t_map_bucketed;
