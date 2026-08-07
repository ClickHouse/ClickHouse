-- Tags: no-old-analyzer, no-replicated-database
-- no-old-analyzer: the reference pins EXPLAIN output of the analyzer plan.
-- no-replicated-database: for the lazy_load_tables section below.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET allow_experimental_alias_table_engine = 1;

DROP TABLE IF EXISTS t04741_dist_leaf_a;
DROP TABLE IF EXISTS t04741_dist_leaf_b;
DROP TABLE IF EXISTS t04741_dist_a;
DROP TABLE IF EXISTS t04741_dist_b;
DROP TABLE IF EXISTS t04741_merge_leaf_a;
DROP TABLE IF EXISTS t04741_merge_leaf_b;
DROP TABLE IF EXISTS t04741_merge_child;
DROP TABLE IF EXISTS t04741_buffer_dest;
DROP TABLE IF EXISTS t04741_buffer_child;
DROP TABLE IF EXISTS t04741_alias_target;
DROP TABLE IF EXISTS t04741_alias_child;
DROP TABLE IF EXISTS t04741_memory_target;
DROP TABLE IF EXISTS t04741_alias_memory_child;
DROP TABLE IF EXISTS t04741_plain_a;
DROP TABLE IF EXISTS t04741_plain_b;
DROP TABLE IF EXISTS t04741_view_src;
DROP TABLE IF EXISTS t04741_view_child;
DROP TABLE IF EXISTS t04741_mv_target;
DROP TABLE IF EXISTS t04741_mv_child;
DROP TABLE IF EXISTS t04741_shadow;
DROP TABLE IF EXISTS t04741_dist_other_db;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

-- Distinct row counts per table so a wrong answer differs by value, not only by emptiness.
CREATE TABLE t04741_dist_leaf_a (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t04741_dist_leaf_b (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t04741_dist_leaf_a SELECT number FROM numbers(10);
INSERT INTO t04741_dist_leaf_b SELECT number FROM numbers(100);
CREATE TABLE t04741_dist_a AS t04741_dist_leaf_a
    ENGINE = Distributed('test_shard_localhost', currentDatabase(), t04741_dist_leaf_a, rand());
CREATE TABLE t04741_dist_b AS t04741_dist_leaf_b
    ENGINE = Distributed('test_shard_localhost', currentDatabase(), t04741_dist_leaf_b, rand());

CREATE TABLE t04741_merge_leaf_a (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t04741_merge_leaf_b (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t04741_merge_leaf_a SELECT number FROM numbers(10);
INSERT INTO t04741_merge_leaf_b SELECT number FROM numbers(100);
CREATE TABLE t04741_merge_child (x UInt32) ENGINE = Merge(currentDatabase(), '^t04741_merge_leaf_[ab]$');

CREATE TABLE t04741_buffer_dest (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t04741_buffer_dest SELECT number FROM numbers(100);
CREATE TABLE t04741_buffer_child (x UInt32)
    ENGINE = Buffer(currentDatabase(), t04741_buffer_dest, 1, 3600, 3600, 100000, 1000000, 10000000, 100000000);
-- Also insert into the Buffer itself so both of its row groups are live: buffered rows carry the
-- Buffer's own name, destination rows carry the destination's. The thresholds prevent a flush.
INSERT INTO t04741_buffer_child SELECT number FROM numbers(13);

CREATE TABLE t04741_alias_target (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t04741_alias_target SELECT number FROM numbers(100);
CREATE TABLE t04741_alias_child ENGINE = Alias(currentDatabase(), t04741_alias_target);

CREATE TABLE t04741_memory_target (x UInt32) ENGINE = Memory;
INSERT INTO t04741_memory_target SELECT number FROM numbers(50);
CREATE TABLE t04741_alias_memory_child ENGINE = Alias(currentDatabase(), t04741_memory_target);

CREATE TABLE t04741_plain_a (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t04741_plain_b (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t04741_plain_a SELECT number FROM numbers(10);
INSERT INTO t04741_plain_b SELECT number FROM numbers(100);

CREATE TABLE t04741_view_src (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t04741_view_src SELECT number FROM numbers(30);
CREATE VIEW t04741_view_child AS SELECT x FROM t04741_view_src;

CREATE TABLE t04741_mv_target (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW t04741_mv_child TO t04741_mv_target AS SELECT x FROM t04741_view_src;
INSERT INTO t04741_mv_target SELECT number FROM numbers(20);

CREATE TABLE t04741_shadow (`_table` Int32, x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t04741_shadow VALUES (6, 1), (7, 2);

-- A leaf in another database, so the leaf's _database differs from the Merge parent's.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t04741_other_db_leaf (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t04741_other_db_leaf SELECT number FROM numbers(7);
CREATE TABLE t04741_dist_other_db (x UInt32)
    ENGINE = Distributed('test_shard_localhost', {CLICKHOUSE_DATABASE_1:String}, t04741_other_db_leaf, rand());

SELECT '-- arm 1: Distributed child at FetchColumns, filter the leaf name';
SELECT m._table, count() FROM merge(currentDatabase(), '^t04741_dist_[ab]$') AS m
    ARRAY JOIN [1, 2] AS z WHERE m._table = 't04741_dist_leaf_b' GROUP BY 1 ORDER BY 1;

SELECT '-- arm 1 control: same query with no _table filter';
SELECT m._table, count() FROM merge(currentDatabase(), '^t04741_dist_[ab]$') AS m
    ARRAY JOIN [1, 2] AS z GROUP BY 1 ORDER BY 1;

SELECT '-- arm 2: Merge child, filter the inner leaf name';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_merge_child$')
    WHERE _table = 't04741_merge_leaf_b' GROUP BY 1 ORDER BY 1;

SELECT '-- arm 2 control: same query with no _table filter';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_merge_child$') GROUP BY 1 ORDER BY 1;

SELECT '-- arm 3: Buffer child, filter the destination name';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_buffer_child$')
    WHERE _table = 't04741_buffer_dest' GROUP BY 1 ORDER BY 1;

SELECT '-- arm 3b: Buffer child, filter the Buffer own name, keeps only the buffered rows';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_buffer_child$')
    WHERE _table = 't04741_buffer_child' GROUP BY 1 ORDER BY 1;

SELECT '-- arm 3 control: same query with no _table filter';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_buffer_child$') GROUP BY 1 ORDER BY 1;

SELECT '-- arm 4: Alias child over MergeTree, filter the target name';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_alias_child$')
    WHERE _table = 't04741_alias_target' GROUP BY 1 ORDER BY 1;

SELECT '-- arm 4 control: same query with no _table filter';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_alias_child$') GROUP BY 1 ORDER BY 1;

SELECT '-- arm 5: _database of a cross-database Distributed child, filter the leaf database';
SELECT m._database = {CLICKHOUSE_DATABASE_1:String}, m._table, count()
    FROM merge(currentDatabase(), '^t04741_dist_other_db$') AS m
    ARRAY JOIN [1, 2] AS z WHERE m._database = {CLICKHOUSE_DATABASE_1:String} GROUP BY 1, 2 ORDER BY 2;

SELECT '-- arm 5 control: same query with no _database filter';
SELECT m._database = {CLICKHOUSE_DATABASE_1:String}, m._table, count()
    FROM merge(currentDatabase(), '^t04741_dist_other_db$') AS m
    ARRAY JOIN [1, 2] AS z GROUP BY 1, 2 ORDER BY 2;

SELECT '-- arm 5 control: the Merge parent database does not match the leaf database';
SELECT count() FROM merge(currentDatabase(), '^t04741_dist_other_db$') AS m
    ARRAY JOIN [1, 2] AS z WHERE m._database = currentDatabase();

SELECT '-- arm 6: negation of a Distributed child own name keeps both leaves';
SELECT m._table, count() FROM merge(currentDatabase(), '^t04741_dist_[ab]$') AS m
    ARRAY JOIN [1, 2] AS z WHERE m._table != 't04741_dist_a' GROUP BY 1 ORDER BY 1;

SELECT '-- arm 6 control: negation of a leaf name excludes exactly that leaf';
SELECT m._table, count() FROM merge(currentDatabase(), '^t04741_dist_[ab]$') AS m
    ARRAY JOIN [1, 2] AS z WHERE m._table != 't04741_dist_leaf_a' GROUP BY 1 ORDER BY 1;

SELECT '-- arm 6b: IN over the inner leaves of a Merge child';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_merge_child$')
    WHERE _table IN ('t04741_merge_leaf_a', 't04741_merge_leaf_b') GROUP BY 1 ORDER BY 1;

SELECT '-- arm 7 control: MergeTree children keep being pruned by name';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_plain_[ab]$')
    WHERE _table = 't04741_plain_a' GROUP BY 1 ORDER BY 1;

SELECT '-- arm 7 control: the pruned MergeTree leaf is absent from the plan';
SELECT count() > 0 FROM (EXPLAIN indexes = 1
    SELECT _table, count() FROM merge(currentDatabase(), '^t04741_plain_[ab]$')
        WHERE _table = 't04741_plain_a' GROUP BY 1)
    WHERE explain ILIKE '%ReadFromMergeTree (' || currentDatabase() || '.t04741_plain_a)%';
SELECT count() FROM (EXPLAIN indexes = 1
    SELECT _table, count() FROM merge(currentDatabase(), '^t04741_plain_[ab]$')
        WHERE _table = 't04741_plain_a' GROUP BY 1)
    WHERE explain ILIKE '%t04741_plain_b%';

SELECT '-- arm 8 control: pruning of a name that matches nothing reads no rows';
SELECT count() FROM merge(currentDatabase(), '^t04741_plain_[ab]$') WHERE _table = 't04741_nosuch';

SELECT '-- arm 9 control: a View child is pruned by its own name';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_view_child$')
    WHERE _table = 't04741_view_child' GROUP BY 1 ORDER BY 1;

SELECT '-- arm 9 control: a MaterializedView child is pruned by its own name';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_mv_child$')
    WHERE _table = 't04741_mv_child' GROUP BY 1 ORDER BY 1;

SELECT '-- arm 9b: Alias child over Memory, filter the target name';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_alias_memory_child$')
    WHERE _table = 't04741_memory_target' GROUP BY 1 ORDER BY 1;

SELECT '-- arm 9b control: same query with no _table filter';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_alias_memory_child$') GROUP BY 1 ORDER BY 1;

SELECT '-- arm 10 characterization: a physical _table column keeps shadowing the virtual one';
SELECT toTypeName(_table), _table, x FROM merge(currentDatabase(), '^t04741_shadow$')
    WHERE _table = 6 ORDER BY x;

SELECT '-- arm 11: an admitted delegating child still prunes, one level lower where the names are right';
SELECT count() > 0 FROM (EXPLAIN indexes = 1
    SELECT _table, count() FROM merge(currentDatabase(), '^t04741_merge_child$')
        WHERE _table = 't04741_merge_leaf_b' GROUP BY 1)
    WHERE explain ILIKE '%ReadFromMergeTree (' || currentDatabase() || '.t04741_merge_leaf_b)%';
SELECT count() FROM (EXPLAIN indexes = 1
    SELECT _table, count() FROM merge(currentDatabase(), '^t04741_merge_child$')
        WHERE _table = 't04741_merge_leaf_b' GROUP BY 1)
    WHERE explain ILIKE '%t04741_merge_leaf_a%';

-- With `lazy_load_tables = 1` the object attached in the database is a `StorageTableProxy`, and it
-- stays one for the process lifetime, so neither the MergeTree nor the common-virtual-columns test
-- recognises it. It is classified by recursing into the nested storage through `tryGetNested`,
-- which returns it only when it is already materialized and so never forces a lazy load. A nested
-- MergeTree is not streaming, so the proxy is admitted. Arm C is why admitting is the right answer
-- -- a proxy over a delegating storage carries its grandchildren's names. Arms A and B pay for it
-- with a lost pruning of a proxied MergeTree, which still answers correctly because the per-row
-- filter applies above it.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.t04741_lazy_a (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.t04741_lazy_b (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.t04741_lazy_a SELECT number FROM numbers(11);
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.t04741_lazy_b SELECT number FROM numbers(101);

CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.t04741_lzm_a (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.t04741_lzm_b (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.t04741_lzm_a SELECT number FROM numbers(17);
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.t04741_lzm_b SELECT number FROM numbers(103);
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.t04741_lzm_child (x UInt32)
    ENGINE = Merge({CLICKHOUSE_DATABASE_2:String}, '^t04741_lzm_[ab]$');

DETACH DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_2:Identifier};

USE {CLICKHOUSE_DATABASE_2:Identifier};

SELECT '-- arm A positive control: the children really are lazy proxies';
SELECT name, engine FROM system.tables
    WHERE database = currentDatabase() AND name LIKE 't04741_lazy_%' ORDER BY name;

SELECT '-- arm A: a lazily-proxied MergeTree child answers correctly';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_lazy_[ab]$')
    WHERE _table = 't04741_lazy_a' GROUP BY 1 ORDER BY 1;

SELECT '-- arm B: an admitted lazy proxy reads both children and the per-row filter decides';
SELECT count() > 0 FROM (EXPLAIN indexes = 1
    SELECT _table, count() FROM merge(currentDatabase(), '^t04741_lazy_[ab]$')
        WHERE _table = 't04741_lazy_a' GROUP BY 1)
    WHERE explain ILIKE '%t04741_lazy_a%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1
    SELECT _table, count() FROM merge(currentDatabase(), '^t04741_lazy_[ab]$')
        WHERE _table = 't04741_lazy_a' GROUP BY 1)
    WHERE explain ILIKE '%t04741_lazy_b%';

SELECT '-- arm C positive control: the Merge child really is a lazy proxy';
SELECT name, engine FROM system.tables
    WHERE database = currentDatabase() AND name LIKE 't04741_lzm_%' ORDER BY name;

SELECT '-- arm C: a proxy over a Merge carries its grandchildren names, so it must not be pruned';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_lzm_child$')
    WHERE _table = 't04741_lzm_b' GROUP BY 1 ORDER BY 1;

SELECT '-- arm C control: same query with no _table filter';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_lzm_child$') GROUP BY 1 ORDER BY 1;

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};

-- The file-like engines (`File`, `URL`, the object storages) declare `_table` at `Reader` place and
-- stamp it from their own id, so they may be pruned by a `_table` predicate. They do not declare
-- `_database`, so their rows carry an empty one and a `_database` predicate must NOT prune them: arms
-- F and G are the same child under the two predicates. `t04741_file_child` names a path that never
-- exists, so opening it raises FILE_DOESNT_EXIST at the default `engine_file_empty_if_not_exists`,
-- which makes the pruning observable: arm F succeeds only because the child is never opened.
DROP TABLE IF EXISTS t04741_file_child;
DROP TABLE IF EXISTS t04741_file_sibling;
CREATE TABLE t04741_file_sibling (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t04741_file_sibling SELECT number FROM numbers(23);
CREATE TABLE t04741_file_child (x UInt32) ENGINE = File(TSV, '04741_no_such_file.tsv');

SELECT '-- arm F: a _table filter prunes the file-like child, so its missing file is never opened';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_file_(child|sibling)$')
    WHERE _table = 't04741_file_sibling' GROUP BY 1 ORDER BY 1;

SELECT '-- arm F control: with no filter the same query opens the child and fails';
SELECT count() FROM merge(currentDatabase(), '^t04741_file_(child|sibling)$'); -- { serverError FILE_DOESNT_EXIST }

-- Arm G filters for the EMPTY `_database` the child's rows actually carry, which its own database
-- name never matches, so the prefilter would exclude it. A `_database = currentDatabase()` arm would
-- not discriminate: that name matches, so the child is kept whether or not the per-column rule holds.
SELECT '-- arm G: a _database filter must not prune it, because its rows carry an empty _database';
SELECT count() FROM merge(currentDatabase(), '^t04741_file_(child|sibling)$')
    WHERE _database = ''; -- { serverError FILE_DOESNT_EXIST }

SELECT '-- arm H: reading the file-like child by its own name still selects it';
INSERT INTO TABLE FUNCTION file('04741_present.tsv', TSV, 'x UInt32') SELECT number FROM numbers(19)
    SETTINGS engine_file_truncate_on_insert = 1;
CREATE TABLE t04741_file_present (x UInt32) ENGINE = File(TSV, '04741_present.tsv');
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_file_(present|sibling)$')
    WHERE _table = 't04741_file_present' GROUP BY 1 ORDER BY 1;

-- `_path` here is a physical column, so `getVirtualsForFileLikeStorage` omits the `_path` virtual and
-- the child no longer declares the whole family. It still stamps its own `_table`, so it stays
-- prunable: master answers this query, and reading the absent file would raise FILE_DOESNT_EXIST.
SELECT '-- arm J: a file-like child whose family virtual is shadowed physically is still pruned';
CREATE TABLE t04741_file_shadow (x UInt32, _path String) ENGINE = File(TSV, '04741_no_such_file_2.tsv');
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_file_(shadow|sibling)$')
    WHERE _table = 't04741_file_sibling' GROUP BY 1 ORDER BY 1;

SELECT '-- arm J control: with no filter the same query opens the shadowed child and fails';
SELECT count() FROM merge(currentDatabase(), '^t04741_file_(shadow|sibling)$'); -- { serverError FILE_DOESNT_EXIST }

DROP TABLE t04741_file_present;
DROP TABLE t04741_file_shadow;

-- `loop` re-reads its inner storage forever and declares no `_database`/`_table` of its own, so a
-- query that names it cannot succeed either way and admitting an excluded one never terminates. Same
-- for a `WindowView`, which forwards the read to an arbitrary `TO` target: both must stay prunable.
DROP TABLE IF EXISTS t04741_loop_child;
DROP TABLE IF EXISTS t04741_loop_inner;
DROP TABLE IF EXISTS t04741_loop_sibling;
CREATE TABLE t04741_loop_inner (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t04741_loop_inner SELECT number FROM numbers(29);
CREATE TABLE t04741_loop_sibling (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t04741_loop_sibling SELECT number FROM numbers(31);
CREATE TABLE t04741_loop_child AS loop(currentDatabase(), 't04741_loop_inner');

SELECT '-- arm I: a _table filter prunes the loop child, so the query terminates';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_loop_(child|sibling)$')
    WHERE _table = 't04741_loop_sibling' GROUP BY 1 ORDER BY 1;

SELECT '-- arm I control: the pruned plan really does not read it';
SELECT count() FROM (EXPLAIN SELECT count() FROM merge(currentDatabase(), '^t04741_loop_(child|sibling)$')
    WHERE _table = 't04741_loop_sibling')
    WHERE explain ILIKE '%ReadFromLoop%';

SELECT '-- arm I control: without the filter it is admitted, and it really is readable';
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM merge(currentDatabase(), '^t04741_loop_(child|sibling)$'))
    WHERE explain ILIKE '%ReadFromLoop%';
SELECT count() FROM (SELECT x FROM merge(currentDatabase(), '^t04741_loop_child$') LIMIT 5);

DROP TABLE t04741_loop_child;
DROP TABLE t04741_loop_sibling;
DROP TABLE t04741_loop_inner;
DROP TABLE t04741_file_child;
DROP TABLE t04741_file_sibling;
DROP TABLE t04741_dist_other_db;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t04741_other_db_leaf;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE t04741_shadow;
DROP TABLE t04741_mv_child;
DROP TABLE t04741_mv_target;
DROP TABLE t04741_view_child;
DROP TABLE t04741_view_src;
DROP TABLE t04741_plain_b;
DROP TABLE t04741_plain_a;
DROP TABLE t04741_alias_memory_child;
DROP TABLE t04741_memory_target;
DROP TABLE t04741_alias_child;
DROP TABLE t04741_alias_target;
DROP TABLE t04741_buffer_child;
DROP TABLE t04741_buffer_dest;
DROP TABLE t04741_merge_child;
DROP TABLE t04741_merge_leaf_b;
DROP TABLE t04741_merge_leaf_a;
DROP TABLE t04741_dist_b;
DROP TABLE t04741_dist_a;
DROP TABLE t04741_dist_leaf_b;
DROP TABLE t04741_dist_leaf_a;
