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

-- Measured on all three binaries (master, this branch, the branch with the classifier reverted) this
-- reads 0, so it is a control and not a witness: it asserts that pruning an ordinary file-like child
-- is preserved. Arms F, G and J are the witnesses for the class. A count cannot be used here at all,
-- because an admitted child's rows are dropped by the per-row filter and the total is unchanged.
SELECT '-- arm H control: an excluded file-like child stays absent from the plan';
-- The path is left implicit on purpose: a `File` table without a path owns a directory under its own
-- database, so concurrent runs of this test (the flaky check runs it several times at once) cannot
-- truncate each other's data the way a shared `user_files` path does.
CREATE TABLE t04741_file_present (x UInt32) ENGINE = File(TSV);
INSERT INTO t04741_file_present SELECT number FROM numbers(19);
SELECT count() FROM (EXPLAIN SELECT count() FROM merge(currentDatabase(), '^t04741_file_(present|sibling)$')
    WHERE _table = 't04741_file_sibling')
    WHERE explain ILIKE '%ReadFromFile%';

SELECT '-- arm H control: without the filter it is admitted, and it really is readable';
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM merge(currentDatabase(), '^t04741_file_(present|sibling)$'))
    WHERE explain ILIKE '%ReadFromFile%';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_file_present$') GROUP BY 1 ORDER BY 1;

-- `_path` here is a physical column, so `getVirtualsForFileLikeStorage` omits the `_path` virtual and
-- the child no longer declares the whole family. It still stamps its own `_table`, so it stays
-- prunable: master answers this query, and reading the absent file would raise FILE_DOESNT_EXIST.
SELECT '-- arm J: a file-like child whose family virtual is shadowed physically is still pruned';
CREATE TABLE t04741_file_shadow (x UInt32, _path String) ENGINE = File(TSV, '04741_no_such_file_2.tsv');
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_file_(shadow|sibling)$')
    WHERE _table = 't04741_file_sibling' GROUP BY 1 ORDER BY 1;

SELECT '-- arm J control: with no filter the same query opens the shadowed child and fails';
SELECT count() FROM merge(currentDatabase(), '^t04741_file_(shadow|sibling)$'); -- { serverError FILE_DOESNT_EXIST }

-- A wrapper over a file-like target emits the TARGET's name, not the wrapper's, so it must be pruned
-- by name exactly like the target would be. Master prunes it, and the destination file is absent, so
-- admitting it raises where master answered.
SELECT '-- arm K: a Buffer over a file-like target is pruned by name';
CREATE TABLE t04741_file_buf_target (x UInt32) ENGINE = File(TSV, '04741_no_such_file_3.tsv');
CREATE TABLE t04741_file_buf (x UInt32)
    ENGINE = Buffer(currentDatabase(), t04741_file_buf_target, 1, 3600, 3600, 10, 100, 10000, 100000);
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_file_(buf|sibling)$')
    WHERE _table = 't04741_file_sibling' GROUP BY 1 ORDER BY 1;

SELECT '-- arm K control: with no filter the same query opens the absent destination and fails';
SELECT count() FROM merge(currentDatabase(), '^t04741_file_(buf|sibling)$'); -- { serverError FILE_DOESNT_EXIST }

-- Arm L is arm K under the other predicate. The target declares no `_database`, so the wrapper's rows
-- carry an empty one, which the wrapper's own database name never matches: a `_database` predicate must
-- not prune it. Arm G is the same pair for a direct file-like child. Filtering for the empty value is
-- what discriminates; `_database = currentDatabase()` matches the wrapper's name and is kept either way.
SELECT '-- arm L: a _database filter must not prune a wrapper over a file-like target';
SELECT count() FROM merge(currentDatabase(), '^t04741_file_(buf|sibling)$')
    WHERE _database = ''; -- { serverError FILE_DOESNT_EXIST }

-- Arms M and N are the other direction of the same rule: pruning is sound whenever the predicate
-- rejects the identity the rows carry, whichever names it reads. Both filter the sibling by name and
-- also require a `_database` the file-like rows can never have, so the child must still be pruned and
-- its absent file never opened. Arm N is the witness: a rule keyed on which names the predicate reads
-- admits the child there and raises. Arm M reads 23 on master, on that rule and on this fix, so it is
-- a control asserting the wrapper keeps being pruned rather than a witness for the change.
SELECT '-- arm M control: a wrapper stays pruned when the whole predicate rejects its target identity';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_file_(buf|sibling)$')
    WHERE _table = 't04741_file_sibling' AND _database = currentDatabase() GROUP BY 1 ORDER BY 1;

SELECT '-- arm N: same for a direct file-like child';
SELECT _table, count() FROM merge(currentDatabase(), '^t04741_file_(child|sibling)$')
    WHERE _table = 't04741_file_sibling' AND _database = currentDatabase() GROUP BY 1 ORDER BY 1;

-- The wrapper's rows carry the TARGET's `_table`, so a predicate naming the target must admit it.
SELECT '-- arm O: a _table filter naming the forwarding target admits the wrapper';
SELECT count() FROM merge(currentDatabase(), '^t04741_file_(buf|sibling)$')
    WHERE _table = 't04741_file_buf_target'; -- { serverError FILE_DOESNT_EXIST }

-- A materialized view is not a transparent wrapper: `StorageMaterializedView::getInMemoryMetadataPtr`
-- moves the target's `_database`/`_table` to `Plan`, and `StorageWithCommonVirtualColumns::read`
-- materializes both from the VIEW's own `StorageID`. So a view over a file-like target emits the
-- view's identity, not the target's, and must be pruned against that: following the target instead
-- admits the view for `_table = '<target>'` / `_database = ''` and opens its absent file, where
-- master answered. Arms P and Q are the witnesses, each raising `FILE_DOESNT_EXIST` without the rule.
CREATE TABLE t04741_file_mv_src (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t04741_file_mv_target (x UInt32) ENGINE = File(TSV, '04741_no_such_file_4.tsv');
CREATE MATERIALIZED VIEW t04741_file_mv TO t04741_file_mv_target AS SELECT x FROM t04741_file_mv_src;

SELECT '-- arm P: a materialized view child is pruned against its own name, not its file-like target';
SELECT count() FROM merge(currentDatabase(), '^t04741_file_(mv|sibling)$')
    WHERE _table = 't04741_file_mv_target';

-- The view declares no `_database` (its file-like target does not), so its rows carry an empty one and
-- a `_database = ''` predicate must not prune it, exactly as for the direct file-like child of arm G.
SELECT '-- arm Q control: a _database filter must not prune the view, whose rows carry an empty one';
SELECT count() FROM merge(currentDatabase(), '^t04741_file_(mv|sibling)$')
    WHERE _database = ''; -- { serverError FILE_DOESNT_EXIST }

SELECT '-- arm R control: naming the view itself admits it, and its absent target does raise';
SELECT count() FROM merge(currentDatabase(), '^t04741_file_(mv|sibling)$')
    WHERE _table = 't04741_file_mv'; -- { serverError FILE_DOESNT_EXIST }

DROP TABLE t04741_file_mv;
DROP TABLE t04741_file_mv_target;
DROP TABLE t04741_file_mv_src;

DROP TABLE t04741_file_buf;
DROP TABLE t04741_file_buf_target;
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
