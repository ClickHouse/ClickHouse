-- Read-in-order analysis over a column fixed by `WHERE`: with a normal `PROJECTION` on a fixed key,
-- and through an `ALIAS` on the filter column.

SET explain_query_plan_default = 'legacy';
SET optimize_read_in_order = 1;
SET optimize_use_projections = 1;
SET optimize_use_projection_filtering = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET read_in_order_use_virtual_row = 1;
SET enable_parallel_replicas = 0;
SET parallel_replicas_for_non_replicated_merge_tree = 0;
SET use_top_k_dynamic_filtering = 0;
SET use_skip_indexes_for_top_k = 0;
SET optimize_distinct_in_order = 1;

DROP TABLE IF EXISTS mt_fixed_key SYNC;

CREATE TABLE mt_fixed_key
(
    a UInt64,
    b UInt64,
    c UInt64,
    PROJECTION p (SELECT * ORDER BY b, c)
)
ENGINE = MergeTree
ORDER BY (c, b)
SETTINGS index_granularity = 4;

INSERT INTO mt_fixed_key SELECT number, number % 3, number FROM numbers(64);

SELECT 'reverse order read, projection step, base table step, base table step without projections';
SELECT count() FROM (EXPLAIN actions = 1 SELECT a, c FROM mt_fixed_key WHERE b = 1 ORDER BY c DESC LIMIT 5) WHERE explain LIKE '%ReadType: InReverseOrder%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT a, c FROM mt_fixed_key WHERE b = 1 ORDER BY c DESC LIMIT 5) WHERE explain LIKE '%ReadFromMergeTree (p)%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT a, c FROM mt_fixed_key WHERE b = 1 ORDER BY c DESC LIMIT 5) WHERE explain LIKE '%ReadFromMergeTree (%.mt_fixed_key)%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT a, c FROM mt_fixed_key WHERE b = 1 ORDER BY c DESC LIMIT 5 SETTINGS optimize_use_projections = 0) WHERE explain LIKE '%ReadFromMergeTree (%.mt_fixed_key)%';

SELECT 'same rows and same order as reading the table';
SELECT groupArray(c) = (SELECT groupArray(c) FROM (SELECT c FROM mt_fixed_key WHERE b = 1 ORDER BY c DESC LIMIT 5 SETTINGS optimize_use_projections = 0)) FROM (SELECT c FROM mt_fixed_key WHERE b = 1 ORDER BY c DESC LIMIT 5);

SELECT 'ordering by a column outside the projection key';
SELECT count() FROM (EXPLAIN actions = 1 SELECT a, c FROM mt_fixed_key WHERE b = 1 ORDER BY a DESC LIMIT 5) WHERE explain LIKE '%ReadType: In%';

SELECT 'optimization disabled';
SELECT count() FROM (EXPLAIN actions = 1 SELECT a, c FROM mt_fixed_key WHERE b = 1 ORDER BY c DESC LIMIT 5 SETTINGS optimize_read_in_order = 0) WHERE explain LIKE '%ReadType: In%';

SELECT 'a fixed column in ORDER BY does not dictate the read direction';
SELECT count() FROM (EXPLAIN actions = 1 SELECT a, c FROM mt_fixed_key WHERE b = 1 ORDER BY b, c DESC LIMIT 5) WHERE explain LIKE '%ReadType: InReverseOrder%';

SELECT 'DISTINCT in order';
SELECT count() FROM (EXPLAIN actions = 1 SELECT DISTINCT c FROM mt_fixed_key WHERE b = 1) WHERE explain LIKE '%ReadType: InOrder%';

DROP TABLE mt_fixed_key SYNC;

DROP TABLE IF EXISTS mt_proj_only SYNC;

-- The table key cannot serve the `ORDER BY`, so only the projection can, and only once its
-- leading key is seen as fixed by the `WHERE`.
CREATE TABLE mt_proj_only
(
    a UInt64,
    b UInt64,
    c UInt64,
    PROJECTION p (SELECT * ORDER BY b, c)
)
ENGINE = MergeTree
ORDER BY a
SETTINGS index_granularity = 4;

INSERT INTO mt_proj_only SELECT number, number % 3, number FROM numbers(64);

SELECT 'in order read of the projection, projection step';
SELECT count() FROM (EXPLAIN actions = 1 SELECT a, c FROM mt_proj_only WHERE b = 1 ORDER BY c LIMIT 5) WHERE explain LIKE '%ReadType: InOrder%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT a, c FROM mt_proj_only WHERE b = 1 ORDER BY c LIMIT 5) WHERE explain LIKE '%ReadFromMergeTree (p)%';

SELECT 'same rows and same order as reading the table, projection only';
SELECT groupArray(c) = (SELECT groupArray(c) FROM (SELECT c FROM mt_proj_only WHERE b = 1 ORDER BY c LIMIT 5 SETTINGS optimize_use_projections = 0)) FROM (SELECT c FROM mt_proj_only WHERE b = 1 ORDER BY c LIMIT 5);

DROP TABLE mt_proj_only SYNC;

DROP TABLE IF EXISTS mt_alias SYNC;

CREATE TABLE mt_alias
(
    a UInt64,
    c UInt64,
    f UInt8 ALIAS (a = 1)
)
ENGINE = MergeTree
ORDER BY (a, c)
SETTINGS index_granularity = 4;

INSERT INTO mt_alias SELECT number % 4, number FROM numbers(64);

SELECT 'an aliased filter column in a filter step, no projection involved';
SELECT count() FROM (EXPLAIN actions = 1 SELECT c FROM mt_alias WHERE f ORDER BY c LIMIT 5 SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0) WHERE explain LIKE '%ReadType: InOrder%';

SELECT 'same rows and same order as reading the table, aliased filter column';
SELECT groupArray(c) = (SELECT groupArray(c) FROM (SELECT c FROM mt_alias WHERE f ORDER BY c LIMIT 5 SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, optimize_read_in_order = 0)) FROM (SELECT c FROM mt_alias WHERE f ORDER BY c LIMIT 5 SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0);

DROP TABLE mt_alias SYNC;
