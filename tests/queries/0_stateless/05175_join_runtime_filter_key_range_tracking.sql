-- Build-side key range tracking costs an extra pass over every build chunk, so it must be enabled
-- only for the runtime filters that the probe side can really use for granule pruning.

SET explain_query_plan_default = 'legacy'; -- the `Key range tracking` line is printed by the non-pretty EXPLAIN
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET join_algorithm = 'hash';
SET max_bytes_ratio_before_external_join = 0;

DROP TABLE IF EXISTS probe_pk;
DROP TABLE IF EXISTS probe_skip_index;
DROP TABLE IF EXISTS probe_no_index;
DROP TABLE IF EXISTS build_side;
DROP TABLE IF EXISTS build_two_keys;

CREATE TABLE probe_pk (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE probe_skip_index (k UInt64, v UInt64, INDEX idx_v v TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k;
CREATE TABLE probe_no_index (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE build_side (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE build_two_keys (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO probe_pk SELECT number, number FROM numbers(1000);
INSERT INTO probe_skip_index SELECT number, number FROM numbers(1000);
INSERT INTO probe_no_index SELECT number, number FROM numbers(1000);
INSERT INTO build_side SELECT number FROM numbers(10);
INSERT INTO build_two_keys SELECT number, number FROM numbers(10);

-- The join key is the primary key of the probe side: the range is used, so it is tracked.
SELECT 'primary key';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM probe_pk AS p INNER JOIN build_side AS b ON p.k = b.k
) WHERE explain LIKE '%Key range tracking%';

-- The join key is covered by a `minmax` skip index: the range is used, so it is tracked.
SELECT 'skip index';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM probe_skip_index AS p INNER JOIN build_side AS b ON p.v = b.k
) WHERE explain LIKE '%Key range tracking%';

-- The join key is neither in the primary key nor covered by a skip index: nothing on the probe side
-- can consume the range, so the build side must not pay for tracking it.
SELECT 'no index';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM probe_no_index AS p INNER JOIN build_side AS b ON p.v = b.k
) WHERE explain LIKE '%Key range tracking%';

-- The probe side is not a MergeTree read at all, so there is no index analysis to feed.
SELECT 'not a MergeTree probe side';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM (SELECT number AS k FROM numbers(1000)) AS p INNER JOIN build_side AS b ON p.k = b.k
) WHERE explain LIKE '%Key range tracking%';

-- With the setting off, tracking is off everywhere.
SELECT 'setting off';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM probe_pk AS p INNER JOIN build_side AS b ON p.k = b.k
    SETTINGS enable_join_runtime_filters_index_analysis = 0
) WHERE explain LIKE '%Key range tracking%';

-- Both filters of one query are built with the same setting value, and only the one whose key the
-- probe side can prune is tracked: the decision is per filter, not per query.
SELECT 'two filters in one query';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM probe_no_index AS p
    INNER JOIN build_side AS b1 ON p.k = b1.k
    INNER JOIN build_side AS b2 ON p.v = b2.k
) WHERE explain LIKE '%Key range tracking%' OR explain LIKE '%Build runtime join filter%';

-- A `LEFT ANTI` join builds a negating filter (`ExactNotContains`), which exposes neither recorded
-- key values nor a key range, so no pruning predicate can ever be derived from it - even when the
-- join key is the primary key of the probe side. Tracking must be off there too.
SELECT 'single-key anti join on the primary key';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM probe_pk AS p LEFT ANTI JOIN build_side AS b ON p.k = b.k
) WHERE explain LIKE '%Key range tracking%';

-- The multi-key `LEFT ANTI` case builds one filter on a tuple of the keys, also negating.
SELECT 'multi-key anti join on the primary key';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM probe_pk AS p LEFT ANTI JOIN build_two_keys AS b ON p.k = b.k AND p.v = b.v
) WHERE explain LIKE '%Key range tracking%';

-- One prunable inner-join filter and one anti-join filter in the same query: only the first is tracked.
SELECT 'anti join next to an inner join';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM probe_pk AS p
    INNER JOIN build_side AS b1 ON p.k = b1.k
    LEFT ANTI JOIN build_side AS b2 ON p.k = b2.k
) WHERE explain LIKE '%Key range tracking%' OR explain LIKE '%Build runtime join filter%';

-- The results must not depend on the pruning.
SELECT 'results';
SELECT count() FROM probe_pk AS p INNER JOIN build_side AS b ON p.k = b.k;
SELECT count() FROM probe_no_index AS p INNER JOIN build_side AS b ON p.v = b.k;
SELECT count() FROM probe_pk AS p LEFT ANTI JOIN build_side AS b ON p.k = b.k;
SELECT count() FROM probe_pk AS p LEFT ANTI JOIN build_two_keys AS b ON p.k = b.k AND p.v = b.v;

DROP TABLE probe_pk;
DROP TABLE probe_skip_index;
DROP TABLE probe_no_index;
DROP TABLE build_side;
DROP TABLE build_two_keys;
