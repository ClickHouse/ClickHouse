-- Tags: no-fasttest, shard

SET allow_experimental_row_type = 1;

DROP TABLE IF EXISTS row_type_mismatch;
DROP TABLE IF EXISTS row_final_prewhere;
DROP TABLE IF EXISTS row_text;

SELECT '-- wrapper invariant: field types must mirror the source columns';

-- `combined` names the right columns in the right order, but `a` is
-- `Nullable(String)` while the field is `String` and `b` is `Date` while the
-- field is `UInt32`. That is not a wrapper, so the optimizer must leave the
-- read alone instead of re-exposing `a`/`b` with the wrapper's field types.
CREATE TABLE row_type_mismatch (
    id UInt64,
    a Nullable(String),
    b Date,
    c String,
    combined Row(a String, b UInt32, c String) MATERIALIZED tuple(a, b, c)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO row_type_mismatch (id, a, b, c) SELECT number, toString(number), toDate('2020-01-01') + number, concat('row-', toString(number)) FROM numbers(100);

SELECT count(), max(length(a)), min(b) FROM row_type_mismatch SETTINGS query_plan_use_row_wrappers = 1;
SELECT count(), max(length(a)), min(b) FROM row_type_mismatch SETTINGS query_plan_use_row_wrappers = 0;
SELECT toTypeName(a), toTypeName(b) FROM row_type_mismatch LIMIT 1 SETTINGS query_plan_use_row_wrappers = 1;
-- The rewrite must not fire at all for a non-mirroring `Row`.
SELECT countIf(explain LIKE '%__rowElement%') FROM (
    EXPLAIN actions = 1 SELECT a, b, c FROM row_type_mismatch SETTINGS query_plan_use_row_wrappers = 1
);

DROP TABLE row_type_mismatch;

SELECT '-- FINAL with a deferred PREWHERE keeps the filter column readable';

CREATE TABLE row_final_prewhere (
    id UInt64,
    a UInt64,
    b UInt64,
    c UInt64,
    combined Row(a UInt64, b UInt64, c UInt64) MATERIALIZED tuple(a, b, c)
) ENGINE = ReplacingMergeTree ORDER BY id;

INSERT INTO row_final_prewhere (id, a, b, c) SELECT number, number, number * 2, number * 3 FROM numbers(100);
INSERT INTO row_final_prewhere (id, a, b, c) SELECT number, number + 1, number * 2 + 1, number * 3 + 1 FROM numbers(50);

SELECT b, c FROM row_final_prewhere FINAL PREWHERE a = 7 ORDER BY b
    SETTINGS apply_prewhere_after_final = 1, query_plan_use_row_wrappers = 1;
SELECT b, c FROM row_final_prewhere FINAL PREWHERE a = 7 ORDER BY b
    SETTINGS apply_prewhere_after_final = 1, query_plan_use_row_wrappers = 0;
SELECT sum(b), sum(c) FROM row_final_prewhere FINAL PREWHERE a > 10
    SETTINGS apply_prewhere_after_final = 1, query_plan_use_row_wrappers = 1;
SELECT sum(b), sum(c) FROM row_final_prewhere FINAL PREWHERE a > 10
    SETTINGS apply_prewhere_after_final = 1, query_plan_use_row_wrappers = 0;

DROP TABLE row_final_prewhere;

SELECT '-- a plain Row column round-trips through text formats';

CREATE TABLE row_text (id UInt8, r Row(a UInt8, b String)) ENGINE = Memory;
INSERT INTO row_text VALUES (1, (1, 'x')), (2, (2, 'yy'));
SELECT r FROM row_text ORDER BY id FORMAT TSV;
SELECT r FROM row_text ORDER BY id FORMAT JSONEachRow;
SELECT toTypeName(r) FROM row_text LIMIT 1;

-- Text input works through the format machinery too, not only through VALUES.
SELECT r, toTypeName(r) FROM format(TSV, 'r Row(a UInt8, b String)', '(3,\'zzz\')');
SELECT r FROM format(JSONEachRow, 'r Row(a UInt8, b String)', '{"r":{"a":4,"b":"w"}}');

DROP TABLE row_text;

SELECT '-- Row has a binary type encoding, so parallel replicas can ship the header';

DROP TABLE IF EXISTS row_parallel;

CREATE TABLE row_parallel (
    id UInt64,
    a String,
    b UInt32,
    combined Row(a String, b UInt32) MATERIALIZED tuple(a, b)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO row_parallel (id, a, b) SELECT number, toString(number), toUInt32(number * 2) FROM numbers(100);

SELECT count(), sum(b) FROM row_parallel
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
             cluster_for_parallel_replicas = 'parallel_replicas',
             parallel_replicas_for_non_replicated_merge_tree = 1,
             parallel_replicas_local_plan = 1, query_plan_use_row_wrappers = 1;
SELECT count(), sum(b) FROM row_parallel
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
             cluster_for_parallel_replicas = 'parallel_replicas',
             parallel_replicas_for_non_replicated_merge_tree = 1,
             parallel_replicas_local_plan = 1, query_plan_use_row_wrappers = 0;

DROP TABLE row_parallel;
