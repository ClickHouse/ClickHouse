-- Tags: use-rocksdb, no-fasttest
-- The convert-any-join-to-semi-or-anti pass runs on ordinary joins too, not only decorrelation joins.
-- Its capability check must recognize that DirectKeyValueJoin executes LEFT SEMI / ANTI, otherwise an
-- explicit LEFT ANY JOIN over a key-value storage with join_algorithm = 'direct' stops being rewritten
-- to SEMI / ANTI (a plan regression on ordinary direct joins). Results are identical whether or not the
-- rewrite fires, so assert the plan: the join must be SEMI with convert on and stay ANY with it off.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_04547_left;
DROP TABLE IF EXISTS kv_04547;
CREATE TABLE t_04547_left (id UInt64) ENGINE = Memory;
INSERT INTO t_04547_left SELECT number FROM numbers(5);
CREATE TABLE kv_04547 (id UInt64, val String) ENGINE = EmbeddedRocksDB PRIMARY KEY id;
INSERT INTO kv_04547 SELECT number, toString(number) FROM numbers(10);

-- Result is the matched ids regardless of the rewrite; convert on and off must agree.
SELECT id FROM t_04547_left LEFT ANY JOIN kv_04547 ON t_04547_left.id = kv_04547.id WHERE kv_04547.val != '' ORDER BY id SETTINGS join_algorithm = 'direct', query_plan_convert_any_join_to_semi_or_anti_join = 1;
SELECT id FROM t_04547_left LEFT ANY JOIN kv_04547 ON t_04547_left.id = kv_04547.id WHERE kv_04547.val != '' ORDER BY id SETTINGS join_algorithm = 'direct', query_plan_convert_any_join_to_semi_or_anti_join = 0;

-- With the rewrite on, the direct join must become SEMI; with it off it stays ANY.
SELECT count() > 0 FROM (
    EXPLAIN SELECT id FROM t_04547_left LEFT ANY JOIN kv_04547 ON t_04547_left.id = kv_04547.id WHERE kv_04547.val != ''
    SETTINGS join_algorithm = 'direct', query_plan_convert_any_join_to_semi_or_anti_join = 1
) WHERE explain ILIKE '%Strictness: semi%';
SELECT count() > 0 FROM (
    EXPLAIN SELECT id FROM t_04547_left LEFT ANY JOIN kv_04547 ON t_04547_left.id = kv_04547.id WHERE kv_04547.val != ''
    SETTINGS join_algorithm = 'direct', query_plan_convert_any_join_to_semi_or_anti_join = 0
) WHERE explain ILIKE '%Strictness: any%';

DROP TABLE t_04547_left;
DROP TABLE kv_04547;
