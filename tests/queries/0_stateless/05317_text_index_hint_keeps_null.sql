-- Tags: no-fasttest, no-parallel-replicas
-- Random settings limits: query_plan_direct_read_from_text_index=(1, None); query_plan_text_index_add_hint=(1, None)
-- Direct read from a text index keeps NULL predicate values: a predicate over a JSON path that is absent in a row is NULL,
-- so NOT of it and other NULL-sensitive expressions must give the same result as without the index.

DROP TABLE IF EXISTS t_hint_null;
CREATE TABLE t_hint_null
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_hint_null VALUES (1, '{"a": {"b": 1}, "c": "hello"}'), (2, '{"a": {"d": 2}, "e": "world"}');
INSERT INTO t_hint_null VALUES (3, '{"x": {"y": 3}, "z": "test"}'), (4, '{"p": {"q": 4}, "r": "foo"}');
INSERT INTO t_hint_null VALUES (5, '{"a": {"b": 2}}');

SELECT 'NOT of a conjunction with an absent path';
SELECT id FROM t_hint_null WHERE NOT ((json.a.b = 1) AND (json.nonexistent = 257)) ORDER BY id;

SELECT 'TLP partitions add up to the table';
SELECT count() FROM
(
    SELECT id FROM t_hint_null WHERE (json.a.b = 1) AND (json.nonexistent = 257)
    UNION ALL
    SELECT id FROM t_hint_null WHERE NOT ((json.a.b = 1) AND (json.nonexistent = 257))
    UNION ALL
    SELECT id FROM t_hint_null WHERE isNull((json.a.b = 1) AND (json.nonexistent = 257))
);

SELECT 'NOT of a single path predicate';
SELECT id FROM t_hint_null WHERE NOT (json.a.b = 1 AND id > 0) ORDER BY id;

SELECT 'NOT of a typed path predicate';
SELECT id FROM t_hint_null WHERE NOT (json.a.b.:Int64 = 1 AND id > 0) ORDER BY id;

SELECT 'predicate value in the result';
SELECT id, (json.a.b = 1) AS x FROM t_hint_null WHERE x OR id > 0 ORDER BY id;

SELECT 'positive predicate';
SELECT id FROM t_hint_null WHERE json.a.b = 1 ORDER BY id;

SELECT 'NOT of a non-NULL predicate on an absent path';
SELECT id FROM t_hint_null WHERE NOT (isNotNull(json.a.b) AND id > 0) ORDER BY id;

SELECT 'text index is used';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_hint_null WHERE json.a.b = 1)
WHERE explain ILIKE '%__text_index_idx_JSONPathExists%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_hint_null WHERE NOT (json.a.b = 1 AND id > 0))
WHERE explain ILIKE '%__text_index_idx_JSONPathExists%';

DROP TABLE t_hint_null;
