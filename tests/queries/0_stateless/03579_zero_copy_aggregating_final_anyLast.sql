
CREATE TABLE t_coalesce (
    k UInt64,
    v1 SimpleAggregateFunction(anyLast, Nullable(String)),
    v2 SimpleAggregateFunction(anyLast, Nullable(FixedString(20))),
    v3 SimpleAggregateFunction(anyLast, Array(String)),
    v4 SimpleAggregateFunction(anyLast, Map(String, String)),
    v5 SimpleAggregateFunction(anyLast, JSON),
    v6 SimpleAggregateFunction(anyLast, Variant(String, UInt64)),
    v7 SimpleAggregateFunction(anyLast, Dynamic)
) ENGINE = AggregatingMergeTree() ORDER BY k;

INSERT INTO t_coalesce (k) VALUES (1);
INSERT INTO t_coalesce (k, v1) VALUES (1, 'Hello');
INSERT INTO t_coalesce (k, v2) VALUES (1, 'Annnnnnnnnnnnnnnnnnn');
INSERT INTO t_coalesce (k, v3, v4, v5, v6, v7) VALUES (1, ['a', 'b'], {'a': 'b'}, '{"a": "b"}', '{"a": 1}', '{"a": "b"}');

SELECT k, v1, v2, v3, v4, v5, v6, v7 FROM t_coalesce FINAL;

CREATE TABLE t_coalesce2 (
    k UInt64,
    v1 SimpleAggregateFunction(any, Nullable(String)),
    v2 SimpleAggregateFunction(any, Nullable(FixedString(20))),
    v3 SimpleAggregateFunction(any, Array(String)),
    v4 SimpleAggregateFunction(any, Map(String, String)),
    v5 SimpleAggregateFunction(any, JSON),
    v6 SimpleAggregateFunction(any, Variant(String, UInt64)),
    v7 SimpleAggregateFunction(any, Dynamic)
) ENGINE = AggregatingMergeTree() ORDER BY k;

INSERT INTO t_coalesce2 (k, v3, v4, v5, v6, v7) VALUES (1, ['a', 'b'], {'a': 'b'}, '{"a": "b"}', '{"a": 1}', '{"a": "b"}');
INSERT INTO t_coalesce2 (k, v1) VALUES (1, 'Hello');
INSERT INTO t_coalesce2 (k, v2) VALUES (1, 'Annnnnnnnnnnnnnnnnnn');

SELECT k, v1, v2, v3, v4, v5, v6, v7 FROM t_coalesce2 FINAL;

DROP TABLE t_coalesce;
DROP TABLE t_coalesce2;
