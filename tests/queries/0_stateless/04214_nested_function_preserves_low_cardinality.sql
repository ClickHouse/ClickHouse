-- Regression test for #95582: `nested` and ARRAY JOIN over Nested columns must
-- preserve LowCardinality. Previously LowCardinality was stripped because the
-- default LowCardinality unwrap in IFunction couldn't re-wrap Array(Tuple(...)).

SELECT toTypeName(nested(['name'], CAST(['a', 'b'] AS Array(LowCardinality(String)))));

SELECT toTypeName(nested(['k', 'v'],
                        CAST(['a', 'b'] AS Array(LowCardinality(String))),
                        CAST([1, 2]     AS Array(UInt64))));

DROP TABLE IF EXISTS t_95582;
CREATE TABLE t_95582
(
    ts DateTime,
    `m.stack` Array(Array(LowCardinality(String))),
    `m.value` Array(UInt64)
) ENGINE = MergeTree ORDER BY ts;

INSERT INTO t_95582 VALUES (now(), [['x','y'], ['z']], [10, 20]);

SELECT type
FROM (DESCRIBE (SELECT m.stack FROM t_95582 ARRAY JOIN m))
WHERE name = 'm.stack';

DROP TABLE t_95582;
