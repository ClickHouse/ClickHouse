DROP TABLE IF EXISTS 04259_filter_constant_column_after_where;

CREATE TABLE 04259_filter_constant_column_after_where
(
    x String,
    y UInt64,
    z UInt64,
    n Nullable(UInt64),
    lc LowCardinality(String)
)
ENGINE = Memory;

INSERT INTO 04259_filter_constant_column_after_where VALUES
    ('hello', 1, 2, 1, 'hello'),
    ('world', 2, 2, NULL, 'world'),
    ('hello', 3, 2, 3, 'hello'),
    ('other', 4, 4, 4, 'other');

SELECT dumpColumnStructure(x), count(), sum(y)
FROM 04259_filter_constant_column_after_where
WHERE x = 'hello'
GROUP BY ALL
ORDER BY ALL
SETTINGS query_plan_merge_filters = 0;

SELECT dumpColumnStructure(x), count(), sum(y)
FROM 04259_filter_constant_column_after_where
WHERE 'world' = x AND y >= 0
GROUP BY ALL
ORDER BY ALL
SETTINGS query_plan_merge_filters = 1;

SELECT dumpColumnStructure(y), count()
FROM 04259_filter_constant_column_after_where
WHERE y = 3
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(x), dumpColumnStructure(y), dumpColumnStructure(z), count()
FROM 04259_filter_constant_column_after_where
WHERE x = 'hello' AND y = 1 AND z = 2
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(x), dumpColumnStructure(y), dumpColumnStructure(z), count()
FROM 04259_filter_constant_column_after_where
WHERE x = 'hello' AND y = 1 AND x = 'hello'
GROUP BY ALL
ORDER BY ALL;

WITH x AS x_alias
SELECT dumpColumnStructure(x_alias), count()
FROM 04259_filter_constant_column_after_where
WHERE x_alias = 'world'
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(x), count()
FROM
(
    SELECT 'always_true' AS x, number
    FROM numbers(3)
)
WHERE x = 'always_true'
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(y), count()
FROM 04259_filter_constant_column_after_where
WHERE y = 1 OR y = 3
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(y), count()
FROM 04259_filter_constant_column_after_where
WHERE NOT (y = 1)
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(y), count()
FROM 04259_filter_constant_column_after_where
WHERE y = 1::UInt8
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(y), count()
FROM 04259_filter_constant_column_after_where
WHERE toString(y) = '1'
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(n), count()
FROM 04259_filter_constant_column_after_where
WHERE n = 1
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(lc), count()
FROM 04259_filter_constant_column_after_where
WHERE lc = 'hello'
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(z), count()
FROM 04259_filter_constant_column_after_where
WHERE x = 'hello'
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(x), count()
FROM 04259_filter_constant_column_after_where
WHERE x = 'hello' AND y = 1
GROUP BY ALL
ORDER BY ALL;

SELECT count()
FROM 04259_filter_constant_column_after_where
WHERE x = 'missing';

SELECT reinterpretAsUInt64(f)
FROM
(
    SELECT arrayJoin([reinterpretAsFloat64(toUInt64(0)), reinterpretAsFloat64(toUInt64(9223372036854775808))]) AS f
)
WHERE f = 0.0
ORDER BY reinterpretAsUInt64(f);

DROP TABLE 04259_filter_constant_column_after_where;
