DROP TABLE IF EXISTS 04259_filter_constant_column_after_where;

CREATE TABLE 04259_filter_constant_column_after_where
(
    x String,
    y UInt64,
    z UInt64
)
ENGINE = Memory;

INSERT INTO 04259_filter_constant_column_after_where VALUES ('hello', 1, 2), ('world', 2, 2), ('hello', 3, 2), ('other', 4, 4);

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

DROP TABLE 04259_filter_constant_column_after_where;
