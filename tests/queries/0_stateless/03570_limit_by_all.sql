CREATE TABLE test_limit_by_all_comprehensive (
    id Int32,
    category String,
    value Int32,
    name String,
    score Float64,
    is_active Bool
) ENGINE = Memory;

INSERT INTO test_limit_by_all_comprehensive VALUES 
(1, 'A', 100, 'item1', 10.5, true),
(1, 'A', 200, 'item2', 20.3, false),
(1, 'B', 300, 'item3', 15.7, true),
(2, 'A', 400, 'item4', 25.1, true),
(2, 'A', 500, 'item5', 30.2, false),
(2, 'B', 600, 'item6', 35.8, true),
(3, 'C', 700, 'item7', 40.0, true),
(3, 'C', 800, 'item8', 45.2, false),
(4, 'A', 900, 'item9', 50.1, true),
(4, 'B', 1000, 'item10', 55.3, true);

-- Should expand to LIMIT 1 BY id, category
SELECT id, category, value, name
FROM test_limit_by_all_comprehensive 
ORDER BY id, category, value, name
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category, value
SELECT id, category, value, name, value * 2 as doubled_value
FROM test_limit_by_all_comprehensive 
ORDER BY id, category, value, name, doubled_value
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category (excluding aggregate functions)
SELECT id, category, 
       count(*) as cnt,
       sum(value) as total_value,
       avg(score) as avg_score
FROM test_limit_by_all_comprehensive 
GROUP BY id, category
ORDER BY id, category, cnt, total_value, avg_score
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category (excluding window functions)
SELECT id, category, value,
       row_number() OVER (PARTITION BY category ORDER BY value) as rn
FROM test_limit_by_all_comprehensive 
ORDER BY id, category, value, rn
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category, value, name
SELECT id, category, value, name,
       CASE WHEN value > 500 THEN 'high' ELSE 'low' END as value_category,
       substring(name, 1, 3) as name_prefix
FROM test_limit_by_all_comprehensive 
ORDER BY id, category, value, name, value_category, name_prefix
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category, value, name
SELECT id, category, value, name,
       upper(name) as upper_name,
       value + score as combined_value
FROM test_limit_by_all_comprehensive 
ORDER BY id, category, value, name, upper_name, combined_value
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category, value
SELECT id, category, value,
       if(value > 500, 'high', 'low') as value_level
FROM test_limit_by_all_comprehensive 
ORDER BY id, category, value, value_level
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category, value, name, score, is_active
SELECT id, category, value, name, score, is_active
FROM test_limit_by_all_comprehensive 
ORDER BY id, category, value, name, score, is_active
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category, value
SELECT id, category, value, name
FROM test_limit_by_all_comprehensive 
ORDER BY value DESC, id, category, name
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category, value, name
SELECT id, category, value, name
FROM test_limit_by_all_comprehensive 
WHERE value > 500
ORDER BY id, category, value, name
LIMIT 1 BY ALL;

-- Should result in empty LIMIT BY clause (no non-aggregate columns)
SELECT count(*) as total_count,
       sum(value) as total_value,
       avg(score) as average_score
FROM test_limit_by_all_comprehensive 
ORDER BY total_count, total_value, average_score
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category
SELECT id, category,
       count(*) as cnt,
       sum(value) as total_value
FROM test_limit_by_all_comprehensive 
GROUP BY id, category
HAVING count(*) > 1
ORDER BY id, category, cnt, total_value
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category, value
SELECT id, category, value,
       (SELECT count(*) FROM test_limit_by_all_comprehensive t2 WHERE t2.category = test_limit_by_all_comprehensive.category) as category_total
FROM test_limit_by_all_comprehensive 
ORDER BY id, category, value, category_total
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category, value, name
SELECT t1.id, t1.category, t1.value, t1.name,
       t2.value as other_value
FROM test_limit_by_all_comprehensive t1
JOIN test_limit_by_all_comprehensive t2 ON t1.id = t2.id
ORDER BY t1.id, t1.category, t1.value, t1.name, t2.value
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category, value, name
SELECT id, category, value, name
FROM test_limit_by_all_comprehensive 
WHERE value < 500
UNION ALL
SELECT id, category, value, name
FROM test_limit_by_all_comprehensive 
WHERE value >= 500
ORDER BY id, category, value, name
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category, value, name
WITH cte AS (
    SELECT id, category, value, name
    FROM test_limit_by_all_comprehensive 
    WHERE value > 400
)
SELECT id, category, value, name
FROM cte
ORDER BY id, category, value, name
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category
SELECT id, category,
       count(*) as cnt,
       groupArray(name) as names_array
FROM test_limit_by_all_comprehensive 
GROUP BY id, category
ORDER BY id, category, cnt, names_array
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category, value, name
SELECT id, category, value, name,
       length(name) as name_length,
       concat(category, '_', toString(value)) as combined
FROM test_limit_by_all_comprehensive 
ORDER BY id, category, value, name, name_length, combined
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category, value, score
SELECT id, category, value, score,
       round(score, 1) as rounded_score,
       value * score as product
FROM test_limit_by_all_comprehensive 
ORDER BY id, category, value, score, rounded_score, product
LIMIT 1 BY ALL;

-- Should expand to LIMIT 1 BY id, category, value, name
SELECT id, category, value, name,
       toString(value) as value_str,
       toFloat64(value) as value_float
FROM test_limit_by_all_comprehensive 
ORDER BY id, category, value, name, value_str, value_float
LIMIT 1 BY ALL;

-- Clean up
DROP TABLE test_limit_by_all_comprehensive; 