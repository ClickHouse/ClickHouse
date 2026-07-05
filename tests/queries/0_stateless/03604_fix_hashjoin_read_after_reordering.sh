#!/bin/bash
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Define the set of N values for numbers(N)
numbers_values=(5 10 20 50 60)

# Define values for join_output_by_rowlist_perkey_rows_threshold
threshold_value=50

# Define meaningful variables for allow_experimental_join_right_table_sorting
ALLOW_SORTING_DISABLED=0
ALLOW_SORTING_ENABLED=1

# Define value for join_to_sort_minimum_perkey_rows
min_rows_value=10

client_opts=(
  --enable_analyzer=1
)

# ------------------------------
# Test behavior when allow_experimental_join_right_table_sorting is true:
# (1) number of rows does not exceed min_rows_value
# (2) number of rows exceeds min_rows_value but does not exceed threshold_value
# (3) number of rows exceeds both min_rows_value and threshold_value
# ------------------------------
for n in "${numbers_values[@]}"; do
    ${CLICKHOUSE_CLIENT} "${client_opts[@]}" --query "
    SELECT *
    FROM (SELECT 1 x) x
    LEFT JOIN (SELECT 2 y) y ON x.x = y.y
    JOIN (SELECT number FROM numbers(${n})) z ON 1
    ORDER BY ALL
    SETTINGS
        join_algorithm = 'hash',
        join_output_by_rowlist_perkey_rows_threshold = ${threshold_value},
        allow_experimental_join_right_table_sorting = ${ALLOW_SORTING_ENABLED},
        join_to_sort_minimum_perkey_rows = ${min_rows_value};
    "
done

# ------------------------------
# Test behavior when allow_experimental_join_right_table_sorting is false:
# (1) number of rows does not exceed min_rows_value
# (2) number of rows exceeds min_rows_value but does not exceed threshold_value
# (3) number of rows exceeds both min_rows_value and threshold_value
# ------------------------------
for n in "${numbers_values[@]}"; do
    ${CLICKHOUSE_CLIENT} "${client_opts[@]}" --query "
    SELECT *
    FROM (SELECT 1 x) x
    LEFT JOIN (SELECT 2 y) y ON x.x = y.y
    JOIN (SELECT number FROM numbers(${n})) z ON 1
    ORDER BY ALL
    SETTINGS
        join_algorithm = 'hash',
        join_output_by_rowlist_perkey_rows_threshold = ${threshold_value},
        allow_experimental_join_right_table_sorting = ${ALLOW_SORTING_DISABLED},
        join_to_sort_minimum_perkey_rows = ${min_rows_value};
    "
done

# ------------------------------
# Join tests WITHOUT the three custom parameters
# ------------------------------
for n in "${numbers_values[@]}"; do
    ${CLICKHOUSE_CLIENT} "${client_opts[@]}" --query "
    SELECT *
    FROM (SELECT 1 x) x
    LEFT JOIN (SELECT 2 y) y ON x.x = y.y
    JOIN (SELECT number FROM numbers(${n})) z ON 1
    ORDER BY ALL;
    "
done

# ------------------------------
# Simple Join
# ------------------------------
${CLICKHOUSE_CLIENT} "${client_opts[@]}" --query "
SELECT c.name, p.category, o.amount
FROM
    (SELECT 1 AS id, 'Alice' AS name) AS c
JOIN
    (SELECT 1 AS id, 'electronics' AS category) AS p
    ON c.id = p.id
JOIN
    (SELECT 1 AS customer_id, 1 AS product_id, 120 AS amount) AS o
    ON c.id = o.customer_id AND p.id = o.product_id
ORDER BY ALL
"

# ------------------------------
# Aggregation + Join
# ------------------------------
${CLICKHOUSE_CLIENT} "${client_opts[@]}" --query "
SELECT c.name, o.total_amount
FROM
    (SELECT 1 AS id, 'Alice' AS name) AS c
LEFT JOIN
    (SELECT customer_id, SUM(amount) AS total_amount
     FROM (SELECT 1 AS customer_id, 100 AS amount
           UNION ALL
           SELECT 1, 200
           UNION ALL
           SELECT 2, 50) AS orders
     GROUP BY customer_id) AS o
    ON c.id = o.customer_id
ORDER BY ALL;
"

# ------------------------------
# Self-join
# ------------------------------
${CLICKHOUSE_CLIENT} "${client_opts[@]}" --query "
SELECT e1.id AS emp1, e2.id AS emp2, e1.department
FROM
    (SELECT 1 AS id, 'HR' AS department
     UNION ALL
     SELECT 2, 'HR'
     UNION ALL
     SELECT 3, 'IT') AS e1
JOIN
    (SELECT 1 AS id, 'HR' AS department
     UNION ALL
     SELECT 2, 'HR'
     UNION ALL
     SELECT 3, 'IT') AS e2
    ON e1.department = e2.department AND e1.id < e2.id
ORDER BY ALL;
"

# ------------------------------
# Multi-condition Join with expressions
# ------------------------------
${CLICKHOUSE_CLIENT} "${client_opts[@]}" --query "
SELECT a.id, b.value AS b_val, c.value AS c_val
FROM
    (SELECT 1 AS id, 15 AS value
     UNION ALL
     SELECT 2, 30) AS a
JOIN
    (SELECT 1 AS id, 40 AS value
     UNION ALL
     SELECT 2, 60) AS b
    ON a.id = b.id AND a.value + b.value > 30
JOIN
    (SELECT 1 AS id, 20 AS value
     UNION ALL
     SELECT 2, 50) AS c
    ON a.id = c.id AND c.value - a.value < 40
ORDER BY ALL;
"

# ------------------------------
# Multi-layer join
# ------------------------------
${CLICKHOUSE_CLIENT} "${client_opts[@]}" --query "
SELECT t1.id, t2.val2, t3.val3
FROM
    (SELECT 1 AS id, 10 AS val
     UNION ALL
     SELECT 2, 20) AS t1
JOIN
    (SELECT id, val * 2 AS val2
     FROM (SELECT 1 AS id, 10 AS val UNION ALL SELECT 2, 20)) AS t2
    ON t1.id = t2.id
JOIN
    (SELECT id, val2 + 5 AS val3
     FROM (SELECT id, val * 2 AS val2
           FROM (SELECT 1 AS id, 10 AS val UNION ALL SELECT 2, 20))) AS t3
    ON t1.id = t3.id
ORDER BY ALL;
"
