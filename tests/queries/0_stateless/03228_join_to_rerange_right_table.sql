drop table if exists test_left;
drop table if exists test_right;

CREATE TABLE test_left (a Int64, b String, c LowCardinality(String)) ENGINE = MergeTree() ORDER BY a;
CREATE TABLE test_right (a Int64, b String, c LowCardinality(String)) ENGINE = MergeTree() ORDER BY a;

INSERT INTO test_left SELECT number % 10000, number % 10000, number % 10000 FROM numbers(100000);
INSERT INTO test_right SELECT number % 10 , number % 10, number % 10 FROM numbers(10000);

SET allow_experimental_join_right_table_sorting = true;

SELECT MAX(test_right.a), count() FROM test_left INNER JOIN test_right on test_left.b = test_right.b;
SELECT MAX(test_right.a), count() FROM test_left LEFT JOIN test_right on test_left.b = test_right.b;

drop table test_left;
drop table test_right;

-- Right table sorting with payload columns that are not fixed width. Probe keys 3 and 4 have no
-- match, so the join also has to produce the default value of every column kind.
drop table if exists test_payload;
drop table if exists test_probe;

CREATE TABLE test_payload
(
    a Int64,
    s String,
    arr Array(UInt64),
    m Map(String, UInt64),
    v Variant(UInt64, String),
    lc LowCardinality(String)
)
ENGINE = MergeTree() ORDER BY a;

INSERT INTO test_payload SELECT
    number % 3,
    concat('s', toString(number)),
    range(number % 4),
    map('a', number, 'b', number * 2),
    if(number % 2 = 0, number::Variant(UInt64, String), concat('v', toString(number))::Variant(UInt64, String)),
    toString(number % 2)
FROM numbers(9);

CREATE TABLE test_probe (a Int64) ENGINE = MergeTree() ORDER BY a;
INSERT INTO test_probe SELECT number FROM numbers(5);

SELECT a, s, arr, m, variantType(v), toString(v), lc
FROM test_probe LEFT JOIN test_payload USING (a)
ORDER BY a, s
SETTINGS join_algorithm = 'hash', allow_experimental_join_right_table_sorting = 1,
    join_to_sort_minimum_perkey_rows = 2, join_to_sort_maximum_table_rows = 10000,
    query_plan_join_swap_table = 0, join_use_nulls = 0, joined_block_split_single_row = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000;

SELECT 'reranged', count(), sum(cityHash64(a, s, arr, m, toString(v), lc))
FROM test_probe LEFT JOIN test_payload USING (a)
SETTINGS join_algorithm = 'hash', allow_experimental_join_right_table_sorting = 1,
    join_to_sort_minimum_perkey_rows = 2, join_to_sort_maximum_table_rows = 10000,
    query_plan_join_swap_table = 0, join_use_nulls = 0, joined_block_split_single_row = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000;

SELECT 'full_sorting_merge', count(), sum(cityHash64(a, s, arr, m, toString(v), lc))
FROM test_probe LEFT JOIN test_payload USING (a)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0;

drop table test_payload;
drop table test_probe;
