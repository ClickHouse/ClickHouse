SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS test_table_1;
CREATE TABLE test_table_1
(
    pkey UInt32,
    c8 UInt32,
    c9 String,
    c10 Float32,
    c11 String
) ENGINE = MergeTree ORDER BY pkey;

DROP TABLE IF EXISTS test_table_2;
CREATE TABLE test_table_2
(
    vkey UInt32,
    pkey UInt32,
    c15 UInt32
) ENGINE = MergeTree ORDER BY vkey;

WITH test_cte AS
(
    SELECT
        ref_10.c11 as c_2_c2350_1,
        ref_9.c9 as c_2_c2351_2
    FROM
        test_table_1 as ref_9
        RIGHT OUTER JOIN test_table_1 as ref_10 ON (ref_9.c11 = ref_10.c9)
        INNER JOIN test_table_2 as ref_11 ON (ref_10.c8 = ref_11.vkey)
    WHERE ((ref_10.pkey + ref_11.pkey) BETWEEN ref_11.vkey AND (CASE WHEN (-30.87 >= ref_9.c10) THEN ref_11.c15 ELSE ref_11.pkey END))
)
SELECT ref_13.c_2_c2350_1 as c_2_c2357_3 FROM test_cte as ref_13 WHERE (ref_13.c_2_c2351_2) in (select ref_14.c_2_c2351_2 as c_5_c2352_0 FROM test_cte as ref_14);

DROP TABLE test_table_1;
DROP TABLE test_table_2;
