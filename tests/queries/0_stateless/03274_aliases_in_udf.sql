-- Tags: no-parallel

SET skip_redundant_aliases_in_udf = 0;

SELECT 'FIX ISSUE #69143';

DROP TABLE IF EXISTS test_table;

CREATE FUNCTION IF NOT EXISTS 03274_test_function AS ( input_column_name ) -> ((
        '1' AS a,
        input_column_name AS input_column_name
    ).2);

CREATE TABLE IF NOT EXISTS test_table
(
    `metadata_a` String,
    `metadata_b` String
)
ENGINE = MergeTree()
ORDER BY tuple();

ALTER TABLE test_table ADD COLUMN mat_a String MATERIALIZED 03274_test_function(metadata_a);
ALTER TABLE test_table MATERIALIZE COLUMN `mat_a`;

ALTER TABLE test_table ADD COLUMN mat_b String MATERIALIZED 03274_test_function(metadata_b); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SET skip_redundant_aliases_in_udf = 1;

ALTER TABLE test_table ADD COLUMN mat_b String MATERIALIZED 03274_test_function(metadata_b);
ALTER TABLE test_table MATERIALIZE COLUMN `mat_b`;

INSERT INTO test_table SELECT 'a', 'b';

SELECT mat_a FROM test_table;
SELECT mat_b FROM test_table;

SELECT 'EXPLAIN SYNTAX OF UDF';

CREATE FUNCTION IF NOT EXISTS test_03274 AS ( x ) -> ((x + 1 as y, y + 2));

SET skip_redundant_aliases_in_udf = 0;

EXPLAIN SYNTAX SELECT test_03274(4 + 2);

SET skip_redundant_aliases_in_udf = 1;

EXPLAIN SYNTAX SELECT test_03274(4 + 2);

DROP FUNCTION 03274_test_function;
DROP FUNCTION test_03274;
DROP TABLE IF EXISTS test_table;
