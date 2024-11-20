SELECT "FIX ISSUE #69143";

CREATE OR REPLACE FUNCTION test_function AS ( input_column_name ) -> ((
        '1' AS a,
        input_column_name AS input_column_name
    ).2);

CREATE TABLE test_table
(
    `metadata_a` String,
    `metadata_b` String
)
ENGINE = MergeTree()
ORDER BY tuple();


ALTER TABLE test_table ADD COLUMN mat_a String MATERIALIZED test_function(metadata_a);
ALTER TABLE test_table MATERIALIZE COLUMN `mat_a`;

ALTER TABLE test_table ADD COLUMN mat_b String MATERIALIZED test_function(metadata_b);
ALTER TABLE test_table MATERIALIZE COLUMN `mat_b`;

INSERT INTO test_table SELECT 'a', 'b';

SELECT mat_a FROM test_table;
SELECT mat_b FROM test_table;

SELECT "EXPAIN SYNTAX OF UDF";

CREATE OR REPLACE FUNCTION test_03274 AS ( x ) -> ((x + 1 as y, y + 2));

EXPAIN SYNTAX SELECT test_03274(4 + 2);
