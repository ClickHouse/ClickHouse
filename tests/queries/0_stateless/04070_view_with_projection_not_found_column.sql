DROP TABLE IF EXISTS test_proj;
DROP VIEW IF EXISTS test_proj_view;

CREATE TABLE test_proj
(
    id UInt64,
    name String,
    PROJECTION by_name
    (
        SELECT *
        ORDER BY name
    )
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test_proj SELECT number, toString(number) FROM numbers(1000000);

CREATE VIEW test_proj_view AS SELECT * FROM test_proj;

SELECT * FROM test_proj_view WHERE name = '42';

DROP VIEW test_proj_view;
DROP TABLE test_proj;
