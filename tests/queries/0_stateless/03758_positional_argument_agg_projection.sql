DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    `a` UInt64,
    `b` String
)
ENGINE = MergeTree
ORDER BY a;

SET enable_positional_arguments_for_projections = 0;

ALTER TABLE test
    ADD PROJECTION test_projection
    (
        SELECT
            b,
            a
        GROUP BY 1
    ); -- { serverError NOT_AN_AGGREGATE }

SET enable_positional_arguments_for_projections = 1;

ALTER TABLE test
    ADD PROJECTION test_projection
    (
        SELECT
            b,
            a
        GROUP BY 1, 2
    );

DROP TABLE test;
