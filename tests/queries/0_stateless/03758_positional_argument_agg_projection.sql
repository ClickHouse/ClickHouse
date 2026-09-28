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


SET enable_positional_arguments_for_projections=1;

DROP TABLE IF EXISTS test2;
CREATE TABLE test2
(
    user_id UInt64,

    PROJECTION prj
    (
        SELECT
            CAST(user_id, 'String') AS user_id
        GROUP BY
            user_id
    )
)
ENGINE = MergeTree
ORDER BY (user_id);

-- Projection ORDER BY is analyzed as a key expression, not as a positional
-- reference to the SELECT output. Even with positional projection arguments
-- enabled, these declarations cannot be stored and later become unavailable.
SET enable_positional_arguments_for_projections = 1;

CREATE TABLE test_order_star
(
    a UInt64,
    b UInt64,
    PROJECTION p (SELECT * ORDER BY 2)
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

CREATE TABLE test_order_columns
(
    a UInt64,
    b UInt64,
    PROJECTION p (SELECT COLUMNS('^(a|b)$') ORDER BY 2)
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

-- A column expression in the same projection ORDER BY remains valid.
CREATE TABLE test_order_column
(
    a UInt64,
    b UInt64,
    PROJECTION p (SELECT * ORDER BY b)
)
ENGINE = MergeTree ORDER BY tuple();
DROP TABLE test_order_column;

CREATE TABLE test_order_alter (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE test_order_alter ADD PROJECTION p_star (SELECT * ORDER BY 2); -- { serverError ILLEGAL_COLUMN }
ALTER TABLE test_order_alter ADD PROJECTION p_columns (SELECT COLUMNS('^(a|b)$') ORDER BY 2); -- { serverError ILLEGAL_COLUMN }
DROP TABLE test_order_alter;

SET enable_positional_arguments_for_projections=0;

DROP TABLE IF EXISTS test3;
CREATE TABLE test3
(
    user_id UInt64,

    PROJECTION prj
    (
        SELECT
            CAST(user_id, 'String') AS user_id
        GROUP BY
            user_id
    )
)
ENGINE = MergeTree
ORDER BY (user_id);
