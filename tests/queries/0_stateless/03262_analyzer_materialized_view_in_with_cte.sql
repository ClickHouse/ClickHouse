SET enable_analyzer = 1;

DROP TABLE IF EXISTS mv_test;
DROP TABLE IF EXISTS mv_test_target;
DROP VIEW IF EXISTS mv_test_mv;

CREATE TABLE mv_test
(
    `id` UInt64,
    `ref_id` UInt64,
    `final_id` Nullable(UInt64),
    `display` String
)
ENGINE = Log;

CREATE TABLE mv_test_target
(
    `id` UInt64,
    `ref_id` UInt64,
    `final_id` Nullable(UInt64),
    `display` String
)
ENGINE = Log;

CREATE MATERIALIZED VIEW mv_test_mv TO mv_test_target
(
    `id` UInt64,
    `ref_id` UInt64,
    `final_id` Nullable(UInt64),
    `display` String
)
AS WITH
    tester AS
    (
        SELECT
            id,
            ref_id,
            final_id,
            display
        FROM mv_test
    ),
    id_set AS
    (
        SELECT
            display,
            max(id) AS max_id
        FROM mv_test
        GROUP BY display
    )
SELECT *
FROM tester
WHERE id IN (
    SELECT max_id
    FROM id_set
);

INSERT INTO mv_test ( id, ref_id, display) values ( 1, 2, 'test');

SELECT * FROM mv_test_target;

DROP VIEW mv_test_mv;
DROP TABLE mv_test_target;
DROP TABLE mv_test;
