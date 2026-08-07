-- Related to https://github.com/ClickHouse/ClickHouse/issues/69829
--
-- The main goal of the test is to assert that constant transformation
-- for set constant while partition pruning won't be performed
-- if it's not allowed (NOT IN operator case)

DROP TABLE IF EXISTS 03269_filters;
CREATE TABLE 03269_filters (
    id Int32,
    dt Date
)
engine = MergeTree
order by id;

INSERT INTO 03269_filters
SELECT 6, '2020-01-01'
UNION ALL
SELECT 38, '2021-01-01';

SELECT '-- Monotonic function in partition key';

DROP TABLE IF EXISTS 03269_single_monotonic;
CREATE TABLE 03269_single_monotonic(
    id Int32
)
ENGINE = MergeTree
PARTITION BY intDiv(id, 10)
ORDER BY id;

INSERT INTO 03269_single_monotonic SELECT number FROM numbers(50);

SELECT count() FROM 03269_single_monotonic WHERE id NOT IN (6, 38);
SELECT count() FROM 03269_single_monotonic WHERE id NOT IN (
    SELECT id FROM 03269_filters
);

DROP TABLE 03269_single_monotonic;

SELECT '-- Non-monotonic function in partition key';

DROP TABLE IF EXISTS 03269_single_non_monotonic;
CREATE TABLE 03269_single_non_monotonic (
    id Int32
)
ENGINE = MergeTree
PARTITION BY id % 10
ORDER BY id;

INSERT INTO 03269_single_non_monotonic SELECT number FROM numbers(50);

SELECT count() FROM 03269_single_non_monotonic WHERE id NOT IN (6, 38);
SELECT count() FROM 03269_single_non_monotonic WHERE id NOT IN (SELECT id FROM 03269_filters);

DROP TABLE 03269_single_non_monotonic;

SELECT '-- Multiple partition columns';

DROP TABLE IF EXISTS 03269_multiple_part_cols;
CREATE TABLE 03269_multiple_part_cols (
    id Int32,
    dt Date,
)
ENGINE = MergeTree
PARTITION BY (dt, intDiv(id, 10))
ORDER BY id;

INSERT INTO 03269_multiple_part_cols
SELECT number, '2020-01-01' FROM numbers(50)
UNION ALL
SELECT number, '2021-01-01' FROM numbers(50);

SELECT count() FROM 03269_multiple_part_cols WHERE dt NOT IN ('2020-01-01');
SELECT count() FROM 03269_multiple_part_cols WHERE dt NOT IN (SELECT dt FROM 03269_filters WHERE dt < '2021-01-01');

SELECT count() FROM 03269_multiple_part_cols WHERE id NOT IN (6, 38);
SELECT count() FROM 03269_multiple_part_cols WHERE id NOT IN (SELECT id FROM 03269_filters);

SELECT count() FROM 03269_multiple_part_cols WHERE (id, dt) NOT IN ((6, '2020-01-01'), (38, '2021-01-01'));
SELECT count() FROM 03269_multiple_part_cols WHERE (id, dt) NOT IN (SELECT id, dt FROM 03269_filters);

DROP TABLE 03269_multiple_part_cols;
