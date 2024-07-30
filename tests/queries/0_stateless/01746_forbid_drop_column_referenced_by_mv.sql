-- MergeTree
DROP TABLE IF EXISTS `01746_merge_tree`;
CREATE TABLE `01746_merge_tree`
(
    `n1` Int8,
    `n2` Int8,
    `n3` Int8,
    `n4` Int8
)
ENGINE = MergeTree
ORDER BY n1;

DROP TABLE IF EXISTS `01746_merge_tree_mv`;
CREATE MATERIALIZED VIEW `01746_merge_tree_mv`
ENGINE = Memory AS
SELECT
    n2,
    n3
FROM `01746_merge_tree`;

ALTER TABLE `01746_merge_tree`
    DROP COLUMN n3;  -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

ALTER TABLE `01746_merge_tree`
    DROP COLUMN n2;  -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- ok
ALTER TABLE `01746_merge_tree`
    DROP COLUMN n4;

DROP TABLE `01746_merge_tree`;
DROP TABLE `01746_merge_tree_mv`;

-- Null 
DROP TABLE IF EXISTS `01746_null`;
CREATE TABLE `01746_null`
(
    `n1` Int8,
    `n2` Int8,
    `n3` Int8
)
ENGINE = Null;

DROP TABLE IF EXISTS `01746_null_mv`;
CREATE MATERIALIZED VIEW `01746_null_mv`
ENGINE = Memory AS
SELECT
    n1,
    n2
FROM `01746_null`;

ALTER TABLE `01746_null`
    DROP COLUMN n1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

ALTER TABLE `01746_null`
    DROP COLUMN n2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- ok
ALTER TABLE `01746_null`
    DROP COLUMN n3;

DROP TABLE `01746_null`;
DROP TABLE `01746_null_mv`;

-- Distributed

DROP TABLE IF EXISTS `01746_local`;
CREATE TABLE `01746_local`
(
    `n1` Int8,
    `n2` Int8,
    `n3` Int8
)
ENGINE = Memory;

DROP TABLE IF EXISTS `01746_dist`;
CREATE TABLE `01746_dist` AS `01746_local`
ENGINE = Distributed('test_shard_localhost', currentDatabase(), `01746_local`, rand());

DROP TABLE IF EXISTS `01746_dist_mv`;
CREATE MATERIALIZED VIEW `01746_dist_mv`
ENGINE = Memory AS
SELECT
    n1,
    n2
FROM `01746_dist`;

ALTER TABLE `01746_dist`
    DROP COLUMN n1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

ALTER TABLE `01746_dist`
    DROP COLUMN n2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- ok
ALTER TABLE `01746_dist`
    DROP COLUMN n3;

DROP TABLE `01746_local`;
DROP TABLE `01746_dist`;
DROP TABLE `01746_dist_mv`;

-- Merge
DROP TABLE IF EXISTS `01746_merge_t`;
CREATE TABLE `01746_merge_t`
(
    `n1` Int8,
    `n2` Int8,
    `n3` Int8
)
ENGINE = Memory;

DROP TABLE IF EXISTS `01746_merge`;
CREATE TABLE `01746_merge` AS `01746_merge_t`
ENGINE = Merge(currentDatabase(), '01746_merge_t');

DROP TABLE IF EXISTS `01746_merge_mv`;
CREATE MATERIALIZED VIEW `01746_merge_mv`
ENGINE = Memory AS
SELECT
    n1,
    n2
FROM `01746_merge`;

ALTER TABLE `01746_merge`
    DROP COLUMN n1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

ALTER TABLE `01746_merge`
    DROP COLUMN n2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- ok
ALTER TABLE `01746_merge`
    DROP COLUMN n3;

DROP TABLE `01746_merge_t`;
DROP TABLE `01746_merge`;
DROP TABLE `01746_merge_mv`;

-- Buffer
DROP TABLE IF EXISTS `01746_buffer_t`;
CREATE TABLE `01746_buffer_t`
(
    `n1` Int8,
    `n2` Int8,
    `n3` Int8
)
ENGINE = Memory;

DROP TABLE IF EXISTS `01746_buffer`;
CREATE TABLE `01746_buffer` AS `01746_buffer_t`
ENGINE = Buffer(currentDatabase(), `01746_buffer_t`, 16, 10, 100, 10000, 1000000, 10000000, 100000000);

DROP TABLE IF EXISTS `01746_buffer_mv`;
CREATE MATERIALIZED VIEW `01746_buffer_mv`
ENGINE = Memory AS
SELECT
    n1,
    n2
FROM `01746_buffer`;

ALTER TABLE `01746_buffer`
    DROP COLUMN n1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

ALTER TABLE `01746_buffer`
    DROP COLUMN n2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- ok
ALTER TABLE `01746_buffer`
    DROP COLUMN n3;

DROP TABLE `01746_buffer_t`;
DROP TABLE `01746_buffer`;
DROP TABLE `01746_buffer_mv`;
