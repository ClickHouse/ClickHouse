DROP TABLE IF EXISTS `01851_merge_tree`;
CREATE TABLE `01851_merge_tree`
(
    `n1` Int8,
    `n2` Int8,
    `n3` Int8,
    `n4` Int8
)
ENGINE = MergeTree
ORDER BY n1;

DROP TABLE IF EXISTS `001851_merge_tree_mv`;
CREATE MATERIALIZED VIEW `01851_merge_tree_mv`
ENGINE = Memory AS
SELECT
    n2,
    n3
FROM `01851_merge_tree`;

ALTER TABLE `01851_merge_tree`
    DROP COLUMN n3;  -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

ALTER TABLE `01851_merge_tree`
    DROP COLUMN n2;  -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- ok
ALTER TABLE `01851_merge_tree`
    DROP COLUMN n4;

-- CLEAR COLUMN is OK
ALTER TABLE `01851_merge_tree`
    CLEAR COLUMN n2;

DROP TABLE `01851_merge_tree`;
DROP TABLE `01851_merge_tree_mv`;
