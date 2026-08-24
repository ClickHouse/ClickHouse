DROP TABLE IF EXISTS `03604_test`;

SET allow_experimental_lightweight_update = 1;

-- catch error BAD_ARGUMENTS
SET merge_tree_min_read_task_size = 0; -- { serverError BAD_ARGUMENTS }

CREATE TABLE `03604_test` (c0 Int)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO TABLE `03604_test` (c0) VALUES (1);

DELETE FROM `03604_test` WHERE c0 = 2;
UPDATE `03604_test` SET c0 = 3 WHERE TRUE;

-- catch error BAD_ARGUMENTS
SELECT count()
FROM `03604_test`
SETTINGS apply_mutations_on_fly = 1, merge_tree_min_read_task_size = 0; -- {clientError BAD_ARGUMENTS}

DROP TABLE `03604_test`;