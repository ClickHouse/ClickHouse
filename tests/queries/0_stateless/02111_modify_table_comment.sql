-- Tags: no-parallel
-- Disable force_primary_key_reverse_order: SHOW CREATE output contains ORDER BY which changes with forced DESC
SET force_primary_key_reverse_order = 0;

DROP DATABASE IF EXISTS 02111_modify_table_comment;
CREATE DATABASE 02111_modify_table_comment;

USE 02111_modify_table_comment;

CREATE TABLE t
(
    `n` Int8
)
ENGINE = MergeTree
ORDER BY n
COMMENT 'this is a MergeTree table';

SHOW CREATE t;

ALTER TABLE t
    MODIFY COMMENT 'MergeTree Table';

SHOW CREATE t;

CREATE TABLE t_merge AS t
ENGINE = Merge('02111_modify_table_comment', 't')
COMMENT 'this is a Merge table';

SHOW CREATE t_merge;

ALTER TABLE t_merge
    MODIFY COMMENT 'Merge Table';

SHOW CREATE t_merge;

DROP DATABASE 02111_modify_table_comment;
