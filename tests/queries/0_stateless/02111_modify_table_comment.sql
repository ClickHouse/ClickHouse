-- Tags: no-parallel

DROP DATABASE IF EXISTS _02111_modify_table_comment;
CREATE DATABASE _02111_modify_table_comment;

USE _02111_modify_table_comment;

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
ENGINE = Merge('_02111_modify_table_comment', 't')
COMMENT 'this is a Merge table';

SHOW CREATE t_merge;

ALTER TABLE t_merge
    MODIFY COMMENT 'Merge Table';

SHOW CREATE t_merge;

DROP DATABASE _02111_modify_table_comment;
