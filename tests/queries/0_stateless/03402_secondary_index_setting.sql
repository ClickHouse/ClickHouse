
DROP TABLE IF EXISTS test_compact;

CREATE TABLE test_compact
(
    `a` int,
    `b` int,
    `c` int,
    INDEX idx_minmax b TYPE minmax GRANULARITY 1,
    INDEX idx_set c TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY a
SETTINGS min_bytes_for_wide_part = 10485760,
compress_marks = 1;

INSERT INTO test_compact Values(1, 1, 1);

-- default
ALTER TABLE test_compact MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- throw
ALTER TABLE test_compact MODIFY SETTING secondary_indices_on_columns_alter = 'throw';

ALTER TABLE test_compact MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- drop
ALTER TABLE test_compact MODIFY SETTING secondary_indices_on_columns_alter = 'drop';

ALTER TABLE test_compact MODIFY COLUMN b String;

SELECT secondary_indices_marks_bytes
FROM system.parts
WHERE (`table` = 'test_compact') AND (active = 1) AND (database = currentDatabase());

-- rebuild
ALTER TABLE test_compact MODIFY SETTING secondary_indices_on_columns_alter = 'rebuild';

ALTER TABLE test_compact MODIFY COLUMN b int;

SELECT secondary_indices_marks_bytes
FROM system.parts
WHERE (`table` = 'test_compact') AND (active = 1) AND (database = currentDatabase());

-- ignore
ALTER TABLE test_compact MODIFY SETTING secondary_indices_on_columns_alter = 'ignore';

ALTER TABLE test_compact MODIFY COLUMN b String;

DROP TABLE test_compact;


DROP TABLE IF EXISTS test_wide;

CREATE TABLE test_wide
(
    `a` int,
    `b` int,
    `c` int,
    INDEX idx_minmax b TYPE minmax GRANULARITY 1,
    INDEX idx_set c TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY a
SETTINGS min_bytes_for_wide_part = 0,
compress_marks = 1;

INSERT INTO test_wide Values(1, 1, 1);

-- default
ALTER TABLE test_wide MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- throw
ALTER TABLE test_wide MODIFY SETTING secondary_indices_on_columns_alter = 'throw';

ALTER TABLE test_wide MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- drop
ALTER TABLE test_wide MODIFY SETTING secondary_indices_on_columns_alter = 'drop';

ALTER TABLE test_wide MODIFY COLUMN b String;

SELECT secondary_indices_marks_bytes
FROM system.parts
WHERE (`table` = 'test_wide') AND (active = 1) AND (database = currentDatabase());

-- rebuild
ALTER TABLE test_wide MODIFY SETTING secondary_indices_on_columns_alter = 'rebuild';

ALTER TABLE test_wide MODIFY COLUMN b int;

SELECT secondary_indices_marks_bytes
FROM system.parts
WHERE (`table` = 'test_wide') AND (active = 1) AND (database = currentDatabase());

-- ignore
ALTER TABLE test_wide MODIFY SETTING secondary_indices_on_columns_alter = 'ignore';

ALTER TABLE test_wide MODIFY COLUMN b String;

DROP TABLE test_wide;
