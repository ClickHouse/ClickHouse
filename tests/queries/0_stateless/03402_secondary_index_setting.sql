
DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    `a` int,
    `b` int,
    `c` int,
    INDEX idx_minmax b TYPE minmax GRANULARITY 1,
    INDEX idx_set c TYPE set(100)
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO test Values(1, 1, 1);

-- default
ALTER TABLE test MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- throw
ALTER TABLE test MODIFY SETTING secondary_indices_on_columns_alter = 'throw';

ALTER TABLE test MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- ignore
ALTER TABLE test MODIFY SETTING secondary_indices_on_columns_alter = 'ignore';

ALTER TABLE test MODIFY COLUMN b String;

-- ALTER TABLE test MODIFY SETTING secondary_indices_on_columns_alter = 'drop';

-- ALTER TABLE test MODIFY COLUMN b String;

-- ALTER TABLE test UPDATE b = 2 WHERE b = 1;

DROP TABLE test;