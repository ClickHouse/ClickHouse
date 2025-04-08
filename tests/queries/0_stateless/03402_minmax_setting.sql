
DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    `a` int,
    `b` int, INDEX idx b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO test Values(1, 1);

ALTER TABLE test MODIFY SETTING secondary_indices_on_columns_alter = 'throw';

ALTER TABLE test MODIFY COLUMN b String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
