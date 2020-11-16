DROP TABLE IF EXISTS data_01285;

SET max_threads=1;


CREATE TABLE data_01285 (
    key   Int,
    value SimpleAggregateFunction(max, Nullable(Int)),
    INDEX value_idx assumeNotNull(value) TYPE minmax GRANULARITY 1
)
ENGINE=AggregatingMergeTree()
ORDER BY key;

SELECT 'INSERT';
INSERT INTO data_01285 SELECT 1, number FROM numbers(2);
SELECT * FROM data_01285;
SELECT * FROM data_01285 WHERE assumeNotNull(value) = 1;
SELECT 'INSERT';
INSERT INTO data_01285 SELECT 1, number FROM numbers(4);
SELECT * FROM data_01285;
SELECT * FROM data_01285 WHERE assumeNotNull(value) = 1;
SELECT * FROM data_01285 WHERE assumeNotNull(value) = 3;
SELECT 'OPTIMIZE';
OPTIMIZE TABLE data_01285 FINAL;
SELECT * FROM data_01285;
-- before the fix value_idx contains one range {0, 0}
-- and hence cannot find these record.
SELECT * FROM data_01285 WHERE assumeNotNull(value) = 3;
-- one more time just in case
SELECT 'OPTIMIZE';
OPTIMIZE TABLE data_01285 FINAL;
SELECT * FROM data_01285;
-- and this passes even without fix.
SELECT * FROM data_01285 WHERE assumeNotNull(value) = 3;
