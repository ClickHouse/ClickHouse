SET mutations_sync=2;

DROP TABLE IF EXISTS rep_data;
CREATE TABLE rep_data
(
    p Int,
    t DateTime,
    INDEX idx t TYPE minmax GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rep_data', '1')
PARTITION BY p
ORDER BY t
SETTINGS number_of_free_entries_in_pool_to_execute_mutation=0;
INSERT INTO rep_data VALUES (1, now());
ALTER TABLE rep_data MATERIALIZE INDEX idx IN PARTITION ID 'NO_SUCH_PART'; -- { serverError 248 }
ALTER TABLE rep_data MATERIALIZE INDEX idx IN PARTITION ID '1';
ALTER TABLE rep_data MATERIALIZE INDEX idx IN PARTITION ID '2';

DROP TABLE IF EXISTS data;
CREATE TABLE data
(
    p Int,
    t DateTime,
    INDEX idx t TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY t
SETTINGS number_of_free_entries_in_pool_to_execute_mutation=0;
INSERT INTO data VALUES (1, now());
ALTER TABLE data MATERIALIZE INDEX idx IN PARTITION ID 'NO_SUCH_PART'; -- { serverError 341 }
ALTER TABLE data MATERIALIZE INDEX idx IN PARTITION ID '1';
ALTER TABLE data MATERIALIZE INDEX idx IN PARTITION ID '2';
