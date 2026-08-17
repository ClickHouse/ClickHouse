-- Tags: no-random-settings, no-random-merge-tree-settings
DROP TABLE IF EXISTS t;

CREATE TABLE t (
    key UInt64,
    value String
)
ENGINE MergeTree()
ORDER by key SETTINGS index_granularity = 10, index_granularity_bytes = '1024K';

ALTER TABLE t MODIFY SETTING enable_index_granularity_compression = 0;

INSERT INTO t SELECT number, toString(number) FROM numbers(100);

ALTER TABLE t MODIFY SETTING enable_index_granularity_compression = 1;

INSERT INTO t SELECT number, toString(number) FROM numbers(100);

SELECT index_granularity_bytes_in_memory, index_granularity_bytes_in_memory_allocated FROM system.parts where table = 't' and database = currentDatabase() ORDER BY name;

DROP TABLE IF EXISTS t;
