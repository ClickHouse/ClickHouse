-- https://github.com/ClickHouse/ClickHouse/issues/55843
-- These tests pass without the fix when either of
--   - optimize_read_in_window_order = 0 and optimize_read_in_order = 0
--   - ratio_of_defaults_for_sparse_serialization = 1
-- However it is better to leave the settings as randomized because we run
-- stateless tests quite a few times during a PR, so if a bug is introduced
-- then there is a big chance of catching it. Furthermore, randomized settings
-- might identify new bugs.

CREATE TABLE test1
(
    id String,
    time DateTime64(9),
    key Int64,
    value Bool,
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(time)
ORDER BY (key, id, time);

INSERT INTO test1 VALUES ('id0', now(), 3, false);

SELECT last_value(value) OVER (PARTITION BY id ORDER BY time ASC) as last_value
FROM test1
WHERE (key = 3);

SELECT last_value(value) OVER (ORDER BY time ASC) as last_value
FROM test1
WHERE (key = 3);

SELECT last_value(value) OVER (PARTITION BY id ORDER BY time ASC) as last_value
FROM test1;



CREATE TABLE test2
(
    time DateTime,
    value String
)
ENGINE = MergeTree
ORDER BY (time) AS SELECT 0, '';

SELECT any(value) OVER (ORDER BY time ASC) FROM test2;
SELECT last_value(value) OVER (ORDER BY time ASC) FROM test2;
