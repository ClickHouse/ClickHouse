CREATE TABLE test0 (
        c0 UInt64
    ) ENGINE = MergeTree() PARTITION BY c0 ORDER BY c0;

SET allow_experimental_live_view=1;

CREATE LIVE VIEW live1 AS SELECT * FROM test0;

ALTER LIVE VIEW live1 REFRESH; -- success
