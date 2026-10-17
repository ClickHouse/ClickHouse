-- A `LOOKUP INDEX` is stored in the replicated table metadata in ZooKeeper as an extra line that
-- replicas running an older build ignore and drop on the next metadata rewrite, so the experimental
-- feature is rejected for `ReplicatedMergeTree` altogether.

SET allow_experimental_lookup_index = 1;

DROP TABLE IF EXISTS lookup_replicated SYNC;

CREATE TABLE lookup_replicated
(
    id UInt64,
    value String,
    LOOKUP INDEX idx_set (id) TYPE table_set
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/lookup_replicated', '1')
ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE lookup_replicated
(
    id UInt64,
    value String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/lookup_replicated', '1')
ORDER BY id;

ALTER TABLE lookup_replicated ADD LOOKUP INDEX idx_set (id) TYPE table_set; -- { serverError SUPPORT_IS_DISABLED }

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'lookup_replicated';

DROP TABLE lookup_replicated SYNC;
