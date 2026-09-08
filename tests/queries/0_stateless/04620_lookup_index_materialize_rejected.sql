-- Lookup indices are in-memory structures rebuilt on demand and have no materialization path.
-- `MATERIALIZE INDEX <lookup_name>` must be rejected up front instead of being queued as a
-- silent no-op mutation.

SET allow_experimental_lookup_index = 1;

DROP TABLE IF EXISTS table_lookup_materialize SYNC;

CREATE TABLE table_lookup_materialize
(
    id UInt64,
    value String,
    INDEX idx_skip value TYPE set(0) GRANULARITY 1,
    LOOKUP INDEX idx_lookup (id) TYPE table_set
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO table_lookup_materialize VALUES (1, 'a'), (2, 'b');

-- Materializing a skip index still works.
ALTER TABLE table_lookup_materialize MATERIALIZE INDEX idx_skip SETTINGS mutations_sync = 2;

-- Materializing a lookup index is rejected.
ALTER TABLE table_lookup_materialize MATERIALIZE INDEX idx_lookup; -- { serverError BAD_ARGUMENTS }

-- A nonexistent index name keeps the old skip-with-warning behavior.
ALTER TABLE table_lookup_materialize MATERIALIZE INDEX idx_nonexistent SETTINGS mutations_sync = 2;

SELECT count() FROM table_lookup_materialize;

DROP TABLE table_lookup_materialize SYNC;
