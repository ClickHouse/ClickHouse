-- Lookup indices are in-memory structures and do not have per-part files to clear.
-- `CLEAR INDEX <lookup_name>` must be rejected instead of being queued as a silent no-op.

SET allow_experimental_lookup_index = 1;
SET validate_mutation_query = 0;

DROP TABLE IF EXISTS table_lookup_clear SYNC;

CREATE TABLE table_lookup_clear
(
    id UInt64,
    value String,
    LOOKUP INDEX idx_lookup (id) TYPE table_set
)
ENGINE = MergeTree
PARTITION BY id
ORDER BY id;

INSERT INTO table_lookup_clear VALUES (1, 'a'), (2, 'b');

ALTER TABLE table_lookup_clear CLEAR INDEX idx_lookup IN PARTITION 1; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM table_lookup_clear;

DROP TABLE table_lookup_clear SYNC;
