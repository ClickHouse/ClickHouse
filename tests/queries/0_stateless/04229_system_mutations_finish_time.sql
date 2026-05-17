-- Check that non-replicated MergeTree mutations expose `finish_time` in `system.mutations`.
-- Cover immediate completion on an empty table, a queued unfinished mutation, and multiple mutations finishing in order.

DROP TABLE IF EXISTS system_mutations_finish_time SYNC;
DROP TABLE IF EXISTS system_mutations_finish_time_empty SYNC;

-- A mutation on an empty table has no parts to process and should get `finish_time` immediately.
CREATE TABLE system_mutations_finish_time_empty
(
    id UInt64,
    value UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS finished_mutations_to_keep = 100;

ALTER TABLE system_mutations_finish_time_empty
    UPDATE value = value + 1
    WHERE id < 10
    SETTINGS mutations_sync = 1;

SELECT
    'empty',
    count(),
    countIf(finish_time != toDateTime(0)),
    countIf(finish_time >= create_time),
    countIf(is_done = 1),
    sum(parts_to_do)
FROM system.mutations
WHERE database = currentDatabase()
  AND table = 'system_mutations_finish_time_empty';

CREATE TABLE system_mutations_finish_time
(
    id UInt64,
    value UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS finished_mutations_to_keep = 100;

INSERT INTO system_mutations_finish_time SELECT number, number FROM numbers(20);

-- Stop mutation execution to make the pending `finish_time = 0` state deterministic.
SYSTEM STOP MERGES system_mutations_finish_time;

ALTER TABLE system_mutations_finish_time
    UPDATE value = value + 1
    WHERE id < 10
    SETTINGS mutations_sync = 0;

ALTER TABLE system_mutations_finish_time
    UPDATE value = value + 1
    WHERE id < 5
    SETTINGS mutations_sync = 0;

SELECT
    'unfinished',
    count(),
    countIf(finish_time = toDateTime(0)),
    countIf(is_done = 0),
    sum(parts_to_do > 0)
FROM system.mutations
WHERE database = currentDatabase()
  AND table = 'system_mutations_finish_time';

-- Restart mutation execution. The synchronous third mutation waits for the earlier
-- queued mutations and leaves all retained entries visible in `system.mutations`.
SYSTEM START MERGES system_mutations_finish_time;

ALTER TABLE system_mutations_finish_time
    UPDATE value = value + 1
    WHERE id < 3
    SETTINGS mutations_sync = 1;

SELECT
    'finished',
    count(),
    countIf(finish_time != toDateTime(0)),
    countIf(finish_time >= create_time),
    countIf(is_done = 1),
    sum(parts_to_do)
FROM system.mutations
WHERE database = currentDatabase()
  AND table = 'system_mutations_finish_time';

-- The three mutations update 10, 5, and 3 rows.
SELECT sum(value - id) FROM system_mutations_finish_time;

DROP TABLE system_mutations_finish_time SYNC;
DROP TABLE system_mutations_finish_time_empty SYNC;
