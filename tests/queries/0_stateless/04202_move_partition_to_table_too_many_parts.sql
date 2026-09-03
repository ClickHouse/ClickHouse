-- Test: exercises back-pressure on `MOVE PARTITION ... TO TABLE`.
-- Covers: src/Storages/StorageMergeTree.cpp `delayInsertOrThrowIfNeeded(nullptr, local_context, true)`
-- which now applies `parts_to_throw_insert`/`max_parts_in_total` limits to MOVEs.

DROP TABLE IF EXISTS source_t;
DROP TABLE IF EXISTS dest_t;

CREATE TABLE source_t (x UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dest_t   (x UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS parts_to_delay_insert = 100, parts_to_throw_insert = 100, max_avg_part_size_for_too_many_parts = 0;

SYSTEM STOP MERGES dest_t;
SYSTEM STOP MERGES source_t;

-- Populate destination so that, after the threshold is lowered, dest exceeds parts_to_throw_insert.
INSERT INTO dest_t VALUES (1);
INSERT INTO dest_t VALUES (2);
INSERT INTO source_t VALUES (3);

-- Lower destination thresholds so that the destination is over parts_to_throw_insert.
ALTER TABLE dest_t MODIFY SETTING parts_to_delay_insert = 1, parts_to_throw_insert = 1;

-- The MOVE must apply destination's INSERT back-pressure and throw TOO_MANY_PARTS.
ALTER TABLE source_t MOVE PARTITION tuple() TO TABLE dest_t; -- { serverError TOO_MANY_PARTS }

-- The source partition must remain on the source after the failed move.
SELECT count() FROM source_t;
SELECT count() FROM dest_t;

-- Now relax destination limits, the move should succeed.
ALTER TABLE dest_t MODIFY SETTING parts_to_delay_insert = 1000, parts_to_throw_insert = 1000;
ALTER TABLE source_t MOVE PARTITION tuple() TO TABLE dest_t;

SELECT count() FROM source_t;
SELECT count() FROM dest_t;

DROP TABLE source_t;
DROP TABLE dest_t;
