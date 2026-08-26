-- Regression test for STID 2139-5111: a `direct` JOIN on a MergeTree right table
-- could shuffle the right-side columns out of order. The right block ended up with the
-- schema names/types from `right_sample_block` paired with column data taken from the
-- lookup plan in a different (plan-header) order. Downstream comparisons against those
-- columns then hit `executeGenericIdenticalTypes` with one `Nullable` column and one
-- non-`Nullable` column, raising a `LOGICAL_ERROR` "Columns are assumed to be of identical
-- types, but they are different in Nullable".

DROP TABLE IF EXISTS events_04206;
DROP TABLE IF EXISTS attributes_04206;

SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE events_04206
(
    `Id` UInt64,
    `Idu32` UInt32 MATERIALIZED toUInt32(Id),
    `Idu32n` Nullable(UInt32) MATERIALIZED if(Id == 111, NULL, toUInt32(Id)),
    `Idlcn` LowCardinality(Nullable(UInt64)) MATERIALIZED if(Id == 111, NULL, Id),
    `Idlc` LowCardinality(UInt64) MATERIALIZED Id,
    `Payload` String,
    `Time` DateTime
)
ENGINE = Memory;

INSERT INTO events_04206
SELECT number, concat('Payload_', toString(number)), toDateTime('2024-01-01 00:00:00') + INTERVAL number MINUTE FROM numbers(500)
UNION ALL
SELECT 32, 'Payload_Dup', toDateTime('2024-01-01 00:10:00');
-- Multiple INSERTs / blocks so the join produces multiple chunks
INSERT INTO events_04206 SELECT number, concat('Payload_', toString(number)), toDateTime('2024-01-01 00:00:00') + INTERVAL number MINUTE FROM numbers(500, 500);

-- Right side: column order EventId, OtherId, Attribute. The lookup plan is free to
-- reorder columns and the previous code returned them in plan-header order.
CREATE TABLE attributes_04206
(
    `EventId` UInt64,
    `OtherId` Nullable(UInt32),
    `Attribute` String
)
ENGINE = MergeTree
ORDER BY EventId;

INSERT INTO attributes_04206 SELECT
    sipHash64(number, 1) % 10000000 AS EventId,
    sipHash64(number, 1) % 10000000 AS OtherId,
    concat('Attribute_', toString(number)) AS Attribute
FROM numbers(100000);
INSERT INTO attributes_04206 VALUES (32, 32, 'Attribute_Dup');
INSERT INTO attributes_04206 VALUES (1000001, NULL, 'Attribute_Dup');

-- Trigger the failing path:
-- - LEFT JOIN with `direct` algorithm (DirectKeyValueJoin over MergeTree)
-- - GROUP BY a comparison that mixes a right-side column (EventId UInt64) with a
--   left-side LowCardinality(Nullable(UInt64)). This is what tripped the broken
--   column order: EventId at its right_sample_block position used to contain the
--   OtherId data (Nullable(UInt32)), so the comparison saw a Nullable column where
--   it expected plain UInt64. Several aggregates plus DISTINCT and the LIMIT mirror
--   the AST fuzzer reproducer that originally surfaced the bug.
SELECT DISTINCT
    count() IGNORE NULLS,
    countIf('' != t1.Attribute),
    minOrNullDistinct(sipHash64(t1.Attribute))
FROM events_04206 AS t0
LEFT JOIN attributes_04206 AS t1 ON t1.OtherId = t0.Idu32n
GROUP BY t1.EventId = t0.Idlcn
ORDER BY 1, 2, 3
LIMIT 713
SETTINGS enable_analyzer = 1, join_algorithm = 'direct';

-- SEMI/ANTI joins project no right-side columns, so the lookup requests a minimal column
-- set. The successful-lookup path must still return a chunk whose schema matches the
-- not-found and empty-key paths; otherwise it produces a data-dependent schema mismatch.
SELECT t0.Id
FROM events_04206 AS t0
LEFT SEMI JOIN attributes_04206 AS t1 ON t1.EventId = t0.Id
ORDER BY t0.Id
LIMIT 5
SETTINGS enable_analyzer = 1, join_algorithm = 'direct';

SELECT t0.Id
FROM events_04206 AS t0
LEFT ANTI JOIN attributes_04206 AS t1 ON t1.EventId = t0.Id
ORDER BY t0.Id
LIMIT 5
SETTINGS enable_analyzer = 1, join_algorithm = 'direct';

DROP TABLE events_04206;
DROP TABLE attributes_04206;

-- Issue #107272: a `direct` INNER JOIN where the right MergeTree table has had a
-- column added by ALTER ... ADD COLUMN, filtering on the join key and projecting a
-- right-side column. The broken column order placed the projected String column under
-- the slot named `id` (UInt64); the downstream `WHERE r.id = 1` comparison then ran a
-- generic comparison over a String column typed as UInt64, dereferencing a null pointer
-- in ColumnString::compareAt (SIGSEGV). Projecting the added UInt64 column was the
-- non-crashing manifestation: count() returned 0 instead of 1.
DROP TABLE IF EXISTS lft_107272;
DROP TABLE IF EXISTS rgt_107272;
CREATE TABLE lft_107272 (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO lft_107272 VALUES (1), (2), (3);
CREATE TABLE rgt_107272 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO rgt_107272 VALUES (1, 'a'), (2, 'b');
ALTER TABLE rgt_107272 ADD COLUMN extra UInt64 DEFAULT 0;
INSERT INTO rgt_107272 SELECT number + 100, 'c', 0 FROM numbers(50);

-- direct join over a MergeTree right table is only supported by the new analyzer; the old
-- analyzer rejects it with NOT_IMPLEMENTED, so pin enable_analyzer = 1 like the cases above.
-- Used to crash in ColumnString::compareAt; must return 1.
SELECT count() FROM (SELECT r.value FROM lft_107272 AS l INNER JOIN rgt_107272 AS r ON l.id = r.id WHERE r.id = 1)
SETTINGS enable_analyzer = 1, join_algorithm = 'direct';
-- Used to return wrong result 0; must return 1.
SELECT count() FROM (SELECT r.extra FROM lft_107272 AS l INNER JOIN rgt_107272 AS r ON l.id = r.id WHERE r.id = 1)
SETTINGS enable_analyzer = 1, join_algorithm = 'direct';
-- Projected value must match the hash/full_sorting_merge result.
SELECT r.value FROM lft_107272 AS l INNER JOIN rgt_107272 AS r ON l.id = r.id WHERE r.id = 1
SETTINGS enable_analyzer = 1, join_algorithm = 'direct';

DROP TABLE lft_107272;
DROP TABLE rgt_107272;
