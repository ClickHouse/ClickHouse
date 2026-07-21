-- Tags: no-old-analyzer

-- The band join pre-sorts only the interval side (ascending by `lo`, NULLS LAST), which makes
-- that input eligible for the read-in-order optimization: when the interval table is a
-- MergeTree ordered by `lo`, the sort relaxes to a streaming merge over an in-order read. The
-- point side has no sort, so it must stay a default read. Results must not depend on
-- `optimize_read_in_order` or on the virtual rows the in-order readers emit.

SET join_algorithm = 'band_join,hash';

DROP TABLE IF EXISTS rio_p;
DROP TABLE IF EXISTS rio_i;

CREATE TABLE rio_p (t Int64, y Int64) ENGINE = MergeTree ORDER BY t;
CREATE TABLE rio_i (lo Int64, hi Int64) ENGINE = MergeTree ORDER BY lo;

-- Several parts on the interval side, so the in-order read produces several sorted streams
-- that have to be merged (and, with virtual rows enabled, each stream is led by one).
SYSTEM STOP MERGES rio_i;
INSERT INTO rio_p SELECT number, number % 97 FROM numbers(1500);
INSERT INTO rio_i SELECT number * 5, number * 5 + 37 FROM numbers(400);
INSERT INTO rio_i SELECT number * 7 + 3, number * 7 + 40 FROM numbers(400);
INSERT INTO rio_i SELECT number * 11 + 1, number * 11 + 90 FROM numbers(300);

SELECT 'band join used', count() > 0 FROM (
    EXPLAIN SELECT count() FROM rio_p p JOIN rio_i i ON p.t >= i.lo AND p.t <= i.hi
    SETTINGS optimize_read_in_order = 1
) WHERE explain LIKE '%BandJoin%';

-- Exactly one in-order read: the interval side; the point side keeps the default read.
SELECT 'in-order reads', countIf(explain LIKE '%Read type: InOrder%'), countIf(explain LIKE '%Read type: Default%') FROM (
    EXPLAIN actions = 1 SELECT count() FROM rio_p p JOIN rio_i i ON p.t >= i.lo AND p.t <= i.hi
    SETTINGS optimize_read_in_order = 1
);

-- The pre-sort is relaxed to finishing an already-sorted read, not a full sort.
SELECT 'sort relaxed', count() > 0 FROM (
    EXPLAIN actions = 1 SELECT count() FROM rio_p p JOIN rio_i i ON p.t >= i.lo AND p.t <= i.hi
    SETTINGS optimize_read_in_order = 1
) WHERE explain LIKE '%Prefix sort description: lo ASC%';

-- The result does not depend on how the input order was produced ...
SELECT 'read in order result', (
    SELECT (count(), sum(cityHash64(p.t, p.y, i.lo, i.hi))) FROM rio_p p JOIN rio_i i ON p.t >= i.lo AND p.t <= i.hi
    SETTINGS optimize_read_in_order = 1
) = (
    SELECT (count(), sum(cityHash64(p.t, p.y, i.lo, i.hi))) FROM rio_p p JOIN rio_i i ON p.t >= i.lo AND p.t <= i.hi
    SETTINGS optimize_read_in_order = 0
);

-- ... and matches the generic join executor.
SELECT 'oracle result', (
    SELECT (count(), sum(cityHash64(p.t, p.y, i.lo, i.hi))) FROM rio_p p JOIN rio_i i ON p.t >= i.lo AND p.t <= i.hi
    SETTINGS optimize_read_in_order = 1
) = (
    SELECT (count(), sum(cityHash64(p.t, p.y, i.lo, i.hi))) FROM rio_p p JOIN rio_i i ON p.t >= i.lo AND p.t <= i.hi
    SETTINGS join_algorithm = 'hash'
);

SELECT count() FROM rio_p p JOIN rio_i i ON p.t >= i.lo AND p.t <= i.hi;

-- Virtual rows are scheduling hints emitted by the in-order readers; the merge that finishes
-- the relaxed pre-sort consumes them and none may leak into the index as a phantom row.
SELECT 'virtual rows in pipeline', count() > 0 FROM (
    EXPLAIN PIPELINE SELECT count() FROM rio_p p JOIN rio_i i ON p.t >= i.lo AND p.t <= i.hi
    SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1
) WHERE explain LIKE '%VirtualRowTransform%';

SELECT 'virtual row result', (
    SELECT (count(), sum(cityHash64(p.t, p.y, i.lo, i.hi))) FROM rio_p p JOIN rio_i i ON p.t >= i.lo AND p.t <= i.hi
    SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1
) = (
    SELECT (count(), sum(cityHash64(p.t, p.y, i.lo, i.hi))) FROM rio_p p JOIN rio_i i ON p.t >= i.lo AND p.t <= i.hi
    SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0
);

SELECT 'virtual row per block result', (
    SELECT (count(), sum(cityHash64(p.t, p.y, i.lo, i.hi))) FROM rio_p p JOIN rio_i i ON p.t >= i.lo AND p.t <= i.hi
    SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1
) = (
    SELECT (count(), sum(cityHash64(p.t, p.y, i.lo, i.hi))) FROM rio_p p JOIN rio_i i ON p.t >= i.lo AND p.t <= i.hi
    SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0
);

DROP TABLE rio_p;
DROP TABLE rio_i;
