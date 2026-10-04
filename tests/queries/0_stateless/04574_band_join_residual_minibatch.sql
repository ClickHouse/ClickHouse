-- Tags: no-old-analyzer

-- SEMI/ANTI band join with a residual ON condition: the first candidate PASSING the residual
-- decides each point row. Every fat point row's band covers all 3000 intervals (> 2 residual
-- mini-batches of candidates), and the walk visits them in descending `lo` order, so the
-- interval with the passing tag at the smallest `lo` is reached only in the last mini-batch.
-- The small `max_block_size` splits the interval side into many index blocks, so one
-- mini-batch spans several blocks.

SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,hash';
SET cross_to_inner_join_rewrite = 0;
SET max_block_size = 256;

DROP TABLE IF EXISTS mb_p;
DROP TABLE IF EXISTS mb_i;

CREATE TABLE mb_p (id UInt32, t Int64, sel Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mb_i (id UInt32, lo Int64, hi Int64, tag Nullable(Int32)) ENGINE = MergeTree ORDER BY id;

-- The residual `sel = tag` decides:
--   id 1: passes for exactly one interval (tag 42 appears once, at the smallest `lo`,
--         which the backward walk reaches last - beyond the first two mini-batches)
--   id 2: passes for many intervals
--   id 3: passes for none (no tag -5)
--   id 4: sel is NULL, the residual is NULL for every candidate
--   id 5: below every interval (no band candidates at all)
--   id 6: above every `hi` (the walk stops on the prefix-max immediately)
INSERT INTO mb_p VALUES (1, 500000, 42), (2, 500000, 7), (3, 500000, -5), (4, 500000, NULL), (5, -100, 8), (6, 5000000, 8);

INSERT INTO mb_i SELECT number + 1, toInt64(number), 1000000, if(number = 0, 42, if(number % 8 = 3, NULL, toInt32(number % 10))) FROM numbers(3000);

-- The joins must run as a band join with the equality as an in-operator residual
SELECT 'routed', countIf(explain LIKE '%BandJoin%') > 0, countIf(explain LIKE '%Residual filter%') > 0
FROM (EXPLAIN actions = 1 SELECT p.id FROM mb_p p LEFT SEMI JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag);

SELECT 'semi', p.id FROM mb_p p LEFT SEMI JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag ORDER BY ALL;
SELECT 'anti', p.id FROM mb_p p LEFT ANTI JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag ORDER BY ALL;

-- The oracle from the cross join agrees on the decided rows
SELECT 'semi vs oracle', (
    SELECT arraySort(groupArray(id)) FROM (SELECT p.id AS id FROM mb_p p LEFT SEMI JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag)
) = (
    SELECT arraySort(groupArray(id)) FROM mb_p WHERE id IN (SELECT p.id FROM mb_p p, mb_i i WHERE p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag)
);
SELECT 'anti vs oracle', (
    SELECT arraySort(groupArray(id)) FROM (SELECT p.id AS id FROM mb_p p LEFT ANTI JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag)
) = (
    SELECT arraySort(groupArray(id)) FROM mb_p WHERE id NOT IN (SELECT p.id FROM mb_p p, mb_i i WHERE p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag)
);

-- The SEMI row's interval-side companion must itself satisfy all conditions (which interval
-- row is picked is not fixed across algorithms, so project a condition check instead)
SELECT 'semi pair valid', p.id, (p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag)
FROM mb_p p LEFT SEMI JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag ORDER BY ALL;

-- Byte-parity with `ie_join` on the decided rows
SELECT 'semi parity',
    (SELECT arraySort(groupArray((p.id, p.t, p.sel))) FROM mb_p p LEFT SEMI JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag)
        = (SELECT arraySort(groupArray((p.id, p.t, p.sel))) FROM mb_p p LEFT SEMI JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag SETTINGS join_algorithm = 'ie_join');
SELECT 'anti parity',
    (SELECT arraySort(groupArray((p.id, p.t, p.sel, i.id, i.lo, i.hi, i.tag))) FROM mb_p p LEFT ANTI JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag)
        = (SELECT arraySort(groupArray((p.id, p.t, p.sel, i.id, i.lo, i.hi, i.tag))) FROM mb_p p LEFT ANTI JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag SETTINGS join_algorithm = 'ie_join');

-- LEFT with the same residual: id 1 keeps exactly its one late-passing pair, the undecided
-- rows come out padded; per-row match counts pin the mini-batch flushes emitting everything
SELECT 'left counts', p.id, countIf(i.id != 0) FROM mb_p p LEFT JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag GROUP BY p.id ORDER BY p.id;
SELECT 'left vs oracle',
    (SELECT arraySort(groupArray((p.id, p.t, p.sel, i.id, i.lo, i.hi, i.tag))) FROM mb_p p LEFT JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag)
        = (SELECT arraySort(arrayConcat(
            (SELECT groupArray((p.id, p.t, p.sel, i.id, i.lo, i.hi, i.tag)) FROM mb_p p, mb_i i WHERE p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag),
            (SELECT groupArray((id, t, sel, toUInt32(0), toInt64(0), toInt64(0), CAST(NULL, 'Nullable(Int32)'))) FROM mb_p WHERE id NOT IN (SELECT p.id FROM mb_p p, mb_i i WHERE p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag)))));
SELECT 'left parity',
    (SELECT arraySort(groupArray((p.id, p.t, p.sel, i.id, i.lo, i.hi, i.tag))) FROM mb_p p LEFT JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag)
        = (SELECT arraySort(groupArray((p.id, p.t, p.sel, i.id, i.lo, i.hi, i.tag))) FROM mb_p p LEFT JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag SETTINGS join_algorithm = 'ie_join');

-- A row cap far below the mini-batch size forces cap-triggered flushes mid-walk
SELECT 'left caps',
    (SELECT arraySort(groupArray((p.id, p.t, p.sel, i.id, i.lo, i.hi, i.tag))) FROM mb_p p LEFT JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag SETTINGS max_joined_block_size_rows = 100)
        = (SELECT arraySort(groupArray((p.id, p.t, p.sel, i.id, i.lo, i.hi, i.tag))) FROM mb_p p LEFT JOIN mb_i i ON p.t >= i.lo AND p.t <= i.hi AND p.sel = i.tag);

DROP TABLE mb_p;
DROP TABLE mb_i;
