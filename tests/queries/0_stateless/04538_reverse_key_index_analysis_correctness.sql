-- Tags: no-random-merge-tree-settings

-- Primary key index analysis for a reverse (`DESC`) sort key column builds wrong hyperrectangles
-- when the checked mark range spans more than one value of a preceding key column: the boundary
-- of the reversed column is taken from the opposite end of the range and with the wrong direction.
-- Binary search over the primary key may then discard granules that contain matching rows.
-- The layout below triggers the row loss: `org_a` spans [2026-06-01, 2026-07-01), while `org_b`
-- (the next value in key order) only starts at 2026-06-10, above the `dt < 2026-06-05` cutoff.

DROP TABLE IF EXISTS t_reverse_key_correctness;

CREATE TABLE t_reverse_key_correctness (org String, dt DateTime, id UInt64)
ENGINE = MergeTree
ORDER BY (org, dt DESC, id)
SETTINGS index_granularity = 128;

INSERT INTO t_reverse_key_correctness SELECT 'org_a', toDateTime('2026-06-01') + intDiv(number * 2592000, 10000), number FROM numbers(10000);
INSERT INTO t_reverse_key_correctness SELECT 'org_b', toDateTime('2026-06-10') + intDiv(number * 1814400, 40000), number FROM numbers(40000);

OPTIMIZE TABLE t_reverse_key_correctness FINAL;

-- Ground truth, does not use the primary key index.
SELECT countIf(org = 'org_a' AND dt < toDateTime('2026-06-05')) FROM t_reverse_key_correctness;

-- The same predicate through primary key index analysis, with both index representations.
SELECT count() FROM t_reverse_key_correctness WHERE org = 'org_a' AND dt < toDateTime('2026-06-05') SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_reverse_key_correctness WHERE org = 'org_a' AND dt < toDateTime('2026-06-05') SETTINGS use_lightweight_primary_key_index_analysis = 0;

-- Other combinations of key value position and range direction.
SELECT count() FROM t_reverse_key_correctness WHERE org = 'org_a' AND dt > toDateTime('2026-06-25');
SELECT count() FROM t_reverse_key_correctness WHERE org = 'org_b' AND dt < toDateTime('2026-06-15');
SELECT count() FROM t_reverse_key_correctness WHERE org = 'org_b' AND dt > toDateTime('2026-06-28');
SELECT count() FROM t_reverse_key_correctness WHERE org = 'org_a';

-- Reading in order with a limit must see the same rows.
SELECT dt, id FROM t_reverse_key_correctness WHERE org = 'org_a' AND dt < toDateTime('2026-06-05') ORDER BY org, dt DESC, id LIMIT 3 SETTINGS optimize_read_in_order = 1;
SELECT dt, id FROM t_reverse_key_correctness WHERE org = 'org_b' AND dt > toDateTime('2026-06-28') ORDER BY org, dt DESC, id LIMIT 3 SETTINGS optimize_read_in_order = 1;

DROP TABLE t_reverse_key_correctness;
