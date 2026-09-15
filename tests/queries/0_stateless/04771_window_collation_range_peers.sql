-- Tags: no-fasttest
-- no-fasttest - collations support is disabled for fasttest build

SET enable_analyzer = 1;

-- Under numeric collation '1' and '01' are equal, so all four rows are one peer group.
-- The collated sort may interleave them, so assert order-independent aggregates only.
SELECT 'count', min(c), max(c) FROM (SELECT count() OVER (ORDER BY v COLLATE 'en-u-kn-true' RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c FROM (SELECT arrayJoin(['1', '01', '1', '01']) AS v));
SELECT 'rank', min(r), max(r) FROM (SELECT rank() OVER (ORDER BY v COLLATE 'en-u-kn-true') AS r FROM (SELECT arrayJoin(['1', '01', '1', '01']) AS v));
SELECT 'dense_rank', min(r), max(r) FROM (SELECT dense_rank() OVER (ORDER BY v COLLATE 'en-u-kn-true') AS r FROM (SELECT arrayJoin(['1', '01', '1', '01']) AS v));
SELECT 'nullable', min(c), max(c) FROM (SELECT count() OVER (ORDER BY v COLLATE 'en-u-kn-true' RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c FROM (SELECT toNullable(arrayJoin(['1', '01', '1', '01'])) AS v));
SELECT 'low_cardinality', min(c), max(c) FROM (SELECT count() OVER (ORDER BY v COLLATE 'en-u-kn-true' RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c FROM (SELECT toLowCardinality(arrayJoin(['1', '01', '1', '01'])) AS v));
SELECT 'multiple_keys', min(c), max(c) FROM (SELECT count() OVER (ORDER BY k, v COLLATE 'en-u-kn-true' RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c FROM (SELECT 1 AS k, arrayJoin(['1', '01', '1', '01']) AS v));
SELECT 'descending', min(c), max(c) FROM (SELECT count() OVER (ORDER BY v DESC COLLATE 'en-u-kn-true' RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c FROM (SELECT arrayJoin(['1', '01', '1', '01']) AS v));

-- One row per block, so the peer group spans blocks and the comparison against the reference row
-- reads a different block than the one being scanned.
SELECT 'cross_block', min(c), max(c) FROM (SELECT count() OVER (ORDER BY v COLLATE 'en-u-kn-true' RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c FROM (SELECT if(number % 2 = 0, '1', '01') AS v FROM numbers(4))) SETTINGS max_block_size = 1;

-- A GROUPS frame counts peer groups, so it must agree with RANGE on what a peer group is.
SELECT 'groups', min(c), max(c) FROM (SELECT count() OVER (ORDER BY v COLLATE 'en-u-kn-true' GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS c FROM (SELECT arrayJoin(['1', '01', '1', '01']) AS v));

-- Without COLLATE the values are distinct, so the peer groups are unchanged by the fix.
SELECT 'no_collation', min(c), max(c) FROM (SELECT count() OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c FROM (SELECT arrayJoin(['1', '01', '1', '01']) AS v));
