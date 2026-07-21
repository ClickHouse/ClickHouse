-- Tags: no-random-settings
-- ANY INNER JOIN must return one row per matching key (any_join_distinct_right_table_keys = 0),
-- independent of join_algorithm. partial_merge used to keep every left row (ALL-like), giving a
-- wrong result that differed from hash/full_sorting_merge/grace_hash. See issue #111195.

SET any_join_distinct_right_table_keys = 0;

SELECT 'hash';
SELECT l.k, r.k FROM (SELECT arrayJoin([1, 1, 2]) AS k) AS l
ANY INNER JOIN (SELECT arrayJoin([1, 2, 2]) AS k) AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'hash';

SELECT 'full_sorting_merge';
SELECT l.k, r.k FROM (SELECT arrayJoin([1, 1, 2]) AS k) AS l
ANY INNER JOIN (SELECT arrayJoin([1, 2, 2]) AS k) AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge';

SELECT 'grace_hash';
SELECT l.k, r.k FROM (SELECT arrayJoin([1, 1, 2]) AS k) AS l
ANY INNER JOIN (SELECT arrayJoin([1, 2, 2]) AS k) AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'grace_hash';

-- partial_merge cannot deduplicate left keys across blocks, so it declines INNER ANY and the
-- planner falls back to a capable algorithm; the result must match the reference above.
SELECT 'prefer_partial_merge';
SELECT l.k, r.k FROM (SELECT arrayJoin([1, 1, 2]) AS k) AS l
ANY INNER JOIN (SELECT arrayJoin([1, 2, 2]) AS k) AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'prefer_partial_merge';

SELECT 'partial_merge,hash';
SELECT l.k, r.k FROM (SELECT arrayJoin([1, 1, 2]) AS k) AS l
ANY INNER JOIN (SELECT arrayJoin([1, 2, 2]) AS k) AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'partial_merge,hash';

-- Amplified: 2000000 left rows over 11 keys must collapse to 11, not multiply.
SELECT 'amplified counts (must all be 11)';
SELECT count() FROM (SELECT number % 11 AS k FROM numbers(2000000)) AS l
ANY INNER JOIN (SELECT arrayJoin(range(11)) AS k) AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash';
SELECT count() FROM (SELECT number % 11 AS k FROM numbers(2000000)) AS l
ANY INNER JOIN (SELECT arrayJoin(range(11)) AS k) AS r ON l.k = r.k
SETTINGS join_algorithm = 'prefer_partial_merge';
SELECT count() FROM (SELECT number % 11 AS k FROM numbers(2000000)) AS l
ANY INNER JOIN (SELECT arrayJoin(range(11)) AS k) AS r ON l.k = r.k
SETTINGS join_algorithm = 'full_sorting_merge';

-- Related merge-family strictness/kinds must be unaffected by the INNER ANY capability change.
SELECT 'left any (keep all left)';
SELECT l.k, r.k FROM (SELECT arrayJoin([1, 1, 2]) AS k) AS l
ANY LEFT JOIN (SELECT arrayJoin([1, 2, 2]) AS k) AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'partial_merge';

SELECT 'left semi';
SELECT l.k FROM (SELECT arrayJoin([1, 1, 2, 3]) AS k) AS l
SEMI LEFT JOIN (SELECT arrayJoin([1, 2, 2]) AS k) AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'partial_merge';

SELECT 'all inner';
SELECT l.k, r.k FROM (SELECT arrayJoin([1, 1, 2]) AS k) AS l
ALL INNER JOIN (SELECT arrayJoin([1, 2, 2]) AS k) AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'partial_merge';

-- Legacy behaviour: INNER ANY is rewritten to SEMI LEFT upstream, so partial_merge keeps all
-- left rows and this stays supported.
SELECT 'legacy inner any (all left rows)';
SELECT l.k FROM (SELECT arrayJoin([1, 1, 2]) AS k) AS l
ANY INNER JOIN (SELECT arrayJoin([1, 2, 2]) AS k) AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'partial_merge', any_join_distinct_right_table_keys = 1;
