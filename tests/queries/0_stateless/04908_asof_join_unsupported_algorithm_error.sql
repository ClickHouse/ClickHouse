-- ASOF JOIN is only supported by the `hash` and `full_sorting_merge` algorithms.
-- Requesting any other algorithm must fail rather than silently falling back.

SELECT *
FROM (SELECT 1 AS key, 2 AS t) AS l
ASOF JOIN (SELECT 1 AS key, 1 AS t) AS r
ON l.key = r.key AND l.t >= r.t
SETTINGS join_algorithm = 'grace_hash'; -- { serverError NOT_IMPLEMENTED }

SELECT *
FROM (SELECT 1 AS key, 2 AS t) AS l
ASOF JOIN (SELECT 1 AS key, 1 AS t) AS r
ON l.key = r.key AND l.t >= r.t
SETTINGS join_algorithm = 'partial_merge'; -- { serverError NOT_IMPLEMENTED }

-- The supported algorithms still work.

SELECT *
FROM (SELECT 1 AS key, 2 AS t) AS l
ASOF JOIN (SELECT 1 AS key, 1 AS t) AS r
ON l.key = r.key AND l.t >= r.t
SETTINGS join_algorithm = 'hash';

SELECT *
FROM (SELECT 1 AS key, 2 AS t) AS l
ASOF JOIN (SELECT 1 AS key, 1 AS t) AS r
ON l.key = r.key AND l.t >= r.t
SETTINGS join_algorithm = 'full_sorting_merge';
