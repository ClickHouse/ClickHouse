-- Previously, due to a bug in `ConcurrentHashJoin::onBuildPhaseFinish()` we reserved much less space in `used_flags` than needed.
-- This test just checks that we won't crash.
-- Both sides are bounded explicitly: the outer `LIMIT` counts joined rows, so it is not pushed into a source of a join.
SET enable_analyzer=1;
SELECT
    number,
    number
FROM (SELECT number FROM system.numbers LIMIT 102400) AS t
ANY INNER JOIN (SELECT number FROM system.numbers LIMIT 102400) AS alias277 ON number = alias277.number
LIMIT 102400
FORMAT `Null`
SETTINGS join_algorithm = 'parallel_hash';

