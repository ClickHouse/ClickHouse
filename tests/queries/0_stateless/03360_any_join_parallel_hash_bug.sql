-- Previously, due to a bug in `ConcurrentHashJoin::onBuildPhaseFinish()` we reserved much less space in `used_flags` than needed.
-- This test just checks that we won't crash.
SET enable_analyzer=1;
SELECT
    number,
    number
FROM system.numbers
ANY INNER JOIN system.numbers AS alias277 ON number = alias277.number
LIMIT 102400
FORMAT `Null`
SETTINGS join_algorithm = 'parallel_hash';

