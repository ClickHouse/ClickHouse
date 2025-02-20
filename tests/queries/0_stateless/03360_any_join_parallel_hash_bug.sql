-- Previously, due to a bug in `ConcurrentHashJoin::onBuildPhaseFinish()` we reserved much less space in `used_flags` than needed.
-- This test just checks that we won't crash.
SELECT
    number,
    number
FROM system.numbers
ANY INNER JOIN system.numbers AS alias277 ON number = alias277.number
LIMIT 1024000
FORMAT `Null`
SETTINGS join_algorithm = 'parallel_hash'
