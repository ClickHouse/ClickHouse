-- Verify windowFunnel with strict_deduplication returns correct max level.
-- See https://github.com/ClickHouse/ClickHouse/issues/37177

DROP TABLE IF EXISTS test_windowfunnel_strict_dedup;

CREATE TABLE test_windowfunnel_strict_dedup (time DateTime, event String)
ENGINE = MergeTree ORDER BY time;

INSERT INTO test_windowfunnel_strict_dedup VALUES
('2022-05-13 12:00:00', 'install'),
('2022-05-13 12:00:05', 'start'),
('2022-05-13 12:00:10', 'login'),
('2022-05-13 12:00:15', 'install'),
('2022-05-13 12:00:20', 'start'),
('2022-05-13 12:00:25', 'install'),
('2022-05-13 12:00:30', 'start'),
('2022-05-13 12:00:35', 'login'),
('2022-05-13 12:00:40', 'end');

-- Should return 3 (install→start→login matched before duplicate install stops the chain)
SELECT windowFunnel(60, 'strict_deduplication')(time, event = 'install', event = 'start', event = 'login', event = 'end') AS step
FROM test_windowfunnel_strict_dedup;

-- Same test with strict_once to cover the getEventLevelStrictOnce code path
SELECT windowFunnel(60, 'strict_deduplication', 'strict_once')(time, event = 'install', event = 'start', event = 'login', event = 'end') AS step
FROM test_windowfunnel_strict_dedup;

DROP TABLE test_windowfunnel_strict_dedup;
