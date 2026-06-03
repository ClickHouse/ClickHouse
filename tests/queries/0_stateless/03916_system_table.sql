-- Tags: no-parallel
-- Other tests can enable failpoints and interfere with this test

-- Basic: table exists and returns rows
SELECT count() > 0 FROM system.fail_points;

-- Schema check: verify columns and types
SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'fail_points' ORDER BY position;

-- All failpoints are disabled by default
SELECT count() FROM system.fail_points WHERE enabled = 1;
-- Expected: 0

-- All four types are present
SELECT type, count() > 0 FROM system.fail_points GROUP BY type ORDER BY type;
-- Expected:
-- once            1
-- regular         1
-- pauseable_once  1
-- pauseable       1

-- Filtering by type works
SELECT count() > 0 FROM system.fail_points WHERE type = 'pauseable';

-- Filtering by name with LIKE
SELECT count() > 0 FROM system.fail_points WHERE name LIKE '%smt_%';

-- Verify the specific failpoint we will test is present and disabled
SELECT name, enabled FROM system.fail_points WHERE name = 'replicated_merge_tree_insert_retry_pause';
-- Expected: replicated_merge_tree_insert_retry_pause 0

-- Enable a failpoint, verify it shows as enabled
SYSTEM ENABLE FAILPOINT replicated_merge_tree_insert_retry_pause;
SELECT name, enabled FROM system.fail_points WHERE name = 'replicated_merge_tree_insert_retry_pause';
-- Expected: replicated_merge_tree_insert_retry_pause  1

-- Disable it, verify it shows as disabled again
SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_retry_pause;
SELECT name, enabled FROM system.fail_points WHERE name = 'replicated_merge_tree_insert_retry_pause';
-- Expected: replicated_merge_tree_insert_retry_pause 0

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103403
-- Querying system.fail_points must NOT consume ONCE / PAUSEABLE_ONCE failpoints.
-- Previously fillData called fiu_fail() which triggered and consumed ONETIME points.

-- ONCE failpoint: should still report enabled=1 across repeated SELECTs.
SYSTEM ENABLE FAILPOINT replicated_queue_fail_next_entry;
SELECT 'once_first', enabled FROM system.fail_points WHERE name = 'replicated_queue_fail_next_entry';
SELECT 'once_second', enabled FROM system.fail_points WHERE name = 'replicated_queue_fail_next_entry';
SELECT 'once_third', enabled FROM system.fail_points WHERE name = 'replicated_queue_fail_next_entry';
SYSTEM DISABLE FAILPOINT replicated_queue_fail_next_entry;
SELECT 'once_disabled', enabled FROM system.fail_points WHERE name = 'replicated_queue_fail_next_entry';

-- PAUSEABLE_ONCE failpoint: same regression, must not be consumed by SELECT.
SYSTEM ENABLE FAILPOINT smt_commit_tweaks_gate_open;
SELECT 'pauseable_once_first', enabled FROM system.fail_points WHERE name = 'smt_commit_tweaks_gate_open';
SELECT 'pauseable_once_second', enabled FROM system.fail_points WHERE name = 'smt_commit_tweaks_gate_open';
SYSTEM DISABLE FAILPOINT smt_commit_tweaks_gate_open;
SELECT 'pauseable_once_disabled', enabled FROM system.fail_points WHERE name = 'smt_commit_tweaks_gate_open';
