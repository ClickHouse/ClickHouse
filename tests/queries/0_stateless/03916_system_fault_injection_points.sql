-- Tags: no-parallel
-- Other tests can enable failpoints and interfere with this test

-- Basic: table exists and returns rows
SELECT count() > 0 FROM system.fail_points;

-- Schema check: verify columns and types
SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'fail_points' ORDER BY position;

-- system.fail_points is process-global, so a bare count() flakes on failpoints other tests leave enabled.
-- Scope to three integration-only failpoints whose check sites no concurrent stateless query can reach,
-- so they stay disabled here and a once-enabled one cannot be consumed mid-test.
SELECT count() FROM system.fail_points
WHERE enabled = 1 AND name IN ('replicated_merge_tree_insert_retry_pause', 'delta_kernel_fail_literal_visitor', 'finish_set_quorum_failed_parts');
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
-- Its check site (DeltaLake predicate literal visit) cannot be reached by a concurrent stateless query,
-- so nothing else fires and consumes it between the SELECTs below.
SYSTEM ENABLE FAILPOINT delta_kernel_fail_literal_visitor;
SELECT 'once_first', enabled FROM system.fail_points WHERE name = 'delta_kernel_fail_literal_visitor';
SELECT 'once_second', enabled FROM system.fail_points WHERE name = 'delta_kernel_fail_literal_visitor';
SELECT 'once_third', enabled FROM system.fail_points WHERE name = 'delta_kernel_fail_literal_visitor';
SYSTEM DISABLE FAILPOINT delta_kernel_fail_literal_visitor;
SELECT 'once_disabled', enabled FROM system.fail_points WHERE name = 'delta_kernel_fail_literal_visitor';

-- PAUSEABLE_ONCE failpoint: same regression, must not be consumed by SELECT.
SYSTEM ENABLE FAILPOINT finish_set_quorum_failed_parts;
SELECT 'pauseable_once_first', enabled FROM system.fail_points WHERE name = 'finish_set_quorum_failed_parts';
SELECT 'pauseable_once_second', enabled FROM system.fail_points WHERE name = 'finish_set_quorum_failed_parts';
SYSTEM DISABLE FAILPOINT finish_set_quorum_failed_parts;
SELECT 'pauseable_once_disabled', enabled FROM system.fail_points WHERE name = 'finish_set_quorum_failed_parts';
