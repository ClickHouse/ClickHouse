#!/usr/bin/env bash
# Tags: no-replicated-database
# ^ creates a user and a DEFINER view; access entities are not part of the Replicated test database.

# Plan-based parallel replicas merges a UNION ALL over MergeTree branches into a single distributed plan
# fragment that executes under one context. Result-affecting transforms (here a per-branch SQL-security row
# policy) are baked as plan steps during each branch's own analysis before the split is inserted, so the
# shared execution context must not change the rows returned. Checked with parallel_replicas_local_plan both
# off (remote plan fragment shipped/deserialized by name) and on (local plan fragment, with the failpoint
# slowing its read so the remote replicas emit rows first). Regression test for PR #111063 review.
#
# The definer user is named per test database because users are global objects (the flaky check runs the
# test concurrently); the row policy sits on the definer's own table so no user reads a foreign-policy table
# (would be denied under throw_on_unmatched_row_policies).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --multiquery --query "
DROP USER IF EXISTS ${user};
CREATE USER ${user};
GRANT SELECT ON *.* TO ${user};

CREATE TABLE ub1 (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE ub2 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO ub1 SELECT number FROM numbers(1_000_000);
INSERT INTO ub2 SELECT number + 1_000_000 FROM numbers(1_000_000);

-- Row policy on ub1 for the definer only (a < 500000). It applies when ub1 is read as ${user} (the
-- definer view below); it never denies anyone because ub1 is only read through that view.
CREATE ROW POLICY rp ON ub1 FOR SELECT USING a < 500000 TO ${user};

-- dv1: DEFINER = ${user} over ub1 -> its inner read runs as ${user}, so the policy filters ub1 to a<500000.
CREATE VIEW dv1 DEFINER = ${user} SQL SECURITY DEFINER AS SELECT a FROM ub1;
-- iv2: INVOKER over ub2 (no policy) -> returns all of ub2.
CREATE VIEW iv2 SQL SECURITY INVOKER AS SELECT a FROM ub2;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- Expect ub1 filtered by the definer's policy (500000 rows: 0..499999) plus all of ub2 (1000000 rows).
-- If the merged plan fragment dropped the definer's context/policy for the ub1 branch, ub1 would return all
-- rows and the count/sum would grow. Checked for both plan fragment execution paths.
SELECT count(), sum(a) FROM (SELECT a FROM dv1 UNION ALL SELECT a FROM iv2)
SETTINGS parallel_replicas_local_plan = 0;

SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;
SELECT count(), sum(a) FROM (SELECT a FROM dv1 UNION ALL SELECT a FROM iv2)
SETTINGS parallel_replicas_local_plan = 1;
SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- The union ships as one distributed plan fragment (single shared context), no leftover split.
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%ParallelReplicasSplit%') AS splits
FROM (EXPLAIN optimize = 1, description = 0
    SELECT a FROM (SELECT a FROM dv1 UNION ALL SELECT a FROM iv2));

-- Drop the definer view before the user (a user cannot be dropped while it is a view's definer).
DROP VIEW dv1;
DROP VIEW iv2;
DROP USER ${user};
"
