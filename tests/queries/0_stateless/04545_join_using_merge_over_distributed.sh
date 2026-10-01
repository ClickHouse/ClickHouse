#!/usr/bin/env bash
# Tags: shard

# JOIN ... USING a `Merge` table that wraps a `Distributed` table must not throw
# `LOGICAL_ERROR` "query tree node does not have valid source node" (issue #111253).
# When `Merge` builds the per-child query it removes the JOIN and strips the parts of
# WHERE/PREWHERE that reference the joined-away table. A bare-column predicate over that
# table (e.g. `WHERE r.k`) used to be left in the child query, leaving a column with a
# dangling source that raised the logical error when the `Distributed` child was re-planned.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Dedicated, uniquely-named database so the test is safe to run concurrently with itself.
DB="${CLICKHOUSE_DATABASE}_04545"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB};
CREATE DATABASE ${DB};

CREATE TABLE ${DB}.jm (k UInt32, lc String) ENGINE = MergeTree ORDER BY k;
INSERT INTO ${DB}.jm SELECT number, toString(number % 5) FROM numbers(100);

CREATE TABLE ${DB}.jm_d AS ${DB}.jm ENGINE = Distributed(test_cluster_two_shards, ${DB}, jm);
CREATE TABLE ${DB}.jm_m AS ${DB}.jm_d ENGINE = Merge(${DB}, '^jm_d\$');

CREATE TABLE ${DB}.jr (k UInt32, n Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO ${DB}.jr SELECT number, number FROM numbers(150);

SET enable_analyzer = 1;
SET distributed_product_mode = 'global';

-- Bare-column predicate over the joined-away table: the exception from the issue.
SELECT 'bare_where_r_k', l.lc, countDistinct(l.k) FROM ${DB}.jm_m AS l LEFT JOIN ${DB}.jr AS r USING (k) WHERE r.k GROUP BY l.lc ORDER BY l.lc;

-- Another bare column of the joined-away table.
SELECT 'bare_where_r_n', l.lc, countDistinct(l.k) FROM ${DB}.jm_m AS l LEFT JOIN ${DB}.jr AS r USING (k) WHERE r.n GROUP BY l.lc ORDER BY l.lc;

-- Function predicate over the joined-away table (already worked; guards against regression).
SELECT 'func_where_r_k', l.lc, countDistinct(l.k) FROM ${DB}.jm_m AS l LEFT JOIN ${DB}.jr AS r USING (k) WHERE r.k > 0 GROUP BY l.lc ORDER BY l.lc;

-- Conjunction mixing joined-away and surviving columns.
SELECT 'and_where', l.lc, countDistinct(l.k) FROM ${DB}.jm_m AS l LEFT JOIN ${DB}.jr AS r USING (k) WHERE r.k AND l.k > 0 GROUP BY l.lc ORDER BY l.lc;

-- Bare-column predicate over the surviving (Merge) side must be preserved, not dropped.
SELECT 'bare_where_l_k', l.lc, countDistinct(l.k) FROM ${DB}.jm_m AS l LEFT JOIN ${DB}.jr AS r USING (k) WHERE l.k GROUP BY l.lc ORDER BY l.lc;

-- The planner folds QUALIFY into HAVING and HAVING into WHERE, so both reach the same
-- stripping helper and both used to raise the logical error.
SELECT 'qualify_r', count(), uniqExact(k) FROM (SELECT l.k AS k FROM ${DB}.jm_m AS l LEFT JOIN ${DB}.jr AS r USING (k) QUALIFY r.k);
SELECT 'having_r', count(), uniqExact(k) FROM (SELECT l.k AS k FROM ${DB}.jm_m AS l LEFT JOIN ${DB}.jr AS r USING (k) HAVING r.k);
SELECT 'qualify_l', count(), uniqExact(k) FROM (SELECT l.k AS k FROM ${DB}.jm_m AS l LEFT JOIN ${DB}.jr AS r USING (k) QUALIFY l.k);

-- PREWHERE is stripped through a separate call site from WHERE, so it is pinned on its own.
-- Only the inner-join form is pinned: its result does not depend on whether the predicate is
-- applied before or after the join, so it must equal the WHERE form of the same predicate.
-- With an outer join, PREWHERE is applied before the join by design (#89097).
SELECT 'prewhere_r', count(), uniqExact(k) FROM (SELECT l.k AS k FROM ${DB}.jm_m AS l JOIN ${DB}.jr AS r USING (k) PREWHERE r.k);
SELECT 'prewhere_on_r', count(), uniqExact(k) FROM (SELECT l.k AS k FROM ${DB}.jm_m AS l JOIN ${DB}.jr AS r ON l.k = r.k PREWHERE r.k);
SELECT 'prewhere_l', count(), uniqExact(k) FROM (SELECT l.k AS k FROM ${DB}.jm_m AS l JOIN ${DB}.jr AS r USING (k) PREWHERE l.k);

-- Self-join of the Merge table, with ON and with USING: one table is enough to reach it.
SELECT 'self_join_on_r', count(), uniqExact(k) FROM (SELECT l.k AS k FROM ${DB}.jm_m AS l JOIN ${DB}.jm_m AS r ON l.k = r.k WHERE r.k);
SELECT 'self_join_using_r', count(), uniqExact(k) FROM (SELECT l.k AS k FROM ${DB}.jm_m AS l JOIN ${DB}.jm_m AS r USING (k) WHERE r.k);
SELECT 'self_join_on_l', count(), uniqExact(k) FROM (SELECT l.k AS k FROM ${DB}.jm_m AS l JOIN ${DB}.jm_m AS r ON l.k = r.k WHERE l.k);

DROP DATABASE ${DB};
"
