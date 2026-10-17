-- Demonstrates that enable_alias_marker is a correctness toggle for distributed ALIAS columns.
-- Distributed-over-distributed with a String ALIAS (`a_str`) and a UInt64 ALIAS (`inner_c`):
--   * marker ON  -> columns reconciled by name, correct results.
--   * marker OFF -> the inlined ALIAS expansion swaps columns; the String 'aaaa' is routed into
--                   the UInt64 `inner_c` slot and the query fails with CANNOT_PARSE_TEXT.
DROP TABLE IF EXISTS t_se_local;
DROP TABLE IF EXISTS t_se_inner;
DROP TABLE IF EXISTS t_se_outer;

CREATE TABLE t_se_local (x UInt64) ENGINE = MergeTree() ORDER BY x;
INSERT INTO t_se_local VALUES (1), (2), (10);

CREATE TABLE t_se_inner (x UInt64, inner_c UInt64 ALIAS x + 1)
ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_se_local);

CREATE TABLE t_se_outer (x UInt64, inner_c UInt64, a_str String ALIAS 'aaaa')
ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_se_inner);

-- serialize_query_plan is pinned to 0 throughout: this test targets the AST-path alias marker.
-- On the serialized-plan path the header is reconciled by name regardless of the marker, so the
-- marker_off swap below does not occur there; the "distributed plan" CI flavor would otherwise
-- force the plan path on and the marker_off query would succeed instead of erroring.
SELECT 'marker_on';
SELECT x, a_str, inner_c
FROM t_se_outer
ORDER BY x
SETTINGS enable_analyzer = 1, enable_alias_marker = 1, prefer_localhost_replica = 0, serialize_query_plan = 0
FORMAT TSVWithNames;

SELECT 'marker_off_reintroduces_swap';
-- No output format header here: the query errors mid-execution, so it must not stream a header.
SELECT x, a_str, inner_c
FROM t_se_outer
ORDER BY x
SETTINGS enable_analyzer = 1, enable_alias_marker = 0, prefer_localhost_replica = 0, serialize_query_plan = 0; -- { serverError CANNOT_PARSE_TEXT }

DROP TABLE t_se_outer;
DROP TABLE t_se_inner;
DROP TABLE t_se_local;
