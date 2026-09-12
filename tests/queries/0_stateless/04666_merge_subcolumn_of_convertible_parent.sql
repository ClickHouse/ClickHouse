-- Tags: no-fasttest, use-rocksdb, long
-- long: one run of this file goes past the flaky check's 180s per-run budget under ASan
-- with S3 storage and metadata in Keeper, where every statement pays an object-storage
-- round trip. Untagged, that budget fails the check outright rather than reporting a flake.
-- A subcolumn read through a `Merge` table must agree with the parent column that same table
-- returns for the row. `Buffer` and `StorageView` carry the same wrapper defect and are
-- deliberately NOT covered here: each needs a different mechanism and ships separately.

-- Pin the analyzer so `compatibility` randomization cannot flip the test into the old one, which
-- rejects the subcolumn outright; the arms near the end of the file assert that rejection.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_merge_sub_good;
DROP TABLE IF EXISTS t_merge_sub_str;
DROP TABLE IF EXISTS t_merge_sub;

CREATE TABLE t_merge_sub_good (arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_merge_sub_str  (arr String)       ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_good VALUES ([1, 2]), ([]), ([3]);
INSERT INTO t_merge_sub_str  VALUES ('[4,5,6]');
CREATE TABLE t_merge_sub (arr Array(UInt8))
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_(good|str)$');

SELECT 'size0 agrees with length, subcolumns off';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'size0 agrees with length, subcolumns on';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub SETTINGS optimize_functions_to_subcolumns = 1;

-- The per-row form is the strongest oracle: an aggregate can be right in total while
-- individual rows disagree.
SELECT 'no row where size0 disagrees with length';
SELECT count() FROM t_merge_sub WHERE arr.size0 != length(arr) SETTINGS optimize_functions_to_subcolumns = 0;

-- The parent is never mentioned, so the header the child output is converted to contains only
-- `arr.size0 UInt64`. Conversion matches positionally, so a fix that relies on the downstream
-- converting actions instead of casting inside the derivation fails exactly here.
SELECT 'subcolumn read with the parent never mentioned';
SELECT sum(arr.size0) FROM t_merge_sub SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_merge_sub_nest_good;
DROP TABLE IF EXISTS t_merge_sub_nest_str;
DROP TABLE IF EXISTS t_merge_sub_nest;

CREATE TABLE t_merge_sub_nest_good (arr Array(Array(UInt8))) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_merge_sub_nest_str  (arr String)              ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_nest_good VALUES ([[1], [2]]);
INSERT INTO t_merge_sub_nest_str  VALUES ('[[1,2],[3],[4]]');
CREATE TABLE t_merge_sub_nest (arr Array(Array(UInt8)))
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_nest_(good|str)$');

SELECT 'nested array parent';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub_nest SETTINGS optimize_functions_to_subcolumns = 0;

-- Two identically-structured mistyped children: the second hits the per-structure query-info
-- cache, which restores the derivation rather than recomputing it. If the derivation lived only
-- on the cache-miss path, the twins would disagree and the total would depend on table order.
DROP TABLE IF EXISTS t_merge_sub_twin_good;
DROP TABLE IF EXISTS t_merge_sub_twin_a;
DROP TABLE IF EXISTS t_merge_sub_twin_b;
DROP TABLE IF EXISTS t_merge_sub_twin;

CREATE TABLE t_merge_sub_twin_good (arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_merge_sub_twin_a    (arr String)       ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_merge_sub_twin_b    (arr String)       ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_twin_good VALUES ([1, 2]);
INSERT INTO t_merge_sub_twin_a    VALUES ('[1,2,3]');
INSERT INTO t_merge_sub_twin_b    VALUES ('[4,5,6]');
CREATE TABLE t_merge_sub_twin (arr Array(UInt8))
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_twin_(good|a|b)$');

SELECT 'identically-structured mistyped children agree';
SELECT _table, sum(arr.size0) FROM t_merge_sub_twin
GROUP BY _table ORDER BY _table SETTINGS optimize_functions_to_subcolumns = 0;

-- A Merge over a Merge raises the child processing stage above FetchColumns, where the child
-- executes the query itself and no alias can be materialized against its output. The resolved
-- replacement in the query tree has to carry the derivation on its own there.
DROP TABLE IF EXISTS t_merge_sub_outer;
CREATE TABLE t_merge_sub_outer (arr Array(UInt8))
    ENGINE = Merge(currentDatabase(), '^t_merge_sub$');

SELECT 'merge over merge, subcolumns off';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub_outer SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'merge over merge, subcolumns on';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub_outer SETTINGS optimize_functions_to_subcolumns = 1;

-- A child carrying an `ALIAS` column has its read list replaced by its alias DAG's requirements,
-- which cannot name a subcolumn the child does not resolve. The derivation must therefore be
-- decided from the names requested of the `Merge` table, not from that rewritten list.
DROP TABLE IF EXISTS t_merge_sub_al_good;
DROP TABLE IF EXISTS t_merge_sub_al_str;
DROP TABLE IF EXISTS t_merge_sub_al;
DROP TABLE IF EXISTS t_merge_sub_al_outer;

-- `tiny` keeps the parent out of the smallest-physical-column fallback, which would otherwise
-- pick the parent itself and make the read succeed by accident.
CREATE TABLE t_merge_sub_al_good (arr Array(UInt8), tiny UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_merge_sub_al_str  (arr String, tiny UInt8, a_len UInt64 ALIAS length(arr))
    ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_al_good VALUES ([1, 2], 1), ([], 1), ([3], 1);
INSERT INTO t_merge_sub_al_str  VALUES ('[4,5,6]', 1);
CREATE TABLE t_merge_sub_al (arr Array(UInt8), tiny UInt8)
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_al_(good|str)$');

SELECT 'alias-bearing child, parent never mentioned, subcolumns off';
SELECT sum(arr.size0) FROM t_merge_sub_al SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'alias-bearing child, parent never mentioned, subcolumns on';
SELECT sum(arr.size0) FROM t_merge_sub_al SETTINGS optimize_functions_to_subcolumns = 1;

-- Control: when the parent is also requested it enters the alias DAG on its own, so this form
-- works even without the fix. The asymmetry between the two is what makes the cell above precise.
SELECT 'alias-bearing child, parent also requested';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub_al SETTINGS optimize_functions_to_subcolumns = 0;

CREATE TABLE t_merge_sub_al_outer (arr Array(UInt8), tiny UInt8)
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_al$');

SELECT 'alias-bearing child through a nested merge';
SELECT sum(arr.size0) FROM t_merge_sub_al_outer SETTINGS optimize_functions_to_subcolumns = 0;

-- The parent has to be in the child's physical read list: an `EPHEMERAL` parent has no data and an
-- `ALIAS` one is reachable only through alias expansion. `MATERIALIZED` and `DEFAULT` are the
-- controls. The `ALIAS` rows pin pre-existing behaviour, not a correct result -- see the note below.
DROP TABLE IF EXISTS t_merge_sub_unread_good;
DROP TABLE IF EXISTS t_merge_sub_eph;
DROP TABLE IF EXISTS t_merge_sub_m_eph;
DROP TABLE IF EXISTS t_merge_sub_alp;
DROP TABLE IF EXISTS t_merge_sub_m_alp;
DROP TABLE IF EXISTS t_merge_sub_alpc;
DROP TABLE IF EXISTS t_merge_sub_m_alpc;
DROP TABLE IF EXISTS t_merge_sub_mat;
DROP TABLE IF EXISTS t_merge_sub_m_mat;
DROP TABLE IF EXISTS t_merge_sub_def;
DROP TABLE IF EXISTS t_merge_sub_m_def;
DROP TABLE IF EXISTS t_merge_sub_defa;
DROP TABLE IF EXISTS t_merge_sub_m_defa;

CREATE TABLE t_merge_sub_unread_good (arr Array(UInt8), tiny UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_unread_good VALUES ([1, 2, 3], 0);

-- `tiny` again keeps the parent out of the smallest-physical-column fallback.
CREATE TABLE t_merge_sub_eph (n UInt64, tiny UInt8, arr String EPHEMERAL) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_eph (n, tiny) VALUES (7, 0);
CREATE TABLE t_merge_sub_m_eph (arr Array(UInt8), tiny UInt8)
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_(unread_good|eph)$');

SELECT 'ephemeral parent keeps the default, subcolumns off';
SELECT sum(arr.size0) FROM t_merge_sub_m_eph SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'ephemeral parent keeps the default, subcolumns on';
SELECT sum(arr.size0) FROM t_merge_sub_m_eph SETTINGS optimize_functions_to_subcolumns = 1;

CREATE TABLE t_merge_sub_alp (n UInt64, tiny UInt8, arr String ALIAS concat('[', toString(n), ']'))
    ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_alp (n, tiny) VALUES (7, 0);
CREATE TABLE t_merge_sub_m_alp (arr Array(UInt8), tiny UInt8)
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_(unread_good|alp)$');

SELECT 'alias parent keeps the default, subcolumns off';
SELECT sum(arr.size0) FROM t_merge_sub_m_alp SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'alias parent keeps the default, subcolumns on';
SELECT sum(arr.size0) FROM t_merge_sub_m_alp SETTINGS optimize_functions_to_subcolumns = 1;

-- A constant alias expression needs no input, so it is asserted separately. The `0` it yields
-- contradicts the parent in the same row and is a pre-existing `Merge`-side defect, unrelated to the
-- conversion this file covers; pinned here only so a change in it is noticed.
CREATE TABLE t_merge_sub_alpc (n UInt64, tiny UInt8, arr String ALIAS '[4,5,6]')
    ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_alpc (n, tiny) VALUES (7, 0);
CREATE TABLE t_merge_sub_m_alpc (arr Array(UInt8), tiny UInt8)
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_(unread_good|alpc)$');

SELECT 'constant alias parent keeps the default too';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub_m_alpc SETTINGS optimize_functions_to_subcolumns = 0;

CREATE TABLE t_merge_sub_mat (n UInt64, tiny UInt8, arr String MATERIALIZED concat('[', toString(n), ']'))
    ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_mat (n, tiny) VALUES (7, 0);
CREATE TABLE t_merge_sub_m_mat (arr Array(UInt8), tiny UInt8)
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_(unread_good|mat)$');

-- This arm proves the guard narrows by readability, so it must show the derived subcolumn AGREEING
-- with the parent rather than a total that happens to match. It is the only arm that can carry the
-- pair: an unreadable parent makes the parent term itself throw.
SELECT 'materialized parent is still derived, subcolumns off';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub_m_mat SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'materialized parent is still derived, subcolumns on';
SELECT sum(arr.size0) FROM t_merge_sub_m_mat SETTINGS optimize_functions_to_subcolumns = 1;

-- `DEFAULT` is the other non-stored physical kind, and it comes in two read paths: the value is
-- stored when the row was inserted under the column, and evaluated at read time when `ALTER ADD`
-- left the part without it. Both must derive, so both carry the parent term to assert agreement.
CREATE TABLE t_merge_sub_def (s String, tiny UInt8, arr String DEFAULT s)
    ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_def (s, tiny) VALUES ('[7,8,9,10]', 0);
CREATE TABLE t_merge_sub_m_def (arr Array(UInt8), tiny UInt8)
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_(unread_good|def)$');

SELECT 'default parent is still derived, subcolumns off';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub_m_def SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'default parent is still derived, subcolumns on';
SELECT sum(arr.size0) FROM t_merge_sub_m_def SETTINGS optimize_functions_to_subcolumns = 1;

CREATE TABLE t_merge_sub_defa (s String, tiny UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_defa (s, tiny) VALUES ('[7,8,9,10]', 0);
ALTER TABLE t_merge_sub_defa ADD COLUMN arr String DEFAULT s;
CREATE TABLE t_merge_sub_m_defa (arr Array(UInt8), tiny UInt8)
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_(unread_good|defa)$');

SELECT 'read-time default parent is still derived, subcolumns off';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub_m_defa SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'read-time default parent is still derived, subcolumns on';
SELECT sum(arr.size0) FROM t_merge_sub_m_defa SETTINGS optimize_functions_to_subcolumns = 1;

-- A child whose engine reports `supportsSubcolumns() = false` must derive too: the parent is read
-- and the subcolumn comes off the converted value, so the requested name never reaches that reader.
-- `StripeLog` is that case; `TinyLog` is a `StorageLog` sibling control, where the capability is true.
DROP TABLE IF EXISTS t_merge_sub_nosub_good;
DROP TABLE IF EXISTS t_merge_sub_nosub_sl;
DROP TABLE IF EXISTS t_merge_sub_nosub_tl;
DROP TABLE IF EXISTS t_merge_sub_m_nosub_sl;
DROP TABLE IF EXISTS t_merge_sub_m_nosub_tl;

CREATE TABLE t_merge_sub_nosub_good (arr Array(UInt8), tiny UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_nosub_good VALUES ([1, 2, 3], 0);
CREATE TABLE t_merge_sub_nosub_sl (arr String, tiny UInt8) ENGINE = StripeLog;
INSERT INTO t_merge_sub_nosub_sl VALUES ('[9, 9]', 0);
CREATE TABLE t_merge_sub_nosub_tl (arr String, tiny UInt8) ENGINE = TinyLog;
INSERT INTO t_merge_sub_nosub_tl VALUES ('[9, 9]', 0);
CREATE TABLE t_merge_sub_m_nosub_sl (arr Array(UInt8), tiny UInt8)
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_nosub_(good|sl)$');
CREATE TABLE t_merge_sub_m_nosub_tl (arr Array(UInt8), tiny UInt8)
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_nosub_(good|tl)$');

SELECT 'subcolumn-less child engine still derives, StripeLog';
SELECT arr.size0, length(arr) FROM t_merge_sub_m_nosub_sl
ORDER BY _table SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'capable sibling control, TinyLog';
SELECT arr.size0, length(arr) FROM t_merge_sub_m_nosub_tl
ORDER BY _table SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE t_merge_sub_m_nosub_tl;
DROP TABLE t_merge_sub_m_nosub_sl;
DROP TABLE t_merge_sub_nosub_tl;
DROP TABLE t_merge_sub_nosub_sl;
DROP TABLE t_merge_sub_nosub_good;

-- The derived cast throws on data it cannot decode. Asserted bare once here because every other
-- unconvertible value in this test is hidden by a row policy: it is a deliberate change from
-- master's silent type default.
DROP TABLE IF EXISTS t_merge_sub_bad_good;
DROP TABLE IF EXISTS t_merge_sub_bad_str;
DROP TABLE IF EXISTS t_merge_sub_bad;

CREATE TABLE t_merge_sub_bad_good (arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_merge_sub_bad_str  (arr String)       ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_bad_good VALUES ([1, 2]);
INSERT INTO t_merge_sub_bad_str  VALUES ('not-an-array');
CREATE TABLE t_merge_sub_bad (arr Array(UInt8))
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_bad_(good|str)$');

SELECT 'an unconvertible parent value throws instead of defaulting';
SELECT sum(arr.size0) FROM t_merge_sub_bad
SETTINGS optimize_functions_to_subcolumns = 0; -- { serverError CANNOT_READ_ARRAY_FROM_TEXT }

-- The derivation casts the parent, so it must run after the child's row policy filter: the hidden
-- row here holds a value that cannot convert at all. `query_plan_merge_expressions = 0` is required:
-- with the merge on, the hidden row's cast is not evaluated, so the ordering is unobservable.
DROP TABLE IF EXISTS t_merge_sub_rp_good;
DROP TABLE IF EXISTS t_merge_sub_rp_str;
DROP TABLE IF EXISTS t_merge_sub_rp;

CREATE TABLE t_merge_sub_rp_good (arr Array(UInt8), ok UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_merge_sub_rp_str  (arr String, ok UInt8)       ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_rp_good VALUES ([1, 2], 1);
INSERT INTO t_merge_sub_rp_str  VALUES ('[4,5,6]', 1), ('not-an-array', 0);
CREATE TABLE t_merge_sub_rp (arr Array(UInt8), ok UInt8)
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_rp_(good|str)$');
CREATE ROW POLICY t_merge_sub_rp_p ON t_merge_sub_rp_str USING ok = 1 AS PERMISSIVE TO ALL;

SELECT 'row policy hides an unconvertible parent value, subcolumns off';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub_rp
SETTINGS optimize_functions_to_subcolumns = 0, query_plan_merge_expressions = 0;

SELECT 'row policy hides an unconvertible parent value, subcolumns on';
SELECT sum(arr.size0) FROM t_merge_sub_rp
SETTINGS optimize_functions_to_subcolumns = 1, query_plan_merge_expressions = 0;

-- The same query without the subcolumn already worked, and must keep the same answer: it is the
-- reference the two rows above are asserted against.
SELECT 'and the parent-only form agrees';
SELECT sum(length(arr)) FROM t_merge_sub_rp
SETTINGS optimize_functions_to_subcolumns = 0, query_plan_merge_expressions = 0;

-- `allow_reorder_prewhere_conditions` must stay off: it can place the policy conjunct after the
-- cast, which then runs on the row the policy hides. The parent form `WHERE length(arr) = 3`
-- throws the same way, so that ordering is not what this fix covers.
SELECT 'row policy plus a WHERE on the derived subcolumn';
SELECT count() FROM t_merge_sub_rp WHERE arr.size0 = 3
SETTINGS query_plan_merge_expressions = 0, allow_reorder_prewhere_conditions = 0;
SELECT count() FROM t_merge_sub_rp WHERE arr.size0 = 3
SETTINGS query_plan_merge_expressions = 1, allow_reorder_prewhere_conditions = 0;
SELECT count() FROM t_merge_sub_rp WHERE arr.size0 = 3
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, allow_reorder_prewhere_conditions = 0;

DROP ROW POLICY t_merge_sub_rp_p ON t_merge_sub_rp_str;

-- Must not regress: a child row policy may legitimately reference a child `ALIAS` column, because
-- the policy is analyzed against all of the child's columns. So the child's own aliases have to
-- stay before the filter even though the derived one moves after it.
DROP TABLE IF EXISTS t_merge_sub_rpa_good;
DROP TABLE IF EXISTS t_merge_sub_rpa_str;
DROP TABLE IF EXISTS t_merge_sub_rpa;

CREATE TABLE t_merge_sub_rpa_good (arr Array(UInt8), n UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_merge_sub_rpa_str  (arr String, n UInt64, big UInt8 ALIAS n > 10)
    ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_rpa_good VALUES ([1, 2], 99);
INSERT INTO t_merge_sub_rpa_str  VALUES ('[4,5,6]', 20), ('not-an-array', 1);
CREATE TABLE t_merge_sub_rpa (arr Array(UInt8), n UInt64)
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_rpa_(good|str)$');
CREATE ROW POLICY t_merge_sub_rpa_p ON t_merge_sub_rpa_str USING big AS PERMISSIVE TO ALL;

SELECT 'row policy on a child ALIAS column';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub_rpa
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'row policy on a child ALIAS column, parent never mentioned';
SELECT sum(arr.size0) FROM t_merge_sub_rpa SETTINGS optimize_functions_to_subcolumns = 0;

DROP ROW POLICY t_merge_sub_rpa_p ON t_merge_sub_rpa_str;

-- A `DEFAULT` parent reaches the derivation through `evaluateMissingDefaults` rather than a stored
-- column, so the ordering against the child's filter is asserted for it too. The bare form first,
-- to keep the policy arm below from passing on a value that converts.
DROP TABLE IF EXISTS t_merge_sub_rpd_good;
DROP TABLE IF EXISTS t_merge_sub_rpd_str;
DROP TABLE IF EXISTS t_merge_sub_rpd;

CREATE TABLE t_merge_sub_rpd_good (arr Array(UInt8), ok UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_merge_sub_rpd_str  (s String, ok UInt8)         ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_rpd_good VALUES ([1, 2], 1);
INSERT INTO t_merge_sub_rpd_str  VALUES ('[4,5,6]', 1), ('not-an-array', 0);
ALTER TABLE t_merge_sub_rpd_str ADD COLUMN arr String DEFAULT s;
CREATE TABLE t_merge_sub_rpd (arr Array(UInt8), ok UInt8)
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_rpd_(good|str)$');

SELECT 'an unconvertible default parent value throws without the policy';
SELECT sum(arr.size0) FROM t_merge_sub_rpd
SETTINGS optimize_functions_to_subcolumns = 0, query_plan_merge_expressions = 0; -- { serverError CANNOT_READ_ARRAY_FROM_TEXT }

CREATE ROW POLICY t_merge_sub_rpd_p ON t_merge_sub_rpd_str USING ok = 1 AS PERMISSIVE TO ALL;

SELECT 'row policy hides an unconvertible default parent value, subcolumns off';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub_rpd
SETTINGS optimize_functions_to_subcolumns = 0, query_plan_merge_expressions = 0;

SELECT 'row policy hides an unconvertible default parent value, subcolumns on';
SELECT sum(arr.size0) FROM t_merge_sub_rpd
SETTINGS optimize_functions_to_subcolumns = 1, query_plan_merge_expressions = 0;

DROP ROW POLICY t_merge_sub_rpd_p ON t_merge_sub_rpd_str;

-- A `Distributed` child reaches the row policy on the shard, not on the initiator: the shard is
-- sent the derived expression as text and applies its own policy to the local table. The policy
-- must still hide the unconvertible row, so these agree with the local-child arms above.
-- `prefer_localhost_replica = 0` is what puts the child behind `ReadFromRemote`; at its default a
-- local shard is read in process and these arms would repeat the local-child ones above.
DROP TABLE IF EXISTS t_merge_sub_rpr_good;
DROP TABLE IF EXISTS t_merge_sub_rpr_str;
DROP TABLE IF EXISTS t_merge_sub_rpr_dist;
DROP TABLE IF EXISTS t_merge_sub_rpr;

CREATE TABLE t_merge_sub_rpr_good (arr Array(UInt8), ok UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_merge_sub_rpr_str  (arr String, ok UInt8)       ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_rpr_good VALUES ([1, 2], 1);
INSERT INTO t_merge_sub_rpr_str  VALUES ('[4,5,6]', 1), ('not-an-array', 0);
CREATE TABLE t_merge_sub_rpr_dist (arr String, ok UInt8)
    ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_merge_sub_rpr_str');
CREATE TABLE t_merge_sub_rpr (arr Array(UInt8), ok UInt8)
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_rpr_(good|dist)$');

SELECT 'the distributed child is planned as a remote read';
SELECT count() > 0 FROM (
    EXPLAIN SELECT sum(arr.size0) FROM t_merge_sub_rpr
    SETTINGS query_plan_merge_expressions = 0, prefer_localhost_replica = 0
) WHERE explain ILIKE '%ReadFromRemote%';

SELECT 'an unconvertible parent value behind a distributed child throws without the policy';
SELECT sum(arr.size0) FROM t_merge_sub_rpr
SETTINGS query_plan_merge_expressions = 0, prefer_localhost_replica = 0; -- { serverError CANNOT_READ_ARRAY_FROM_TEXT }

CREATE ROW POLICY t_merge_sub_rpr_p ON t_merge_sub_rpr_str USING ok = 1 AS PERMISSIVE TO ALL;

-- A shard applies its own policy only when it plans the read itself, so these two arms pin
-- `serialize_query_plan = 0`: a shipped plan misses a policy defined only on the shard (#112891).
SELECT 'row policy on a distributed child hides an unconvertible parent value';
SELECT sum(arr.size0) FROM t_merge_sub_rpr
SETTINGS query_plan_merge_expressions = 0, prefer_localhost_replica = 0, serialize_query_plan = 0;

SELECT 'row policy on a distributed child plus a WHERE on the derived subcolumn';
SELECT count() FROM t_merge_sub_rpr WHERE arr.size0 = 3
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, allow_reorder_prewhere_conditions = 0, prefer_localhost_replica = 0, serialize_query_plan = 0;

DROP ROW POLICY t_merge_sub_rpr_p ON t_merge_sub_rpr_str;

-- Must not regress: child parent types that already expose the subcolumn keep working and must
-- not be routed through the derivation. These are the common real-world schema evolutions.
DROP TABLE IF EXISTS t_merge_sub_u64;
DROP TABLE IF EXISTS t_merge_sub_m_u64;
CREATE TABLE t_merge_sub_u64 (arr Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_u64 VALUES ([9, 9, 9]);
CREATE TABLE t_merge_sub_m_u64 (arr Array(UInt8))
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_(good|u64)$');

SELECT 'wider element type child';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub_m_u64 SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_merge_sub_nullelem;
DROP TABLE IF EXISTS t_merge_sub_m_nullelem;
CREATE TABLE t_merge_sub_nullelem (arr Array(Nullable(UInt8))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_nullelem VALUES ([1, 2, 3]);
CREATE TABLE t_merge_sub_m_nullelem (arr Array(UInt8))
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_(good|nullelem)$');

SELECT 'nullable element type child';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub_m_nullelem SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_merge_sub_map;
DROP TABLE IF EXISTS t_merge_sub_m_map;
CREATE TABLE t_merge_sub_map (arr Map(String, UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_map VALUES (map('a', 1, 'b', 2));
CREATE TABLE t_merge_sub_m_map (arr Map(String, UInt8))
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_map$');

SELECT 'map parent';
SELECT sum(arr.size0), sum(length(arr)) FROM t_merge_sub_m_map SETTINGS optimize_functions_to_subcolumns = 0;

-- Must not regress: a child missing the column entirely still gets the default value. This is
-- the case the default branch exists for, and narrowing it too far would break Merge over
-- tables with different column sets.
DROP TABLE IF EXISTS t_merge_sub_nocol;
DROP TABLE IF EXISTS t_merge_sub_m_nocol;
CREATE TABLE t_merge_sub_nocol (other UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_nocol VALUES (7);
CREATE TABLE t_merge_sub_m_nocol (arr Array(UInt8))
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_(good|nocol)$');

SELECT 'child missing the column keeps its default';
SELECT sum(arr.size0), sum(length(arr)), count() FROM t_merge_sub_m_nocol SETTINGS optimize_functions_to_subcolumns = 0;

-- Must not regress: the `.null` subcolumn travels the same substitution.
DROP TABLE IF EXISTS t_merge_sub_notnull;
DROP TABLE IF EXISTS t_merge_sub_isnull;
DROP TABLE IF EXISTS t_merge_sub_m_null;
CREATE TABLE t_merge_sub_notnull (c UInt8)           ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_merge_sub_isnull  (c Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_notnull VALUES (1), (2);
INSERT INTO t_merge_sub_isnull  VALUES (NULL), (5);
CREATE TABLE t_merge_sub_m_null (c Nullable(UInt8))
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_(notnull|isnull)$');

SELECT 'null subcolumn agrees with IS NULL';
SELECT sum(c.null), countIf(c IS NULL) FROM t_merge_sub_m_null SETTINGS optimize_functions_to_subcolumns = 0;

-- The fold must still happen for a Merge whose children all expose the subcolumn: it is what
-- keeps this read under the byte cap. Without it the same query needs more than the cap.
DROP TABLE IF EXISTS t_merge_sub_hom_a;
DROP TABLE IF EXISTS t_merge_sub_hom_b;
DROP TABLE IF EXISTS t_merge_sub_hom;
CREATE TABLE t_merge_sub_hom_a (arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_merge_sub_hom_b (arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_hom_a SELECT range(number % 50) FROM numbers(100000);
INSERT INTO t_merge_sub_hom_b SELECT range(number % 50) FROM numbers(100000);
CREATE TABLE t_merge_sub_hom (arr Array(UInt8))
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_hom_(a|b)$');

SELECT 'homogeneous merge still folds to the offsets subcolumn';
SELECT sum(length(arr)) FROM t_merge_sub_hom
SETTINGS optimize_functions_to_subcolumns = 1, max_bytes_to_read = 4000000;

SELECT 'and needs more than the cap without the fold';
SELECT sum(length(arr)) FROM t_merge_sub_hom
SETTINGS optimize_functions_to_subcolumns = 0, max_bytes_to_read = 4000000; -- { serverError TOO_MANY_BYTES }

SELECT 'and the plan still reads the offsets subcolumn';
SELECT countIf(explain LIKE '%size0%')
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT sum(length(arr)) FROM t_merge_sub_hom)
SETTINGS optimize_functions_to_subcolumns = 1;

-- The old analyzer cannot express the request at all: it resolves subcolumns against the child's
-- own type, so a child that declares the parent differently has no `arr.size0` to read. That is
-- why the `SET enable_analyzer = 1` above is a pin and not a preference.
SELECT 'old analyzer rejects the subcolumn instead of returning a wrong value';
SELECT sum(arr.size0) FROM t_merge_sub
SETTINGS enable_analyzer = 0, optimize_functions_to_subcolumns = 0; -- { serverError UNKNOWN_IDENTIFIER }

-- The parent itself still reads, so the rejection above is specific to the subcolumn name.
SELECT 'old analyzer still reads the converted parent';
SELECT sum(length(arr)) FROM t_merge_sub
SETTINGS enable_analyzer = 0, optimize_functions_to_subcolumns = 0;

-- And the implicit `length` -> `size0` rewrite does not reach the child there either, so the
-- wrong-result contract this test guards has no old-analyzer counterpart to fix.
SELECT 'old analyzer keeps the implicit rewrite correct';
SELECT sum(length(arr)) FROM t_merge_sub
SETTINGS enable_analyzer = 0, optimize_functions_to_subcolumns = 1;

-- A View over a mistyped target raises UNKNOWN_IDENTIFIER for the hand-written subcolumn form
-- both before and after this change: that carrier is handled separately.
DROP TABLE IF EXISTS t_merge_sub_view_str;
DROP TABLE IF EXISTS t_merge_sub_view;
CREATE TABLE t_merge_sub_view_str (arr String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_merge_sub_view_str VALUES ('[1,2,3,4,5,6]');
CREATE VIEW t_merge_sub_view (arr Array(UInt8)) AS SELECT arr FROM t_merge_sub_view_str;

SELECT 'view over a mistyped target is unaffected';
SELECT sum(arr.size0) FROM t_merge_sub_view
SETTINGS optimize_functions_to_subcolumns = 0; -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE t_merge_sub_view;
DROP TABLE t_merge_sub_view_str;
DROP TABLE t_merge_sub_bad;
DROP TABLE t_merge_sub_bad_str;
DROP TABLE t_merge_sub_bad_good;
DROP TABLE t_merge_sub_rpd;
DROP TABLE t_merge_sub_rpd_str;
DROP TABLE t_merge_sub_rpd_good;
DROP TABLE t_merge_sub_m_defa;
DROP TABLE t_merge_sub_defa;
DROP TABLE t_merge_sub_m_def;
DROP TABLE t_merge_sub_def;
DROP TABLE t_merge_sub_m_mat;
DROP TABLE t_merge_sub_mat;
DROP TABLE t_merge_sub_m_alpc;
DROP TABLE t_merge_sub_alpc;
DROP TABLE t_merge_sub_m_alp;
DROP TABLE t_merge_sub_alp;
DROP TABLE t_merge_sub_m_eph;
DROP TABLE t_merge_sub_eph;
DROP TABLE t_merge_sub_unread_good;
DROP TABLE t_merge_sub_rpa;
DROP TABLE t_merge_sub_rpa_str;
DROP TABLE t_merge_sub_rpa_good;
DROP TABLE t_merge_sub_rp;
DROP TABLE t_merge_sub_rp_str;
DROP TABLE t_merge_sub_rp_good;
DROP TABLE t_merge_sub_al_outer;
DROP TABLE t_merge_sub_al;
DROP TABLE t_merge_sub_al_str;
DROP TABLE t_merge_sub_al_good;
DROP TABLE t_merge_sub_hom;
DROP TABLE t_merge_sub_hom_b;
DROP TABLE t_merge_sub_hom_a;
-- Two children with identical declared columns but different `supportsSubcolumns()` must each get
-- their own plan: the requested subcolumn resolves for one and is derived from the parent for the
-- other. Both child orders are asserted because the first one planned is the one that seeds reuse.
SELECT 'capability differs under identical declarations, high-capability child first';
CREATE TABLE t_merge_sub_cap_a_mt (k UInt8, arr Array(UInt8)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_merge_sub_cap_b_rd (k UInt8, arr Array(UInt8)) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO t_merge_sub_cap_a_mt VALUES (1, [1, 2]);
INSERT INTO t_merge_sub_cap_b_rd VALUES (2, [3, 4, 5]);
CREATE TABLE t_merge_sub_m_cap1 (k UInt8, arr Array(UInt8))
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_cap_(a_mt|b_rd)$');
SELECT sum(arr.size0) FROM t_merge_sub_m_cap1;
SELECT k, arr.size0 FROM t_merge_sub_m_cap1 ORDER BY k;

SELECT 'capability differs under identical declarations, low-capability child first';
CREATE TABLE t_merge_sub_cap2_a_rd (k UInt8, arr Array(UInt8)) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
CREATE TABLE t_merge_sub_cap2_b_mt (k UInt8, arr Array(UInt8)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_merge_sub_cap2_a_rd VALUES (2, [3, 4, 5]);
INSERT INTO t_merge_sub_cap2_b_mt VALUES (1, [1, 2]);
CREATE TABLE t_merge_sub_m_cap2 (k UInt8, arr Array(UInt8))
    ENGINE = Merge(currentDatabase(), '^t_merge_sub_cap2_(a_rd|b_mt)$');
SELECT sum(arr.size0) FROM t_merge_sub_m_cap2;
SELECT k, arr.size0 FROM t_merge_sub_m_cap2 ORDER BY k;

-- The parent read is unaffected in both orders, so the arms above are about the subcolumn.
SELECT 'the parent column agrees in both orders';
SELECT sum(length(arr)) FROM t_merge_sub_m_cap1 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT sum(length(arr)) FROM t_merge_sub_m_cap2 SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE t_merge_sub_m_cap2;
DROP TABLE t_merge_sub_cap2_b_mt;
DROP TABLE t_merge_sub_cap2_a_rd;
DROP TABLE t_merge_sub_m_cap1;
DROP TABLE t_merge_sub_cap_b_rd;
DROP TABLE t_merge_sub_cap_a_mt;

DROP TABLE t_merge_sub_m_null;
DROP TABLE t_merge_sub_isnull;
DROP TABLE t_merge_sub_notnull;
DROP TABLE t_merge_sub_m_nocol;
DROP TABLE t_merge_sub_nocol;
DROP TABLE t_merge_sub_m_map;
DROP TABLE t_merge_sub_map;
DROP TABLE t_merge_sub_m_nullelem;
DROP TABLE t_merge_sub_nullelem;
DROP TABLE t_merge_sub_m_u64;
DROP TABLE t_merge_sub_u64;
DROP TABLE t_merge_sub_outer;
DROP TABLE t_merge_sub_twin;
DROP TABLE t_merge_sub_twin_b;
DROP TABLE t_merge_sub_twin_a;
DROP TABLE t_merge_sub_twin_good;
DROP TABLE t_merge_sub_nest;
DROP TABLE t_merge_sub_nest_str;
DROP TABLE t_merge_sub_nest_good;
DROP TABLE t_merge_sub;
DROP TABLE t_merge_sub_str;
DROP TABLE t_merge_sub_good;
