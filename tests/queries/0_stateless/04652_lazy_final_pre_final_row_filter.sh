#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Almost every statement below is sent over HTTP with curl rather than through `clickhouse client`,
# because on a sanitizer build a client spawn costs 0.54 s against 0.009 s for curl.
# Randomized settings reach both transports - TestCase.add_effective_settings fills
# CLICKHOUSE_URL_PARAMS from the same draw as CLICKHOUSE_CLIENT_OPT - so the pins and the hostile
# arms below behave identically either way.
#
# Every fixture row is inserted with `VALUES`, not `SELECT <constants>`. Both write one part per
# statement, but `INSERT ... SELECT` builds a query pipeline sized by `max_threads`
# (InterpreterInsertQuery.cpp:135-139), so a single-row insert fans out to one thread per core:
# measured 150 threads and 69 s of server CPU per test run, against 2 threads and 0.06 s for `VALUES`.
#
# `optimize_lazy_final` is `query_plan_optimize_lazy_final && allow_experimental_analyzer`
# (QueryPlanOptimizationSettings.cpp), so with the old analyzer the optimization never runs and every
# plan-shape assertion below changes answer. Two lanes reach that state: the dedicated old-analyzer job
# (install.sh symlinks users.d/analyzer.xml) and stress `compatibility` randomization
# (allow_experimental_analyzer flipped to true in the 24.3 block, so a pre-24.3 value reverts it). A
# pin defends against both: for the client because applyCompatibilitySetting skips manually-changed
# settings, and for curl because the last occurrence of a repeated URL parameter is the one applied.
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --enable_analyzer=1"
CLICKHOUSE_URL="${CLICKHOUSE_URL}&enable_analyzer=1"

# Send one statement over HTTP. `--data-binary` keeps the query bytes verbatim, and `-sS` reports
# transport errors while staying quiet about progress.
ch() {
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "$1"
}

# A row policy is a server-global object, so its table is scoped to $CLICKHOUSE_DATABASE and the
# policy is dropped before and after. This is why the test is .sh and not .sql.

POLICY="p_${CLICKHOUSE_DATABASE}"

cleanup() {
    ch "DROP ROW POLICY IF EXISTS ${POLICY} ON t" 2>/dev/null
    ch "DROP ROW POLICY IF EXISTS ${POLICY}_sk ON t_sk" 2>/dev/null
    ch "DROP ROW POLICY IF EXISTS ${POLICY}_del ON t_del" 2>/dev/null
    ch "DROP ROW POLICY IF EXISTS ${POLICY}_rand ON t" 2>/dev/null
    ch "DROP ROW POLICY IF EXISTS ${POLICY}_throw ON t" 2>/dev/null
    ch "DROP ROW POLICY IF EXISTS ${POLICY}_state ON t" 2>/dev/null
    ch "DROP ROW POLICY IF EXISTS ${POLICY}_two ON t_two" 2>/dev/null
    ch "DROP ROW POLICY IF EXISTS ${POLICY}_ver ON t_del" 2>/dev/null
    ch "DROP ROW POLICY IF EXISTS ${POLICY}_fp ON t_fp" 2>/dev/null
    ch "DROP ROW POLICY IF EXISTS ${POLICY}_fp_safe ON t_fp" 2>/dev/null
    ch "DROP ROW POLICY IF EXISTS ${POLICY}_mix ON t_mix" 2>/dev/null
    ch "DROP ROW POLICY IF EXISTS ${POLICY}_mix_rand ON t_mix" 2>/dev/null
}
cleanup

${CLICKHOUSE_CLIENT} -mn -q "
-- A part written with optimize_on_insert = 0 stays at level 0, and PartsSplitter must treat a level-0
-- part as intersecting because it may hold duplicate primary keys (PartsSplitter.cpp:470). That is
-- correct behaviour, but it means no part is ever classified non-intersecting, so both the
-- all-non-intersecting fast path and the partial split disappear and the plan-shape assertions below
-- would measure the wrong thing. The setting only matters while the parts are being written.
SET optimize_on_insert = 1;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_sk;
DROP TABLE IF EXISTS t_del;
DROP TABLE IF EXISTS t_two;
DROP TABLE IF EXISTS t_float;
DROP TABLE IF EXISTS t_null;
DROP TABLE IF EXISTS t_rev;
DROP TABLE IF EXISTS t_txt;
DROP TABLE IF EXISTS t_fp;
DROP TABLE IF EXISTS t_mix;

-- Key 1's overall winner is flag = 1, so a filter applied BEFORE the merge must surface its
-- flag = 0 row, while a filter applied after the merge must not. Key 2's winner is flag = 0, so
-- both orderings agree on it. Key 3 has no flag = 0 row and is absent either way.
CREATE TABLE t (k UInt64, flag UInt8, version UInt64)
ENGINE = ReplacingMergeTree(version) ORDER BY k SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t;
INSERT INTO t VALUES (1, 0, 1);
INSERT INTO t VALUES (1, 1, 2);
INSERT INTO t VALUES (2, 1, 1);
INSERT INTO t VALUES (2, 0, 2);
INSERT INTO t VALUES (3, 1, 1);
INSERT INTO t VALUES (3, 1, 2);

CREATE TABLE t_sk (k UInt64, sk UInt64, version UInt64)
ENGINE = ReplacingMergeTree(version) ORDER BY (k, sk) SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_sk;
INSERT INTO t_sk VALUES (1, 7, 1);
INSERT INTO t_sk VALUES (1, 7, 2);
INSERT INTO t_sk VALUES (2, 9, 1);
INSERT INTO t_sk VALUES (2, 9, 2);

-- Two independent pre-FINAL filters, one per column, arranged so that dropping either one changes
-- the selected winner into a row the post-merge filter then rejects. Key 4's version ladder is
-- v1 = (flag 0, tier 1) passes both, v2 = (flag 0, tier 9) passes only the policy, v3 = (flag 1,
-- tier 1) passes only the PREWHERE. So the winner is v1 under both filters, v2 under the policy
-- alone and v3 under the PREWHERE alone, and only v1 survives the post-merge filter: an
-- implementation that propagates just one of the two filters returns nothing here.
CREATE TABLE t_two (k UInt64, flag UInt8, tier UInt8, version UInt64)
ENGINE = ReplacingMergeTree(version) ORDER BY k SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_two;
INSERT INTO t_two VALUES (4, 0, 1, 1);
INSERT INTO t_two VALUES (4, 0, 9, 2);
INSERT INTO t_two VALUES (4, 1, 1, 3);

CREATE TABLE t_del (k UInt64, flag UInt8, version UInt64, is_deleted UInt8)
ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY k SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_del;
INSERT INTO t_del VALUES (1, 0, 1, 0);
INSERT INTO t_del VALUES (1, 1, 2, 0);
INSERT INTO t_del VALUES (2, 1, 1, 0);
INSERT INTO t_del VALUES (2, 0, 2, 0);
INSERT INTO t_del VALUES (3, 0, 1, 0);
INSERT INTO t_del VALUES (3, 0, 2, 1);

-- Tables whose sorting key makes the part splitter decline BEFORE it looks at the parts:
-- isSafePrimaryKey rejects Float and Nullable keys, and a sorting key that mixes ASC with DESC is
-- rejected separately. Those declines report 'leave the read alone', which is a different answer from
-- 'there was nothing to split', and each is reached with an unsafe pre-FINAL filter below.
CREATE TABLE t_float (k Float64, flag UInt8, version UInt64)
ENGINE = ReplacingMergeTree(version) ORDER BY k SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_float;
INSERT INTO t_float VALUES (1.5, 0, 1);
INSERT INTO t_float VALUES (1.5, 1, 2);
INSERT INTO t_float VALUES (2.5, 0, 1);

CREATE TABLE t_null (k Nullable(UInt64), flag UInt8, version UInt64)
ENGINE = ReplacingMergeTree(version) ORDER BY k SETTINGS index_granularity = 1, allow_nullable_key = 1;
SYSTEM STOP MERGES t_null;
INSERT INTO t_null VALUES (1, 0, 1);
INSERT INTO t_null VALUES (1, 1, 2);
INSERT INTO t_null VALUES (2, 0, 1);

CREATE TABLE t_rev (a UInt64, b UInt64, flag UInt8, version UInt64)
ENGINE = ReplacingMergeTree(version) ORDER BY (a ASC, b DESC)
SETTINGS index_granularity = 1, allow_experimental_reverse_key = 1, allow_nullable_key = 1;
SYSTEM STOP MERGES t_rev;
INSERT INTO t_rev VALUES (1, 5, 0, 1);
INSERT INTO t_rev VALUES (1, 5, 1, 2);
INSERT INTO t_rev VALUES (2, 6, 0, 1);

-- Float key (so the splitter declines before inspecting the parts, as for t_float) PLUS a text index,
-- which is the one combination that reaches the third precondition those early declines re-test: a
-- direct read from a text index registers index read tasks the lazy source cannot produce virtual
-- columns for. The direct read has no key-safety gate of its own, so an unsafe key carries index read
-- tasks exactly like a safe one. Distinct key values from t_float so a mix-up is visible.
CREATE TABLE t_txt (k Float64, flag UInt8, version UInt64, s String,
                    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = ReplacingMergeTree(version) ORDER BY k SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_txt;
INSERT INTO t_txt VALUES (3.5, 0, 1, 'alpha beta');
INSERT INTO t_txt VALUES (3.5, 1, 2, 'alpha gamma');
INSERT INTO t_txt VALUES (4.5, 0, 1, 'alpha delta');

-- Every key sits in exactly one part, so all parts are non-intersecting and the read is replaced by a
-- plain non-FINAL read. That replacement needs no winner-selection read, so it must remain available
-- even when the lazy branch is not.
CREATE TABLE t_fp (k UInt64, flag UInt8, version UInt64)
ENGINE = ReplacingMergeTree(version) ORDER BY k SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_fp;
INSERT INTO t_fp VALUES (1, 0, 1);
INSERT INTO t_fp VALUES (2, 1, 1);
INSERT INTO t_fp VALUES (3, 0, 1);

-- Key 1 is spread over two parts while keys 2 and 10 are each alone, so the split is partial: the
-- lazy branch handles key 1 and the non-intersecting half is unioned back. Key 1's overall winner is
-- flag = 1, so a correct pre-FINAL filter surfaces its flag = 0 row while keys 2 and 10 must arrive
-- through the union - the arms below assert both halves at once.
CREATE TABLE t_mix (k UInt64, flag UInt8, version UInt64)
ENGINE = ReplacingMergeTree(version) ORDER BY k SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_mix;
INSERT INTO t_mix VALUES (1, 0, 1);
INSERT INTO t_mix VALUES (1, 1, 2);
INSERT INTO t_mix VALUES (2, 0, 1);
INSERT INTO t_mix VALUES (10, 0, 1);
"

# ORDER BY on the FINAL query disables the optimization (readsInOrder), so the results are
# collected with arraySort(groupArray(...)) instead.
LAZY_ON="query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0"
LAZY_OFF="query_plan_optimize_lazy_final = 0"

# Each answer is asserted against the same query with the optimization disabled, so the test pins
# equality between the lazy and the ordinary FINAL path rather than a plan-dependent constant.
run_pair() {
    local label="$1" query="$2" extra="$3"
    local on off
    on=$(ch "${query} SETTINGS ${extra}${LAZY_ON}")
    off=$(ch "${query} SETTINGS ${extra}${LAZY_OFF}")
    echo -e "${label}\t${on}\t${off}\t$([ "$on" = "$off" ] && echo same || echo DIFFERENT)"
}

# Whether the lazy plan was built at all. Without this the fix could silently degenerate into
# "lazy FINAL never applies", which is a performance regression that a result-only test cannot see.
lazy_fires() {
    local label="$1" query="$2" extra="$3"
    echo -e "${label}\t$(ch "SELECT count() > 0 FROM (EXPLAIN indexes = 0 ${query} SETTINGS ${extra}${LAZY_ON}) WHERE explain ILIKE '%LazyReadReplacingFinal%'")"
}

# Whether the lazy branch actually EXECUTED, not merely that its plan was built (`lazy_fires`): a
# runtime degradation of the true-branch signal keeps every result row equal and every EXPLAIN row
# unchanged. LazyMaterializingRows is created only on the true branch, so its "Lazily reading" trace
# fires only when that branch runs. The log level is requested per statement because shell_config.sh
# already passes one on the command line and a repeated option is rejected. The count is a boolean
# because one transform runs per pipeline stream, and the arms must return a non-empty result: with no
# winners the transform finishes before it materializes anything.
#
# This is the one helper that cannot use curl: server log lines are not part of the HTTP response
# body, and neither system.text_log nor the X-ClickHouse-Summary header carries this trace. There are
# only four such arms, so the client cost is paid deliberately here.
lazy_runs() {
    local label="$1" query="$2" extra="$3"
    local n
    n=$(${CLICKHOUSE_CLIENT} -q "${query} SETTINGS ${extra}${LAZY_ON}, send_logs_level = 'trace'" 2>&1 \
        | grep -c 'LazyMaterializingTransform.*Lazily reading')
    echo -e "${label}\t$([ "$n" -gt 0 ] && echo 1 || echo 0)"
}

# Presence of an arbitrary plan step name. `lazy_fires` alone cannot tell "the whole read was replaced
# by a non-FINAL read" from "the optimization declined and the ordinary FINAL read was kept" - both
# read 0 - so the arms below pair it with Union (partial split) and NonIntersectingSplit (the split's
# own index entry).
plan_has() {
    local label="$1" query="$2" extra="$3" needle="$4" idx="${5:-0}"
    echo -e "${label}\t$(ch "SELECT count() > 0 FROM (EXPLAIN indexes = ${idx} ${query} SETTINGS ${extra}${LAZY_ON}) WHERE explain ILIKE '%${needle}%'")"
}

# Whether the read still carries FINAL. This is the discriminator between "the all-non-intersecting
# fast path replaced the read" (createNonIntersectingPlan clears it, so absent) and any decline (the
# ordinary FINAL read is kept, so present). `plan_has` cannot serve here: `FINAL: 1` comes from
# ReadFromMergeTree::describeActions, which QueryPlan.cpp gates on `options.actions`, and while
# `explain_query_plan_default = pretty` currently force-sets that flag for a bare EXPLAIN PLAN, a
# pre-26.7 `compatibility` value maps the setting back to `legacy` and the line disappears - measured.
# CI randomizes `compatibility` in stress runs, so the mode has to be requested explicitly.
plan_has_actions() {
    local label="$1" query="$2" extra="$3" needle="$4"
    echo -e "${label}\t$(ch "SELECT count() > 0 FROM (EXPLAIN actions = 1 ${query} SETTINGS ${extra}${LAZY_ON}) WHERE explain ILIKE '%${needle}%'")"
}

echo "-- parts must stay unmerged, otherwise FINAL has nothing to deduplicate"
ch "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active"

echo "-- pre-FINAL filters: lazy FINAL must return the same rows as the ordinary FINAL read"
ch "CREATE ROW POLICY ${POLICY} ON t AS RESTRICTIVE FOR SELECT USING (flag = 0) TO ALL"
run_pair   "row_policy"            "SELECT arraySort(groupArray(k)) FROM t FINAL" "apply_row_policy_after_final = 0, "
lazy_fires "row_policy_optimized"  "SELECT arraySort(groupArray(k)) FROM t FINAL" "apply_row_policy_after_final = 0, "
lazy_runs  "row_policy_runs"       "SELECT arraySort(groupArray(k)) FROM t FINAL" "apply_row_policy_after_final = 0, "
run_pair   "row_policy_count"      "SELECT count() FROM t FINAL" "apply_row_policy_after_final = 0, "
run_pair   "row_policy_prewhere_promoted"           "SELECT arraySort(groupArray(k)) FROM t FINAL" "apply_row_policy_after_final = 0, optimize_move_to_prewhere_if_final = 1, "
lazy_fires "row_policy_prewhere_promoted_optimized" "SELECT arraySort(groupArray(k)) FROM t FINAL" "apply_row_policy_after_final = 0, optimize_move_to_prewhere_if_final = 1, "
ch "DROP ROW POLICY ${POLICY} ON t"

# A row policy together with an explicit PREWHERE on a different column: both filters are pre-FINAL,
# so BOTH must reach the winner-selection read. Dropping either one selects a different winner that
# the post-merge filter rejects, so this arm returns the empty array unless both are propagated.
ch "CREATE ROW POLICY ${POLICY}_two ON t_two AS RESTRICTIVE FOR SELECT USING (flag = 0) TO ALL"
run_pair   "row_policy_and_prewhere"           "SELECT arraySort(groupArray(k)) FROM t_two FINAL PREWHERE tier < 5" "apply_row_policy_after_final = 0, "
lazy_fires "row_policy_and_prewhere_optimized" "SELECT arraySort(groupArray(k)) FROM t_two FINAL PREWHERE tier < 5" "apply_row_policy_after_final = 0, "
# The winner's own non-sorting-key columns must be the ones from the row that passes both filters.
run_pair   "row_policy_and_prewhere_payload"   "SELECT arraySort(groupArray((tier, version))) FROM t_two FINAL PREWHERE tier < 5" "apply_row_policy_after_final = 0, "
ch "DROP ROW POLICY ${POLICY}_two ON t_two"

# apply_prewhere_after_final defaults to false, so an explicit PREWHERE is pre-FINAL by default.
run_pair   "prewhere"           "SELECT arraySort(groupArray(k)) FROM t FINAL PREWHERE flag = 0" ""
lazy_fires "prewhere_optimized" "SELECT arraySort(groupArray(k)) FROM t FINAL PREWHERE flag = 0" ""

echo "-- is_deleted: the winner-selection argMax over is_deleted must see the filtered rows too"
ch "CREATE ROW POLICY ${POLICY}_del ON t_del AS RESTRICTIVE FOR SELECT USING (flag = 0) TO ALL"
run_pair   "is_deleted_policy"           "SELECT arraySort(groupArray(k)) FROM t_del FINAL" "apply_row_policy_after_final = 0, "
lazy_fires "is_deleted_policy_optimized" "SELECT arraySort(groupArray(k)) FROM t_del FINAL" "apply_row_policy_after_final = 0, "
ch "DROP ROW POLICY ${POLICY}_del ON t_del"
# A filter on is_deleted itself consumes the very column the aggregation reads.
run_pair   "is_deleted_prewhere"           "SELECT arraySort(groupArray(k)) FROM t_del FINAL PREWHERE is_deleted = 0" ""
lazy_fires "is_deleted_prewhere_optimized" "SELECT arraySort(groupArray(k)) FROM t_del FINAL PREWHERE is_deleted = 0" ""

echo "-- an unsafe pre-FINAL filter must not be pushed into the winner-selection read"
# Nondeterministic: rand() is caught by the hasNonDeterministic half of the safety check.
ch "CREATE ROW POLICY ${POLICY}_rand ON t AS RESTRICTIVE FOR SELECT USING (flag = 0 AND rand() % 1 = 0) TO ALL"
lazy_fires "nondeterministic_policy_not_optimized" "SELECT arraySort(groupArray(k)) FROM t FINAL" "apply_row_policy_after_final = 0, "
# Negative control for the three `_runs` arms: a helper that always reported "enabled" would pass them all.
lazy_runs  "nondeterministic_policy_not_runs"      "SELECT arraySort(groupArray(k)) FROM t FINAL" "apply_row_policy_after_final = 0, "
run_pair   "nondeterministic_policy"               "SELECT arraySort(groupArray(k)) FROM t FINAL" "apply_row_policy_after_final = 0, "
ch "DROP ROW POLICY ${POLICY}_rand ON t"
# Stateful but deterministic: timeSeriesStoreTags writes to the per-query tags collector, so
# evaluating it on the winner-selection read's extra rows would record tags for rows the query never
# returns. It is declared deterministic, so only the hasStatefulFunctions half rejects it.
ch "CREATE ROW POLICY ${POLICY}_state ON t AS RESTRICTIVE FOR SELECT USING (flag = 0 AND timeSeriesStoreTags(toUInt64(k), [('t', 'x')]) = k) TO ALL"
lazy_fires "stateful_policy_not_optimized" "SELECT arraySort(groupArray(k)) FROM t FINAL" "apply_row_policy_after_final = 0, "
run_pair   "stateful_policy"               "SELECT arraySort(groupArray(k)) FROM t FINAL" "apply_row_policy_after_final = 0, "
ch "DROP ROW POLICY ${POLICY}_state ON t"
# The safety check runs once per filter kind, on the cloned row-level filter and on the cloned
# prewhere independently. With no row policy in place only the prewhere-branch call can reject this
# predicate, so this arm is what keeps that call load-bearing.
lazy_fires "nondeterministic_prewhere_not_optimized" "SELECT arraySort(groupArray(k)) FROM t FINAL PREWHERE flag = 0 AND rand() % 1 = 0" ""
run_pair   "nondeterministic_prewhere"               "SELECT arraySort(groupArray(k)) FROM t FINAL PREWHERE flag = 0 AND rand() % 1 = 0" ""

# throwIf is declared deterministic and stateless, so it IS pushed into the winner-selection read.
# The read sees no row the set-building read does not already evaluate the predicate on, so this only
# pins the behaviour rather than asserting an exemption. The plan assertion is what distinguishes
# "admitted" from "the optimization silently declined".
echo "-- a throwing pre-FINAL filter behaves the same with and without the optimization"
ch "CREATE ROW POLICY ${POLICY}_throw ON t AS RESTRICTIVE FOR SELECT USING (flag = 0 AND NOT throwIf(k = 99, 'never')) TO ALL"
run_pair   "throwing_policy"           "SELECT arraySort(groupArray(k)) FROM t FINAL" "apply_row_policy_after_final = 0, "
lazy_fires "throwing_policy_optimized" "SELECT arraySort(groupArray(k)) FROM t FINAL" "apply_row_policy_after_final = 0, "
ch "DROP ROW POLICY ${POLICY}_throw ON t"

echo "-- the part splitter's early declines must keep the ordinary FINAL read, not half-apply the split"
# These three tables are rejected by the splitter before it inspects the parts, and each is queried
# with an unsafe pre-FINAL filter so the lazy branch is unavailable at the same time. That pair of
# conditions is what an implementation keying the decision on a single "stop here" flag gets wrong:
# the early declines report "carry on" while the winner-selection read does not exist.
#
# Each table also gets a SAFE-predicate arm. Those are positive controls, and they are carriers of
# this PR's defect in their own right: a table this splitter cannot analyse is still read through the
# lazy branch, so before the fix the pre-FINAL filter was dropped there and the wrong rows came back
# (measured on the merge-base: [2.5] instead of [1.5,2.5], [2] instead of [1,2] twice). Because these
# two returns leave the read un-narrowed they may build the lazy branch, but only when the read-order
# and text-index checks at the bottom of the function pass too, which is why they re-test all three.
run_pair   "unsafe_pk_float"           "SELECT arraySort(groupArray(k)) FROM t_float FINAL PREWHERE flag = 0 AND rand() % 1 = 0" ""
lazy_fires "unsafe_pk_float_optimized" "SELECT arraySort(groupArray(k)) FROM t_float FINAL PREWHERE flag = 0 AND rand() % 1 = 0" ""
run_pair   "safe_pk_float"           "SELECT arraySort(groupArray(k)) FROM t_float FINAL PREWHERE flag = 0" ""
lazy_fires "safe_pk_float_optimized" "SELECT arraySort(groupArray(k)) FROM t_float FINAL PREWHERE flag = 0" ""
lazy_runs  "safe_pk_float_runs"      "SELECT arraySort(groupArray(k)) FROM t_float FINAL PREWHERE flag = 0" ""
run_pair   "unsafe_pk_nullable"           "SELECT arraySort(groupArray(k)) FROM t_null FINAL PREWHERE flag = 0 AND rand() % 1 = 0" ""
lazy_fires "unsafe_pk_nullable_optimized" "SELECT arraySort(groupArray(k)) FROM t_null FINAL PREWHERE flag = 0 AND rand() % 1 = 0" ""
run_pair   "safe_pk_nullable"           "SELECT arraySort(groupArray(k)) FROM t_null FINAL PREWHERE flag = 0" ""
lazy_fires "safe_pk_nullable_optimized" "SELECT arraySort(groupArray(k)) FROM t_null FINAL PREWHERE flag = 0" ""
run_pair   "mixed_reverse_key"           "SELECT arraySort(groupArray(a)) FROM t_rev FINAL PREWHERE flag = 0 AND rand() % 1 = 0" ""
lazy_fires "mixed_reverse_key_optimized" "SELECT arraySort(groupArray(a)) FROM t_rev FINAL PREWHERE flag = 0 AND rand() % 1 = 0" ""
run_pair   "safe_mixed_reverse_key"           "SELECT arraySort(groupArray(a)) FROM t_rev FINAL PREWHERE flag = 0" ""
lazy_fires "safe_mixed_reverse_key_optimized" "SELECT arraySort(groupArray(a)) FROM t_rev FINAL PREWHERE flag = 0" ""
# The third precondition the unsafe-key returns re-test: a direct read from a text index. Both arms
# above vary only `allow_partial_split`, so without these the text-index term could be deleted from the
# unsafe-key guard and every test in the tree would stay green - measured. The predicate is safe and the
# key is unsafe, so the read is un-narrowed and the branch would be built if the term were missing.
# Two settings gate the direct read, and the runner randomizes both: query_plan_direct_read_from_text_index
# turns the token predicate into one (off ~5% of runs), and use_skip_indexes_if_final = 0 makes index
# analysis return before any index is collected under FINAL (ReadFromMergeTree.cpp:2305), so no read task
# is registered and the branch legitimately builds - measured, 20 of 50 randomized runs. Both are pinned
# per statement on these arms only, because no other arm in this file depends on either.
TXT_DIRECT="query_plan_direct_read_from_text_index = 1, use_skip_indexes_if_final = 1, "
lazy_fires "unsafe_pk_text_index_not_optimized" "SELECT arraySort(groupArray(k)) FROM t_txt FINAL PREWHERE flag = 0 AND hasToken(s, 'alpha')" "$TXT_DIRECT"
run_pair   "unsafe_pk_text_index"               "SELECT arraySort(groupArray(k)) FROM t_txt FINAL PREWHERE flag = 0 AND hasToken(s, 'alpha')" "$TXT_DIRECT"
# The decline above reads 0 exactly like a query that never registered an index read task at all, so it
# only means something beside a positive pin that the rewrite happened. The synthetic column the rewrite
# introduces is the marker; it is absent when query_plan_direct_read_from_text_index or use_skip_indexes
# is off - measured. It pins the REWRITE, not the read task: at use_skip_indexes_if_final = 0 the column
# is still produced while no task is registered, which is why that setting is pinned above rather than
# left to this assertion.
plan_has   "unsafe_pk_text_index_direct_read"   "SELECT arraySort(groupArray(k)) FROM t_txt FINAL PREWHERE flag = 0 AND hasToken(s, 'alpha')" "$TXT_DIRECT" "__text_index"
# Positive control: the same table and predicate without the token search. No index read task is
# registered, so the unsafe key alone still builds the lazy branch, which is what makes the arms above
# attributable to the text index rather than to the fixture.
lazy_fires "unsafe_pk_text_index_absent_optimized" "SELECT arraySort(groupArray(k)) FROM t_txt FINAL PREWHERE flag = 0" "$TXT_DIRECT"
run_pair   "unsafe_pk_text_index_absent"           "SELECT arraySort(groupArray(k)) FROM t_txt FINAL PREWHERE flag = 0" "$TXT_DIRECT"
# The same two declines under an ORDER BY on the primary key. `readsInOrder` is what
# `allow_partial_split` is derived from, and the lazy replacement does not preserve order, so these
# must decline - a sibling test asserts the same for a safe key
# (04539_lazy_final_read_in_order_limit_disabled.sql). No arraySort wrapper: the ORDER BY is the point.
# optimize_read_in_order is what turns the ORDER BY into an in-order read, and the runner randomizes it
# to 0 half the time; at 0 the sort consumes the whole stream so an unordered replacement is legitimate
# and the arms answer 1 - measured. It is pinned here only, because no other arm depends on it.
IN_ORDER="optimize_read_in_order = 1, "
lazy_fires "unsafe_pk_float_order_by_optimized" "SELECT k FROM t_float FINAL PREWHERE flag = 0 ORDER BY k" "$IN_ORDER"
run_pair   "unsafe_pk_float_order_by"           "SELECT k FROM t_float FINAL PREWHERE flag = 0 ORDER BY k" "$IN_ORDER"
lazy_fires "mixed_reverse_order_by_optimized"   "SELECT a FROM t_rev FINAL PREWHERE flag = 0 ORDER BY a" "$IN_ORDER"
run_pair   "mixed_reverse_order_by"             "SELECT a FROM t_rev FINAL PREWHERE flag = 0 ORDER BY a" "$IN_ORDER"

echo "-- an unavailable lazy branch must not cost the all-non-intersecting fast path"
# Every part holds a distinct key, so the read is replaced by a plain non-FINAL read that needs no
# winner-selection read at all. `FINAL: 1` is the discriminator: createNonIntersectingPlan clears FINAL,
# so its absence means the replacement really happened, while any decline keeps the ordinary FINAL read
# and prints it. InputSelector is kept alongside as a cheap lazy-branch-absence pin, but it cannot
# discriminate here - InputSelectorStep has a single construction site inside the lazy-branch wiring,
# so an early decline reads 0 exactly like a successful replacement.
ch "CREATE ROW POLICY ${POLICY}_fp ON t_fp AS RESTRICTIVE FOR SELECT USING (flag = 0 AND rand() % 1 = 0) TO ALL"
run_pair "fastpath_unsafe_policy"                "SELECT arraySort(groupArray(k)) FROM t_fp FINAL" "apply_row_policy_after_final = 0, "
lazy_fires "fastpath_unsafe_policy_optimized"    "SELECT arraySort(groupArray(k)) FROM t_fp FINAL" "apply_row_policy_after_final = 0, "
plan_has "fastpath_unsafe_policy_input_selector" "SELECT arraySort(groupArray(k)) FROM t_fp FINAL" "apply_row_policy_after_final = 0, " "InputSelector"
plan_has_actions "fastpath_unsafe_policy_final_absent" "SELECT arraySort(groupArray(k)) FROM t_fp FINAL" "apply_row_policy_after_final = 0, " "FINAL: 1"
ch "DROP ROW POLICY ${POLICY}_fp ON t_fp"
ch "CREATE ROW POLICY ${POLICY}_fp_safe ON t_fp AS RESTRICTIVE FOR SELECT USING (flag = 0) TO ALL"
run_pair "fastpath_safe_policy"                "SELECT arraySort(groupArray(k)) FROM t_fp FINAL" "apply_row_policy_after_final = 0, "
plan_has "fastpath_safe_policy_input_selector" "SELECT arraySort(groupArray(k)) FROM t_fp FINAL" "apply_row_policy_after_final = 0, " "InputSelector"
plan_has_actions "fastpath_safe_policy_final_absent" "SELECT arraySort(groupArray(k)) FROM t_fp FINAL" "apply_row_policy_after_final = 0, " "FINAL: 1"
ch "DROP ROW POLICY ${POLICY}_fp_safe ON t_fp"
# Negative control for the two assertions above: a query the optimization legitimately declines must
# still report FINAL, otherwise a `FINAL: 1`-absent row proves nothing. The unsafe predicate on the
# unsafe-key table is declined by both gates at once.
plan_has_actions "declined_final_present" "SELECT arraySort(groupArray(k)) FROM t_float FINAL PREWHERE flag = 0 AND rand() % 1 = 0" "" "FINAL: 1"

echo "-- partial split: the pre-FINAL filter must reach the lazy half without losing the other half"
ch "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_mix' AND active"
ch "CREATE ROW POLICY ${POLICY}_mix ON t_mix AS RESTRICTIVE FOR SELECT USING (flag = 0) TO ALL"
run_pair   "partial_split_policy"           "SELECT arraySort(groupArray(k)) FROM t_mix FINAL" "apply_row_policy_after_final = 0, "
lazy_fires "partial_split_policy_optimized" "SELECT arraySort(groupArray(k)) FROM t_mix FINAL" "apply_row_policy_after_final = 0, "
lazy_runs  "partial_split_policy_runs"      "SELECT arraySort(groupArray(k)) FROM t_mix FINAL" "apply_row_policy_after_final = 0, "
# Without these two the fixture could silently stop being a partial split and both arms would go vacuous.
plan_has   "partial_split_policy_union"     "SELECT arraySort(groupArray(k)) FROM t_mix FINAL" "apply_row_policy_after_final = 0, " "Union"
plan_has   "partial_split_policy_index"     "SELECT arraySort(groupArray(k)) FROM t_mix FINAL" "apply_row_policy_after_final = 0, " "NonIntersectingSplit" 1
# The winner's own columns must come from the row that passes the filter, in both halves.
run_pair   "partial_split_policy_payload"   "SELECT arraySort(groupArray((k, flag, version))) FROM t_mix FINAL" "apply_row_policy_after_final = 0, "
ch "DROP ROW POLICY ${POLICY}_mix ON t_mix"
# Same partial-split shape with an unsafe predicate: the split must be declined as a whole rather than
# narrowing the read and then finding it has no winner-selection read to pair with.
ch "CREATE ROW POLICY ${POLICY}_mix_rand ON t_mix AS RESTRICTIVE FOR SELECT USING (flag = 0 AND rand() % 1 = 0) TO ALL"
run_pair   "partial_split_unsafe"           "SELECT arraySort(groupArray(k)) FROM t_mix FINAL" "apply_row_policy_after_final = 0, "
lazy_fires "partial_split_unsafe_optimized" "SELECT arraySort(groupArray(k)) FROM t_mix FINAL" "apply_row_policy_after_final = 0, "
ch "DROP ROW POLICY ${POLICY}_mix_rand ON t_mix"

echo "-- a bare required column as the predicate must not be consumed away by the filter"
# `version` is read by the winner-selection aggregation AND is the predicate's own output name, so the
# containment check against the aggregation's required columns is the only thing keeping the column.
# Both filter kinds take bare names, so both consumers of that check are covered.
run_pair   "bare_required_prewhere"           "SELECT arraySort(groupArray((k, version))) FROM t_del FINAL PREWHERE version" ""
lazy_fires "bare_required_prewhere_optimized" "SELECT arraySort(groupArray((k, version))) FROM t_del FINAL PREWHERE version" ""
ch "CREATE ROW POLICY ${POLICY}_ver ON t_del AS RESTRICTIVE FOR SELECT USING version TO ALL"
run_pair   "bare_required_policy"           "SELECT arraySort(groupArray((k, version))) FROM t_del FINAL" "apply_row_policy_after_final = 0, "
lazy_fires "bare_required_policy_optimized" "SELECT arraySort(groupArray((k, version))) FROM t_del FINAL" "apply_row_policy_after_final = 0, "
ch "DROP ROW POLICY ${POLICY}_ver ON t_del"

# There is deliberately no arm for the automatic-parallel-replicas candidate plan, which is the one
# case where the deferral analysis has not run (it is built with query_plan_optimize_primary_key
# disabled) and a filter's pre/post-FINAL ordering therefore cannot be determined. The guard in
# optimizeLazyFinal is a fail-closed precondition, not a live path: a query that still has FINAL when
# the dataflow-statistics walk runs fails it, because ReadFromMergeTree's
# supportsDataflowStatisticsCollection() is !isQueryWithFinal(); and parallel replicas are
# independently disabled for FINAL in the planner. Do not re-add a settings-only arm for it - such an
# arm silently degenerates into a duplicate of the deferred_row_policy control below.

echo "-- controls: these answers and their plans must not change"
ch "CREATE ROW POLICY ${POLICY} ON t AS RESTRICTIVE FOR SELECT USING (flag = 0) TO ALL"
# Deferred (the apply_row_policy_after_final default): the filter runs after the merge, so key 1
# must stay absent.
run_pair   "deferred_row_policy"           "SELECT arraySort(groupArray(k)) FROM t FINAL" "apply_row_policy_after_final = 1, "
lazy_fires "deferred_row_policy_optimized" "SELECT arraySort(groupArray(k)) FROM t FINAL" "apply_row_policy_after_final = 1, "
# Deferral is tracked separately for the row-level filter and for the prewhere, so a deferred
# PREWHERE needs its own arm: key 1's overall winner has flag = 1, so it must stay absent here while
# the non-deferred `prewhere` arm above surfaces it.
run_pair   "deferred_prewhere"           "SELECT arraySort(groupArray(k)) FROM t FINAL PREWHERE flag = 0" "apply_prewhere_after_final = 1, "
lazy_fires "deferred_prewhere_optimized" "SELECT arraySort(groupArray(k)) FROM t FINAL PREWHERE flag = 0" "apply_prewhere_after_final = 1, "
# min_filtered_ratio_for_lazy_final is a runtime cost gate inside the plan, so at its 0.5 default
# the plan is still built but the lazy branch is not taken at execution time.
echo -e "ratio_default\t$(ch "SELECT arraySort(groupArray(k)) FROM t FINAL SETTINGS apply_row_policy_after_final = 0, query_plan_optimize_lazy_final = 1")\t$(ch "SELECT arraySort(groupArray(k)) FROM t FINAL SETTINGS apply_row_policy_after_final = 0, ${LAZY_OFF}")"
ch "DROP ROW POLICY ${POLICY} ON t"
# A plain WHERE legitimately applies after the merge.
run_pair   "plain_where"           "SELECT arraySort(groupArray(k)) FROM t FINAL WHERE flag = 0" ""
lazy_fires "plain_where_optimized" "SELECT arraySort(groupArray(k)) FROM t FINAL WHERE flag = 0" ""
# A policy over sorting-key columns only is deliberately never deferred, and is dedup-invariant.
ch "CREATE ROW POLICY ${POLICY}_sk ON t_sk AS RESTRICTIVE FOR SELECT USING (sk = 7) TO ALL"
run_pair   "sorting_key_policy"           "SELECT arraySort(groupArray(k)) FROM t_sk FINAL" "apply_row_policy_after_final = 0, "
lazy_fires "sorting_key_policy_optimized" "SELECT arraySort(groupArray(k)) FROM t_sk FINAL" "apply_row_policy_after_final = 0, "
ch "DROP ROW POLICY ${POLICY}_sk ON t_sk"

${CLICKHOUSE_CLIENT} -mn -q "
DROP TABLE t;
DROP TABLE t_sk;
DROP TABLE t_del;
DROP TABLE t_two;
DROP TABLE t_float;
DROP TABLE t_null;
DROP TABLE t_rev;
DROP TABLE t_txt;
DROP TABLE t_fp;
DROP TABLE t_mix;
"
cleanup
