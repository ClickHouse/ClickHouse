#include <Core/Joins.h>
#include <Core/SortDescription.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/optimizeReadInOrder.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/typeid_cast.h>

#include <algorithm>

namespace DB::QueryPlanOptimizations
{

namespace
{
/// Extract `(kind, strictness)` from either JoinStep (physical) or JoinStepLogical
/// (analyzer's logical plan, which is what we typically see in the first pass).
struct JoinSemantics
{
    JoinKind kind;
    JoinStrictness strictness;
};

std::optional<JoinSemantics> getJoinSemanticsFromStep(IQueryPlanStep * step)
{
    if (auto * physical = typeid_cast<JoinStep *>(step))
    {
        if (auto join_ptr = physical->getJoin())
        {
            const auto & table_join = join_ptr->getTableJoin();
            return JoinSemantics{table_join.kind(), table_join.strictness()};
        }
        return {};
    }
    if (auto * logical = typeid_cast<JoinStepLogical *>(step))
    {
        const auto & op = logical->getJoinOperator();
        return JoinSemantics{op.kind, op.strictness};
    }
    return {};
}

/// Return `true` when the eventual physical join could have `hasDelayedBlocks()` that the
/// second pass cannot get rid of, in which case `optimizeReadInOrder`'s second-pass traversal
/// will not propagate read-in-order through the join (see `findReadingStep` in
/// `optimizeReadInOrder.cpp`). Used to gate the deferral: if such delayed blocks are possible
/// the deferral would silently disable both `topKThroughJoin` and the second-pass
/// through-join pass.
///
/// `allow_pinning_spilling_join` mirrors `query_plan_read_in_order_through_spilling_join`. When
/// it is set, a join that reports delayed blocks only because it *might* spill
/// (`SpillingHashJoin`, `canKeepLeftPipelineInOrder() == true`) is not a blocker: the second
/// pass pins it in memory and it then keeps the left order. This is the steady state today,
/// because `max_bytes_ratio_before_external_join` defaults to `0.5` and wraps every hash join
/// in `SpillingHashJoin`.
///
/// For a physical `JoinStep` we read `hasDelayedBlocks()` / `canKeepLeftPipelineInOrder()`
/// directly. For `JoinStepLogical` the algorithm is picked later from
/// `JoinSettings::join_algorithms`, so we conservatively assume unavoidable delayed blocks are
/// possible when the configured settings allow `JoinAlgorithm::GRACE_HASH` (never pinnable),
/// or - unless pinning is allowed - when automatic spilling is configured via
/// `max_bytes_*_before_external_join` (which can wrap the chosen hash join in
/// `SpillingHashJoin`).
///
/// `JoinAlgorithm::AUTO` physicalizes to different joins (see the `AUTO` branch of
/// `tryCreateJoin` in `PlannerJoins.cpp`):
///  - with an effective spill threshold, temporary storage, and `GraceHashJoin::isSupported`,
///    it returns `SpillingHashJoin` - pinnable, so not a blocker when pinning is allowed;
///  - otherwise `JoinSwitcher` when `MergeJoin::isSupported` - a genuine blocker: it may
///    switch to `MergeJoin` at runtime and is not pinnable;
///  - otherwise plain `HashJoin` - no delayed blocks at all.
/// With an effective spill threshold the `JoinSwitcher` outcome is unreachable: every join
/// accepted by `MergeJoin::isSupported` (`ALL` x inner/left/right/full or `ANY`/`SEMI` x
/// inner/left, one disjunct, no mixed condition) is also accepted by
/// `GraceHashJoin::isSupported` (non-`ASOF` inner/left/right/full, one disjunct), so the
/// `SpillingHashJoin` branch fires first. Therefore, when pinning is allowed and
/// `getEffectiveMaxBytesBeforeExternalJoin() > 0` (the exact check the physicalization
/// performs via `JoinAlgorithmParams`), `AUTO` is not a blocker either. The one remaining
/// assumption is that temporary storage is configured (`TableJoin::getTempDataOnDisk`): a
/// server without it makes `AUTO` fall back to `JoinSwitcher`, and the deferral then loses
/// the top-k pushdown - a plan-shape pessimization, not a correctness issue, since nothing
/// gets pinned.
bool joinMayHaveDelayedBlocks(const IQueryPlanStep & step, bool allow_pinning_spilling_join)
{
    if (const auto * physical = typeid_cast<const JoinStep *>(&step))
    {
        const auto & join_ptr = physical->getJoin();
        if (!join_ptr)
            return true;
        if (!join_ptr->hasDelayedBlocks())
            return false;
        return !(allow_pinning_spilling_join && join_ptr->canKeepLeftPipelineInOrder());
    }
    if (const auto * logical = typeid_cast<const JoinStepLogical *>(&step))
    {
        const auto & js = logical->getJoinSettings();
        if (!allow_pinning_spilling_join
            && (js.max_bytes_before_external_join > 0 || js.max_bytes_ratio_before_external_join > 0.0))
            return true;
        const bool auto_is_pinnable_spilling_join
            = allow_pinning_spilling_join && js.getEffectiveMaxBytesBeforeExternalJoin() > 0;
        return std::ranges::any_of(js.join_algorithms, [&](JoinAlgorithm a)
        {
            return a == JoinAlgorithm::GRACE_HASH || (a == JoinAlgorithm::AUTO && !auto_is_pinnable_spilling_join);
        });
    }
    /// Unknown step kind - be conservative.
    return true;
}

/// Return `true` when deferring to `optimizeReadInOrder`'s second-pass through-join
/// traversal would fail because the eventual physical join breaks the read-in-order
/// chain on the preserved side. Two distinct failure modes are covered:
///
///  1. The join does not emit left rows in their original stream order
///     (`preservesLeftBlockOrder() == false`). `findReadingStep` rejects such joins
///     directly. `PartialMergeJoin` (`partial_merge` / `prefer_partial_merge`) is the
///     case here: it re-sorts left blocks by the join key.
///  2. The join preserves left order but its physicalization inserts a `Sort ... before
///     JOIN` on the preserved input (`FullSortingMergeJoin`, `full_sorting_merge`; see
///     `addSortingForMergeJoin` in `JoinStepLogical.cpp`). `findReadingStep` only
///     descends through `Expression`/`Filter`/`ArrayJoin`/preliminary `Distinct`/
///     `CreatingSets` steps, so it stops at that `SortingStep` and never installs
///     `requestReadingInOrder`.
///
/// In both modes deferring would silently disable BOTH `topKThroughJoin` and the second
/// pass, so we must NOT defer and instead let `topKThroughJoin` inject its own
/// `Sort + Limit`.
///
/// For a physical `JoinStep` we read `preservesLeftBlockOrder()` directly and also flag
/// `FullSortingMergeJoin` explicitly (it preserves order but adds the pre-JOIN sort). For
/// `JoinStepLogical` the algorithm is chosen later from `JoinSettings::join_algorithms`,
/// so we conservatively flag any configured `PARTIAL_MERGE` / `PREFER_PARTIAL_MERGE` /
/// `FULL_SORTING_MERGE` / `PARALLEL_FULL_SORTING_MERGE` (the parallel variant
/// physicalizes to the same `FullSortingMergeJoin` with the same pre-JOIN sort).
/// `GRACE_HASH` / `AUTO` are already covered by `joinMayHaveDelayedBlocks`.
///
/// The `JoinStepLogical` check is by *list membership*, so listing a merge join as a
/// lower-priority fallback (e.g. `join_algorithm = 'hash,parallel_full_sorting_merge'`)
/// already disables the deferral even though the selected join is plain `hash`: the
/// query then gets `topKThroughJoin`'s own `Sort + Limit` instead of preserved-side
/// read-in-order. This is a plan-shape pessimization, not a wrong result, and it is
/// exactly the pre-existing behavior of `join_algorithm = 'hash,full_sorting_merge'` -
/// adding the new algorithm to the list introduces no divergence from it. Pinned by
/// `04651_parallel_full_sorting_merge_join_fallback_top_k`; the analogous strict-key
/// fallback side effect is pinned by
/// `04602_parallel_full_sorting_merge_join_fallback_strict_keys`.
/// See issues #110662 and #109216.
bool joinDefeatsReadInOrderThroughJoin(const IQueryPlanStep & step)
{
    if (const auto * physical = typeid_cast<const JoinStep *>(&step))
    {
        const auto & join_ptr = physical->getJoin();
        if (!join_ptr || !join_ptr->preservesLeftBlockOrder())
            return true;
        return typeid_cast<const FullSortingMergeJoin *>(join_ptr.get()) != nullptr;
    }
    if (const auto * logical = typeid_cast<const JoinStepLogical *>(&step))
    {
        return std::ranges::any_of(logical->getJoinSettings().join_algorithms, [](JoinAlgorithm a)
        {
            return a == JoinAlgorithm::PARTIAL_MERGE
                || a == JoinAlgorithm::PREFER_PARTIAL_MERGE
                || a == JoinAlgorithm::FULL_SORTING_MERGE
                || a == JoinAlgorithm::PARALLEL_FULL_SORTING_MERGE;
        });
    }
    /// Unknown step kind - be conservative.
    return true;
}

/// Walk down a single-child chain looking for a `ReadFromMergeTree` step. Used for the
/// `MergeTree`-specific guards below (parallel replicas, `FINAL`); the deferral itself
/// probes every reader `optimizeReadInOrder` supports, not only `MergeTree`.
const ReadFromMergeTree * findMergeTreeRead(const QueryPlan::Node * node)
{
    while (node)
    {
        if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(node->step.get()))
            return reading;
        if (node->children.size() != 1)
            return nullptr;
        node = node->children.front();
    }
    return nullptr;
}

}

/// Push `Limit + Sort` down through a Join when the sort key only references
/// columns from the side preserved by the join (left of LEFT JOIN, right of RIGHT JOIN).
///
/// Soundness sketch
/// ----------------
/// Consider `Limit(n) <- Sort(K) <- Join(L, R)` where `K` only references columns from `L`
/// and the join is `LEFT` (so every L row produces at least one output row).
/// Output rows have K values exclusively drawn from L. The top-n rows by K of the join
/// output are therefore drawn from the rows of L that have the n largest (or smallest)
/// K values - that is, the top-n rows of L by K. Pre-sorting L by K and limiting to n
/// before the join restricts the set of L rows we expand without changing the final
/// top-n result. The outer Sort+Limit is preserved because LEFT JOIN may multiply
/// each L row into several output rows.
///
/// Mirror reasoning applies to RIGHT JOIN with K from R.
///
/// We do not apply this optimization to INNER joins: an L row with no R match produces
/// zero output rows, so limiting L to its top-n by K may cause every L survivor to drop
/// out, leaving fewer than n output rows even when the query has more.
///
/// `SEMI` and `ANTI` strictnesses on `LEFT`/`RIGHT` are also rejected: they break the
/// "every preserved-side row produces at least one output row" invariant by filtering
/// the preserved side based on match/non-match against the other side, so truncating
/// to top-n by K may drop rows that actually survive the join.
///
/// `LIMIT WITH TIES` and `LIMIT` steps with `alwaysReadTillEnd` set (e.g. `WITH TOTALS`,
/// `exact_rows_before_limit`) are also skipped: both require the upstream to keep
/// processing past the limit, which our preserved-side `Limit` would prevent.
///
/// Pattern matched: `LimitStep -> SortingStep -> [ExpressionStep] -> JoinStep`.
/// The optional ExpressionStep is allowed only when every sort key column passes
/// through it unchanged. We verify pass-through at the ActionsDAG level: the output
/// node for the sort column must be either an INPUT or a chain of ALIASes ending at
/// an INPUT. Header-name presence alone is too weak - an output named like an input
/// could still be a computed expression (e.g. `SELECT l.k + r.b AS k ORDER BY k`),
/// and pushing the sort below the join using the input column would change results.
size_t tryTopKThroughJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    auto * limit_step = typeid_cast<LimitStep *>(parent_node->step.get());
    if (!limit_step)
        return 0;

    /// LIMIT WITH TIES needs to know how many rows have the threshold value, so
    /// we cannot stop reading early.
    if (limit_step->withTies())
        return 0;

    /// Skip when `always_read_till_end` is set (e.g. `WITH TOTALS`, `exact_rows_before_limit`).
    /// Truncating the preserved input would make the upstream operator see fewer JOIN rows
    /// than it should, breaking `rows_before_limit_at_least` and totals semantics.
    if (limit_step->alwaysReadTillEnd())
        return 0;

    if (parent_node->children.size() != 1)
        return 0;

    auto * sort_node = parent_node->children.front();
    auto * sort_step = typeid_cast<SortingStep *>(sort_node->step.get());
    if (!sort_step)
        return 0;

    /// Only Full sort is meaningful here. PartialSorting/MergingSorted indicate
    /// the input was already sorted, in which case there is nothing to push down.
    if (sort_step->getType() != SortingStep::Type::Full)
        return 0;

    if (sort_node->children.size() != 1)
        return 0;

    /// Peel a chain of ExpressionSteps between Sort and Join, translating the sort
    /// description to the input level of each step. For each sort column we look up
    /// the output node by name and walk through any `ALIAS` chain - if it ends at an
    /// `INPUT` node, the column is a pure pass-through and we replace its name with
    /// the input's name. Anything else (FUNCTION, COLUMN, ARRAY_JOIN, ...) means the
    /// sort key was computed in this step rather than carried over, and pushing the
    /// sort below the join would be unsound.
    ///
    /// The cap of 4 is generous: in current plans the only steps between Sort and
    /// Join after `mergeExpressions` are `Before ORDER BY + Projection` and
    /// `Post Join Actions`, occasionally with one more wrapper.
    SortDescription description = sort_step->getSortDescription();
    QueryPlan::Node * join_node = sort_node->children.front();
    for (size_t peeled = 0; peeled < 4; ++peeled)
    {
        auto * expression_step = typeid_cast<ExpressionStep *>(join_node->step.get());
        if (!expression_step)
            break;
        if (join_node->children.size() != 1)
            return 0;

        const ActionsDAG & dag = expression_step->getExpression();
        /// `arrayJoin` between `Sort` and `Join` changes the number of output rows
        /// per input row. The soundness sketch assumes "every preserved-side row
        /// produces at least one output row" and that the top-n of the sort output
        /// corresponds to the top-n of the preserved side - both invariants break
        /// when an `arrayJoin` above the join expands rows, because the n rows we
        /// keep on the preserved side may expand into fewer (or zero) final rows.
        /// See `#82279`.
        if (dag.hasArrayJoin())
            return 0;
        for (auto & sort_col : description)
        {
            const auto * out_node = dag.tryFindInOutputs(sort_col.column_name);
            if (!out_node)
                return 0;

            while (out_node->type == ActionsDAG::ActionType::ALIAS)
                out_node = out_node->children.front();

            if (out_node->type != ActionsDAG::ActionType::INPUT)
                return 0;

            sort_col.column_name = out_node->result_name;
        }
        join_node = join_node->children.front();
    }

    auto join_semantics_opt = getJoinSemanticsFromStep(join_node->step.get());
    if (!join_semantics_opt)
        return 0;
    if (join_node->children.size() != 2)
        return 0;

    const JoinKind join_kind = join_semantics_opt->kind;
    const JoinStrictness join_strictness = join_semantics_opt->strictness;

    size_t preserved_idx = 0;
    if (join_kind == JoinKind::Left)
        preserved_idx = 0;
    else if (join_kind == JoinKind::Right)
        preserved_idx = 1;
    else
        return 0;

    /// `SEMI` and `ANTI` strictnesses do not preserve the "every row from the preserved
    /// side produces at least one output row" invariant the soundness sketch relies on:
    /// `LEFT SEMI` drops unmatched preserved-side rows, `LEFT ANTI` drops matched ones
    /// (mirrored for `RIGHT`). Truncating the preserved input to its top-n by sort key
    /// could discard rows that survive the join while keeping rows that get filtered out,
    /// changing the final top-n result.
    if (join_strictness == JoinStrictness::Semi || join_strictness == JoinStrictness::Anti)
        return 0;

    const auto & preserved_input_header = join_node->step->getInputHeaders().at(preserved_idx);
    const auto & other_input_header = join_node->step->getInputHeaders().at(1 - preserved_idx);

    /// All sort columns must be in the preserved side's input header, by the (now
    /// translated) name. Other names that may appear in the join output (right-side
    /// columns of a LEFT JOIN, etc.) come from the non-preserved side and would make
    /// the transformation unsound. We additionally require the column to NOT also
    /// appear on the other side: if both inputs carry a column with this name the
    /// analyzer would have renamed one, but defensively avoid ambiguity.
    for (const auto & sort_col : description)
    {
        if (!preserved_input_header->has(sort_col.column_name))
            return 0;
        if (other_input_header->has(sort_col.column_name))
            return 0;
    }

    /// `n` is the maximum number of L rows we need to consider on the preserved side.
    /// Any output row we keep after the outer LIMIT has its sort-key value drawn from
    /// one of the top-(limit+offset) L rows.
    const size_t n = limit_step->getLimitForSorting();
    if (n == 0)
        return 0;

    /// Reuse the cap that already gates `tryOptimizeTopK`. If the user disabled
    /// large-N TopK optimization there, do not work around it here.
    if (settings.max_limit_for_top_k_optimization && n > settings.max_limit_for_top_k_optimization)
        return 0;

    QueryPlan::Node * preserved_input_node = join_node->children.at(preserved_idx);

    /// Avoid re-applying: if the immediate child is already a LimitStep with a
    /// limit no larger than `n`, the optimization has already fired (or there is a
    /// user-supplied LIMIT we should not weaken).
    if (auto * existing_limit = typeid_cast<LimitStep *>(preserved_input_node->step.get()))
    {
        if (existing_limit->getLimit() <= n && existing_limit->getOffset() == 0)
            return 0;
    }

    /// Do not push `Sort + Limit` below the join when the preserved input is read with
    /// parallel replicas. Each replica reads a coordinated subset of rows; per-replica
    /// `Limit n` after a per-replica sort would emit each replica's local top-n instead
    /// of the global top-n. Furthermore, the inserted `Sort` would let `optimizeReadInOrder`
    /// (which has no through-join guard once the join is no longer between sort and read)
    /// turn the preserved-side scan into `WithOrder` mode, conflicting with the existing
    /// `read_in_order_through_join` skip for parallel replicas and causing coordination
    /// mode mismatch ("Replica decided to read in Default mode, not in WithOrder").
    if (const auto * reading = findMergeTreeRead(preserved_input_node))
    {
        if (reading->isParallelReadingFromReplicas())
            return 0;
    }

    /// Defer to `optimizeReadInOrder` (second-pass) when the preserved input can stream
    /// rows in the requested sort order from the storage's sorting key. That path scans
    /// only the rows the LIMIT will keep, without materializing a sort - strictly better
    /// than what we would do here. This mirrors the soundness sketch in the file header
    /// without the cost of an explicit Sort + Limit on top of the storage step.
    ///
    /// The second pass only traverses INNER/LEFT joins via the left child with ANY/ALL
    /// strictness (see `optimizeReadInOrder`); for `RIGHT` joins or non-ANY/ALL strictness
    /// it would not pick the read-in-order through the join, so deferring would silently
    /// disable both optimizations. Likewise, both `read_in_order` and
    /// `read_in_order_through_join` must be enabled for the second pass to apply at all.
    ///
    /// We also require `join_swap_table` to be explicitly `false`. The kind and strictness
    /// we see now are from the logical (or not-yet-optimized physical) join; `optimizeJoinLegacy`
    /// runs later and can swap `LEFT` to `RIGHT` (via `TableJoin::swapSides`) when the swap is
    /// allowed by setting and the left side is smaller. After the swap, `optimizeReadInOrder`
    /// rejects the join (`isInnerOrLeft(JoinKind::Right)` is false), so deferring would silently
    /// disable both optimizations. Only when the user (or test harness) has pinned the setting
    /// off is the join side stable enough to commit to the deferral.
    ///
    /// We additionally require that the eventual physical join cannot have delayed blocks
    /// (`GraceHashJoin`, legacy `JoinSwitcher`). `optimizeReadInOrder`'s join traversal also
    /// rejects those (`findReadingStep`), so deferring when such an algorithm is possible
    /// would silently disable both optimizations whenever the planner picks one.
    /// `SpillingHashJoin` is the exception: it reports delayed blocks only because it *might*
    /// spill, and the second pass makes it keep the left order by pinning it in memory, so it
    /// blocks the deferral only when `query_plan_read_in_order_through_spilling_join` forbids
    /// that pinning. This matters for the steady state, because
    /// `max_bytes_ratio_before_external_join` defaults to `0.5` and wraps every hash join in
    /// `SpillingHashJoin`.
    ///
    /// Finally, the eventual physical join must keep the read-in-order chain intact on the
    /// preserved side. Two merge-join algorithms break it: `partial_merge` builds a
    /// `PartialMergeJoin` that re-sorts left blocks (`preservesLeftBlockOrder() == false`,
    /// rejected directly by `findReadingStep`), and `full_sorting_merge` builds a
    /// `FullSortingMergeJoin` whose physicalization inserts a `Sort ... before JOIN` on the
    /// preserved input (`addSortingForMergeJoin`), which `findReadingStep` will not descend
    /// through. In either case deferring would silently disable both optimizations.
    /// See issues #110662 and #109216.
    const bool second_pass_can_apply
        = settings.read_in_order
        && settings.read_in_order_through_join
        && settings.join_swap_table.has_value() && !settings.join_swap_table.value()
        && join_kind == JoinKind::Left
        && (join_strictness == JoinStrictness::All || join_strictness == JoinStrictness::Any)
        && !joinMayHaveDelayedBlocks(*join_node->step, settings.read_in_order_through_spilling_join)
        && !joinDefeatsReadInOrderThroughJoin(*join_node->step);
    if (second_pass_can_apply)
    {
        /// Probe full read-in-order applicability (direction, nulls direction, collator,
        /// key-expression mapping) rather than just matching column names. A name-only match
        /// defers even when `optimizeReadInOrder` cannot actually satisfy the `SortingStep`
        /// (e.g. `ORDER BY ... COLLATE`), which would silently disable both optimizations.
        ///
        /// The probe covers every reading step pass 2 supports - `ReadFromMergeTree`,
        /// `ReadFromMerge` and `ReadFromObjectStorageStep` - because
        /// `buildInputOrderInfo(SortingStep &, ...)` installs the through-`JOIN` read-in-order
        /// plan for all three. Probing only `ReadFromMergeTree` would keep the pushed-down
        /// `Sort` + `Limit` for a preserved side reading from a `Merge` table or from object
        /// storage even though pass 2 could have streamed it in order. The probe also mirrors
        /// each reader's `requestReadingInOrder` rejections (reverse order with `FINAL` for
        /// `MergeTree` and `Merge`, unsorted configuration for object storage), so a positive
        /// probe means pass 2 will actually commit, not merely that the sort keys match.
        SortingStep probe_sort_step(
            preserved_input_node->step->getOutputHeader(),
            description,
            n,
            sort_step->getSettings());
        const bool read_in_order_useful = wouldReadInOrderBeUseful(
            probe_sort_step,
            *preserved_input_node,
            settings.read_in_order_through_join,
            settings.read_in_order_through_spilling_join);

        if (read_in_order_useful)
            return 0;
    }

    /// Build `Limit(n) <- Sort(K, limit=n)` and graft it on top of the preserved input.
    auto new_sort_step = std::make_unique<SortingStep>(
        preserved_input_header,
        description,
        n,
        sort_step->getSettings());

    auto & new_sort_node = nodes.emplace_back();
    new_sort_node.children.push_back(preserved_input_node);
    new_sort_node.step = std::move(new_sort_step);

    auto new_limit_step = std::make_unique<LimitStep>(
        new_sort_node.step->getOutputHeader(),
        n,
        /*offset_=*/ 0);

    auto & new_limit_node = nodes.emplace_back();
    new_limit_node.children.push_back(&new_sort_node);
    new_limit_node.step = std::move(new_limit_step);

    join_node->children[preserved_idx] = &new_limit_node;

    /// Re-run optimizations on the modified subtree so the inserted Sort+Limit can
    /// be picked up by tryOptimizeTopK / tryPushDownLimit etc.
    return 3;
}

}
