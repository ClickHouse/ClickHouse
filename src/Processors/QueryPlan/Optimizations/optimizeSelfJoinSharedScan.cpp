#include <algorithm>

#include <Common/typeid_cast.h>
#include <Core/Joins.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/CommonSubplanStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

struct ScanDescend
{
    QueryPlan::Node * rmt_parent;
    QueryPlan::Node * rmt_node;

    ReadFromMergeTree * getReadFromMergeTreeStep() const { return typeid_cast<ReadFromMergeTree *>(rmt_node->step.get()); }
};

std::optional<ScanDescend> findReadFromMergeTree(QueryPlan::Node * node)
{
    QueryPlan::Node * parent = nullptr;
    QueryPlan::Node * current = node;
    while (true)
    {
        if (typeid_cast<ReadFromMergeTree *>(current->step.get()))
            return ScanDescend{parent, current};

        const auto * expression_step = typeid_cast<ExpressionStep *>(current->step.get());
        if (!expression_step || current->children.size() != 1)
            return std::nullopt;

        /// Functions like `rowNumberInAllBlocks` or `blockNumber` depend on the runtime stream,
        /// which the rewrite replaces with a replay of the build-side scan.
        if (dagContainsNonDeterministicFunction(expression_step->getExpression()))
            return std::nullopt;

        parent = current;
        current = current->children[0];
    }
}

bool isPlainScan(const ReadFromMergeTree * rmt)
{
    /// A `STREAM` scan is unbounded and keeps producing newly committed rows;
    /// buffering it or replaying a one-shot buffer instead would change semantics.
    ///
    /// An in-order reading contract (from `optimizeReadInOrder`, including
    /// `read_in_order_through_join`) makes the scan emit rows in sorting-key order, which downstream
    /// steps may rely on without an explicit sort. Replaying the probe scan from a buffer filled by
    /// the build scan would emit the build scan's order instead, silently breaking that contract.
    /// `optimizeReadInOrder` runs after this rewrite so `input_order_info` is normally still unset
    /// here; this guard keeps the invariant explicit and robust to pass reordering.
    return !rmt->isQueryWithFinal()
        && rmt->getInputOrder() == nullptr
        && !rmt->getQueryInfo().isStream()
        && !rmt->isQueryWithSampling()
        && !rmt->isParallelReadingFromReplicas()
        && rmt->getFilterActionsDAG() == nullptr
        && rmt->getPrewhereInfo() == nullptr
        && rmt->getRowLevelFilter() == nullptr
        && rmt->getDeferredRowLevelFilter() == nullptr
        && rmt->getDeferredPrewhereInfo() == nullptr
        && !rmt->getVectorSearchParameters().has_value()
        && !rmt->isSelectedForTopKFilterOptimization()
        && !rmt->willOutputEachPartitionThroughSeparatePort();
}

}

void tryOptimizeSelfJoinSharedScan(
    QueryPlan::Node & node,
    QueryPlan::Nodes & nodes,
    const QueryPlanOptimizationSettings & settings)
{
    if (!settings.optimize_self_join_shared_scan)
        return;

    /// The rewrite introduces `CommonSubplanStep` / `CommonSubplanReferenceStep` (later lowered to
    /// buffer steps) that carry runtime state and do not support plan serialization, so under
    /// distributed planning the plan would fail `assertFragmentSerializable`.
    if (settings.make_distributed_plan)
        return;

    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step || node.children.size() != 2)
        return;

    const auto & join_op = join_step->getJoinOperator();
    if (join_op.kind != JoinKind::Inner && join_op.kind != JoinKind::Left)
        return;
    if (join_op.strictness != JoinStrictness::All)
        return;

    /// The rewrite is valid only when the join is executed with a producer-first
    /// (`FillRightFirst`) pipeline: the build side is fully consumed before the probe side is
    /// read, so the probe scan can replay the buffer filled by the build scan.
    ///
    /// Note that whether the join spills is irrelevant here: `SpillingHashJoin` does not override
    /// `pipelineType`, so it is `FillRightFirst` like the rest of the hash family and still drains
    /// the build side completely before the probe side is read.
    ///
    /// `chooseJoinAlgorithm` walks `join_algorithms` in order and executes the first algorithm
    /// that applies, so walk the same list and prove that whichever entry wins is producer-first.
    /// Otherwise (e.g. `join_algorithm = 'full_sorting_merge,hash'`) skip the rewrite: the user's
    /// algorithm list must keep selecting the same algorithm it would select without the rewrite.
    const auto & join_settings = join_step->getJoinSettings();

    bool producer_first_guaranteed = false;
    for (const auto algo : join_settings.join_algorithms)
    {
        /// Never applies here: a direct join requires a key-value build storage, while this
        /// rewrite requires a plain `MergeTree` scan on the build side.
        if (algo == JoinAlgorithm::DIRECT)
            continue;

        /// These always create a producer-first join, so no later entry can win: the hash family
        /// and `default`, which means `direct,hash`.
        if (algo == JoinAlgorithm::HASH || algo == JoinAlgorithm::PARALLEL_HASH || algo == JoinAlgorithm::DEFAULT)
        {
            producer_first_guaranteed = true;
            break;
        }

        /// Anything else may win. `full_sorting_merge` and `paste` are `YShaped`, which reads both
        /// sides concurrently and so cannot replay a buffer the build side has not finished filling.
        /// `grace_hash` and `auto` are producer-first, but re-read the build side from their own
        /// spill files across multiple bucket passes; the rewrite has not been validated against
        /// that replay, so exclude them until it is.
        return;
    }
    if (!producer_first_guaranteed)
        return;

    /// Under `join_overflow_mode = 'break'` the build side stops consuming its input once
    /// `max_rows_in_join` / `max_bytes_in_join` is reached, so the shared buffer would hold only a
    /// prefix of the scan. The probe side would then replay that truncated prefix instead of the
    /// full stream, losing rows beyond what the join's soft limit is allowed to drop (e.g. the
    /// preserved side of a LEFT JOIN).
    if (join_settings.join_overflow_mode == OverflowMode::BREAK
        && (join_settings.max_rows_in_join != 0 || join_settings.max_bytes_in_join != 0))
        return;

    auto left_scan = findReadFromMergeTree(node.children[0]);
    auto right_scan = findReadFromMergeTree(node.children[1]);
    if (!left_scan || !right_scan)
        return;

    auto * rmt_l = left_scan->getReadFromMergeTreeStep();
    auto * rmt_r = right_scan->getReadFromMergeTreeStep();

    if (rmt_l->getStorageID().uuid != rmt_r->getStorageID().uuid)
        return;
    /// Require the exact same StorageSnapshot (pointer equality), not just matching metadata.
    /// With `enable_shared_storage_snapshot_in_query = 0` the two scans may otherwise observe
    /// different part sets, and forcing them through a single shared buffer would change
    /// query semantics under concurrent part changes.
    if (rmt_l->getStorageSnapshot() != rmt_r->getStorageSnapshot())
        return;
    if (!isPlainScan(rmt_l) || !isPlainScan(rmt_r))
        return;

    /// The probe side replays from a buffer filled by the build side, so it can only see columns
    /// that the build side actually scanned and saved.
    const auto & rmt_r_columns = rmt_r->getAllColumnNames();
    const auto & rmt_l_columns = rmt_l->getAllColumnNames();
    for (const auto & col : rmt_l_columns)
        if (std::find(rmt_r_columns.begin(), rmt_r_columns.end(), col) == rmt_r_columns.end())
            return;

    auto & subplan_node = nodes.emplace_back();
    subplan_node.step = std::make_unique<CommonSubplanStep>(
        right_scan->rmt_node->step->getOutputHeader());
    subplan_node.children = {right_scan->rmt_node};

    if (right_scan->rmt_parent)
        right_scan->rmt_parent->children[0] = &subplan_node;
    else
        node.children[1] = &subplan_node;

    auto & ref_node = nodes.emplace_back();
    ref_node.step = std::make_unique<CommonSubplanReferenceStep>(
        left_scan->rmt_node->step->getOutputHeader(),
        &subplan_node,
        ColumnIdentifiers(rmt_l_columns));

    if (left_scan->rmt_parent)
        left_scan->rmt_parent->children[0] = &ref_node;
    else
        node.children[0] = &ref_node;
}

}
