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

        /// `rowNumberInAllBlocks` and friends depend on the runtime stream, which the rewrite
        /// replaces with a replay of the build-side scan.
        if (dagContainsNonDeterministicFunction(expression_step->getExpression()))
            return std::nullopt;

        parent = current;
        current = current->children[0];
    }
}

bool isPlainScan(const ReadFromMergeTree * rmt)
{
    /// `getInputOrder` matters because replaying the probe scan emits the build scan's order,
    /// silently breaking the sorting-key order downstream steps may rely on without an explicit
    /// sort. `optimizeReadInOrder` runs after this pass, so it is normally still unset here.
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

    /// The steps introduced below carry runtime state and do not support plan serialization, so
    /// the plan would fail `assertFragmentSerializable`.
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

    /// The probe side replays a buffer the build side fills, so the join must be producer-first
    /// (`FillRightFirst`). `chooseJoinAlgorithm` executes the first entry of `join_algorithms`
    /// that applies, so walk the same list and prove whichever entry wins is producer-first.
    /// Spilling is irrelevant: `SpillingHashJoin` does not override `pipelineType`.
    const auto & join_settings = join_step->getJoinSettings();

    bool producer_first_guaranteed = false;
    for (const auto algo : join_settings.join_algorithms)
    {
        /// Requires a key-value build storage, so it can never win against a `MergeTree` scan.
        if (algo == JoinAlgorithm::DIRECT)
            continue;

        if (algo == JoinAlgorithm::HASH || algo == JoinAlgorithm::PARALLEL_HASH || algo == JoinAlgorithm::DEFAULT)
        {
            producer_first_guaranteed = true;
            break;
        }

        /// `full_sorting_merge` and `paste` are `YShaped` and read both sides concurrently.
        /// `grace_hash` and `auto` re-read the build side across bucket passes, which the rewrite
        /// has not been validated against.
        return;
    }
    if (!producer_first_guaranteed)
        return;

    /// `break` stops consuming the build side at the limit, so the probe side would replay a
    /// truncated prefix rather than the full stream.
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
    /// Pointer equality, not matching metadata: with `enable_shared_storage_snapshot_in_query = 0`
    /// the two scans may observe different part sets.
    if (rmt_l->getStorageSnapshot() != rmt_r->getStorageSnapshot())
        return;
    if (!isPlainScan(rmt_l) || !isPlainScan(rmt_r))
        return;

    /// The probe side can only see columns the build side saved.
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
