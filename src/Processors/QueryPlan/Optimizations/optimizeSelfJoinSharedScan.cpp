#include <algorithm>

#include <Common/typeid_cast.h>
#include <Core/Joins.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/CommonSubplanStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

struct ScanDescend
{
    QueryPlan::Node * rmt_parent;
    QueryPlan::Node * rmt_node;

    ReadFromMergeTree * rmt_step() const { return typeid_cast<ReadFromMergeTree *>(rmt_node->step.get()); }
};

std::optional<ScanDescend> findReadFromMergeTree(QueryPlan::Node * node)
{
    QueryPlan::Node * parent = nullptr;
    QueryPlan::Node * current = node;
    while (true)
    {
        if (typeid_cast<ReadFromMergeTree *>(current->step.get()))
            return ScanDescend{parent, current};

        if (!typeid_cast<ExpressionStep *>(current->step.get()) || current->children.size() != 1)
            return std::nullopt;

        parent = current;
        current = current->children[0];
    }
}

bool isPlainScan(const ReadFromMergeTree * rmt)
{
    return !rmt->isQueryWithFinal()
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

    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step || node.children.size() != 2)
        return;

    const auto & join_op = join_step->getJoinOperator();
    if (join_op.kind != JoinKind::Inner && join_op.kind != JoinKind::Left)
        return;
    if (join_op.strictness != JoinStrictness::All)
        return;

    const auto & join_algorithms = join_step->getJoinSettings().join_algorithms;
    if (std::ranges::none_of(join_algorithms, [](auto algo) { return algo == JoinAlgorithm::HASH || algo == JoinAlgorithm::PARALLEL_HASH; }))
        return;

    auto left_scan = findReadFromMergeTree(node.children[0]);
    auto right_scan = findReadFromMergeTree(node.children[1]);
    if (!left_scan || !right_scan)
        return;

    auto * rmt_l = left_scan->rmt_step();
    auto * rmt_r = right_scan->rmt_step();

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

    auto & mutable_join_algorithms = join_step->getJoinSettings().join_algorithms;
    std::erase_if(mutable_join_algorithms, [](auto algo) { return algo != JoinAlgorithm::HASH && algo != JoinAlgorithm::PARALLEL_HASH; });
    join_step->setOptimized();
}

}
