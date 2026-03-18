#pragma once
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

namespace DB
{

using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;
struct ProjectionDescription;
class MergeTreeDataSelectExecutor;

}

namespace DB::QueryPlanOptimizations
{

/// Common checks that projection can be used for this step.
bool canUseProjectionForReadingStep(ReadFromMergeTree * reading);

/// Max blocks for sequential consistency reading from replicated table.
std::shared_ptr<PartitionIdToMaxBlock> getMaxAddedBlocks(ReadFromMergeTree * reading);

/// This is a common DAG which is a merge of DAGs from Filter and Expression steps chain.
/// Additionally, for all the Filter steps, we collect filter conditions into filter_nodes.
struct QueryDAG
{
    std::optional<ActionsDAG> dag;
    const ActionsDAG::Node * filter_node = nullptr;

    bool build(QueryPlan::Node & node);

private:
    bool buildImpl(QueryPlan::Node & node, ActionsDAG::NodeRawConstPtrs & filter_nodes);
    void appendExpression(const ActionsDAG & expression);
};

struct ProjectionCandidate
{
    const ProjectionDescription * projection;

    /// The number of marks we are going to read
    size_t sum_marks = 0;

    /// Analysis result, separate for parts with and without projection.
    /// Analysis is done in order to estimate the number of marks we are going to read.
    /// For chosen projection, it is reused for reading step.
    ReadFromMergeTree::AnalysisResultPtr merge_tree_projection_select_result_ptr;
    ReadFromMergeTree::AnalysisResultPtr merge_tree_ordinary_select_result_ptr;
};

/// This function fills ProjectionCandidate structure for specified projection.
/// It returns false if for some reason we cannot read from projection.
bool analyzeProjectionCandidate(
    ProjectionCandidate & candidate,
    const ReadFromMergeTree & reading,
    const MergeTreeDataSelectExecutor & reader,
    const Names & required_column_names,
    const RangesInDataParts & parts_with_ranges,
    const SelectQueryInfo & query_info,
    const ContextPtr & context,
    const std::shared_ptr<PartitionIdToMaxBlock> & max_added_blocks,
    const ActionsDAG * dag);

}
