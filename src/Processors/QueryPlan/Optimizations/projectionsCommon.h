#pragma once
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

namespace DB
{

struct ProjectionDescription;
class MergeTreeDataSelectExecutor;

}

namespace DB::QueryPlanOptimizations
{

/// Common checks that projection can be used for this step.
bool canUseProjectionForReadingStep(ReadFromMergeTree * reading);

/// Max blocks for sequential consistency reading from replicated table.
PartitionIdToMaxBlockPtr getMaxAddedBlocks(ReadFromMergeTree * reading);

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

    /// Estimated total marks to read (including parent and projection)
    size_t sum_marks = 0;

    /// Number of parts, marks, and ranges selected during projection read
    size_t selected_parts = 0;
    size_t selected_marks = 0;
    size_t selected_ranges = 0;
    size_t selected_rows = 0;

    /// Number of parent parts fully pruned by this projection
    size_t filtered_parts = 0;

    /// If applicable, pointing to projection stats for EXPLAIN projections = 1 introspection
    ReadFromMergeTree::ProjectionStat * stat = nullptr;

    /// Analysis result, separate for parts with and without projection.
    /// Analysis is done in order to estimate the number of marks we are going to read.
    /// For chosen projection, it is reused for reading step.
    ReadFromMergeTree::AnalysisResultPtr merge_tree_projection_select_result_ptr;

    /// Parent parts that need to be read due to missing this projection
    std::unordered_set<const IMergeTreeDataPart *> parent_parts;
};

/// Removes parts not in valid_parts from reading_select_result and updates related counters.
/// Returns the number of parts removed.
size_t filterPartsByProjection(
    ReadFromMergeTree::AnalysisResult & reading_select_result, const std::unordered_set<const IMergeTreeDataPart *> & valid_parts);

/// This function fills ProjectionCandidate structure for specified projection.
/// It returns false if for some reason we cannot read from projection.
bool analyzeProjectionCandidate(
    ProjectionCandidate & candidate,
    const MergeTreeDataSelectExecutor & reader,
    MergeTreeData::MutationsSnapshotPtr empty_mutations_snapshot,
    const Names & required_column_names,
    ReadFromMergeTree::AnalysisResult & parent_reading_select_result,
    const SelectQueryInfo & projection_query_info,
    const ContextPtr & context);

/// Attempts to filter out data parts using projections.
void filterPartsUsingProjection(
    const ProjectionDescription & projection,
    const MergeTreeDataSelectExecutor & reader,
    MergeTreeData::MutationsSnapshotPtr empty_mutations_snapshot,
    ReadFromMergeTree::AnalysisResult & parent_reading_select_result,
    const SelectQueryInfo & projection_query_info,
    const ContextPtr & context);

}
