#include <Storages/MergeTree/MergeTreeFinalByValidation.h>

#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Validate FINAL BY expressions against the sorting key.
/// `final_by_dag` is the ActionsDAG built from FINAL BY expressions (by the Planner).
/// `final_by_count` is the number of FINAL BY expressions; the first `final_by_count`
/// outputs of `final_by_dag` correspond to the expressions (remaining outputs are
/// pass-through INPUT nodes).
FinalByValidationResult validateFinalBy(
    ActionsDAG final_by_dag,
    size_t final_by_count,
    const StorageSnapshotPtr & storage_snapshot,
    MergeTreeData::MergingParams::Mode merging_mode)
{
    if (merging_mode != MergeTreeData::MergingParams::Aggregating
        && merging_mode != MergeTreeData::MergingParams::Summing)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "FINAL BY is only supported for AggregatingMergeTree and SummingMergeTree engines");
    }

    const auto & sorting_key = storage_snapshot->metadata->getSortingKey();

    if (final_by_count != sorting_key.column_names.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "FINAL BY has {} expressions but sorting key has {} columns",
            final_by_count, sorting_key.column_names.size());

    /// Match FINAL BY outputs against sorting key DAG outputs.
    const auto & sorting_key_dag = sorting_key.expression->getActionsDAG();
    auto matches = matchTrees(sorting_key_dag.getOutputs(), final_by_dag);

    const auto & fb_outputs = final_by_dag.getOutputs();

    std::vector<SortColumnDescription> final_by_sort_columns;
    final_by_sort_columns.reserve(final_by_count);

    bool has_non_identity = false;

    for (size_t i = 0; i < final_by_count; ++i)
    {
        /// The first `final_by_count` outputs correspond to the FINAL BY expressions
        /// (in order); remaining outputs are pass-through INPUT nodes added by the Planner.
        const ActionsDAG::Node * fb_node = fb_outputs[i];

        auto match_it = matches.find(fb_node);
        if (match_it == matches.end() || !match_it->second.node)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "FINAL BY expression {} ('{}') does not match any sorting key column",
                i + 1, fb_node->result_name);
        }

        const auto & match = match_it->second;

        /// Find which sorting key column this match points to.
        /// We compare by name against `sorting_key.column_names` rather than
        /// iterating `sorting_key_dag.getOutputs()`, because the DAG outputs
        /// include source/input columns in addition to computed key columns
        /// (since `getActions(false)` preserves inputs), so DAG output indices
        /// do not correspond to key column indices.
        size_t matched_sk_idx = SIZE_MAX;
        for (size_t sk = 0; sk < sorting_key.column_names.size(); ++sk)
        {
            if (sorting_key.column_names[sk] == match.node->result_name)
            {
                matched_sk_idx = sk;
                break;
            }
        }

        if (matched_sk_idx != i)
        {
            if (matched_sk_idx == SIZE_MAX)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "FINAL BY expression {} ('{}') does not correspond to sorting key column {} ('{}')",
                    i + 1, fb_node->result_name, i + 1, sorting_key.column_names[i]);
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "FINAL BY expression {} ('{}') matches sorting key column {} ('{}'), expected column {} ('{}')",
                    i + 1, fb_node->result_name,
                    matched_sk_idx + 1, sorting_key.column_names[matched_sk_idx],
                    i + 1, sorting_key.column_names[i]);
        }

        /// Check monotonicity direction: must not flip.
        if (match.monotonicity && match.monotonicity->direction != 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "FINAL BY expression {} ('{}') reverses the sort direction of sorting key column '{}', "
                "which is not allowed",
                i + 1, fb_node->result_name, sorting_key.column_names[i]);
        }

        /// Track whether any expression is not identity (has a monotonic wrapper).
        if (match.monotonicity)
            has_non_identity = true;

        /// Direction inherited from sorting key.
        int sk_direction = (!sorting_key.reverse_flags.empty() && sorting_key.reverse_flags[i]) ? -1 : 1;

        /// For identity matches (no monotonic wrapper), use the sorting key column name
        /// instead of the FINAL BY DAG node name.  The new analyzer may generate names
        /// with type suffixes (e.g. `intDiv(x, 100_UInt8)`) that differ from the sorting
        /// key column name (`intDiv(x, 100)`).  Since identity means no ExpressionTransform
        /// is applied, the block columns keep the sorting key names.
        const String & sort_col_name = match.monotonicity
            ? fb_node->result_name
            : sorting_key.column_names[i];
        final_by_sort_columns.emplace_back(sort_col_name, sk_direction);
    }

    return FinalByValidationResult{
        .dag = std::move(final_by_dag),
        .sort_columns = std::move(final_by_sort_columns),
        .has_non_identity = has_non_identity,
    };
}

}
