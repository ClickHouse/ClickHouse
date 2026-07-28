#include <Storages/MergeTree/StorageFromMergeTreeProjection.h>

#include <Access/Common/AccessFlags.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/EnabledRowPolicies.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/Sources/NullSource.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
}

StorageFromMergeTreeProjection::StorageFromMergeTreeProjection(
    StorageID storage_id_, StoragePtr parent_storage_, StorageMetadataPtr parent_metadata_, ProjectionDescriptionRawPtr projection_)
    : IStorage(storage_id_)
    , parent_storage(std::move(parent_storage_))
    , merge_tree(dynamic_cast<const MergeTreeData &>(*parent_storage))
    , parent_metadata(std::move(parent_metadata_))
    , projection(projection_)
{
    setInMemoryMetadata(*projection->metadata);
}

void StorageFromMergeTreeProjection::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    const auto parent_storage_id = parent_storage->getStorageID();
    context->checkAccess(AccessType::SELECT, parent_storage_id);

    /// Row policies are attached to the parent table, not to this synthetic projection storage, so the
    /// planner never applies them here; a projection part stores real row data, so enforce the policy.
    auto row_policy_filter = context->getRowPolicyFilter(
        parent_storage_id.getDatabaseName(), parent_storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);

    if (row_policy_filter && !row_policy_filter->isAlwaysTrue())
    {
        const auto & projection_columns = projection->metadata->getColumns();

        RequiredSourceColumnsVisitor::Data columns_context;
        RequiredSourceColumnsVisitor(columns_context).visit(row_policy_filter->expression);
        for (const auto & column_name : columns_context.requiredColumns())
        {
            if (!projection_columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column_name))
                throw Exception(ErrorCodes::ACCESS_DENIED,
                    "Cannot read from projection `{}` of table {} because a row policy references "
                    "column `{}`, which is not stored in the projection, so the row policy cannot be enforced",
                    projection->name, parent_storage_id.getNameForLogs(), column_name);
        }

        /// Build the filter against the projection's own columns; the reading step then reads the
        /// referenced columns, filters the rows, and drops any of them that were not requested.
        ASTPtr expr = row_policy_filter->expression->clone();
        auto syntax_result = TreeRewriter(context).analyze(expr, projection_columns.getAll());
        ExpressionAnalyzer analyzer(expr, syntax_result, context);
        auto filter_actions_dag = analyzer.getActionsDAG(false /* add_aliases */, false /* remove_unused_result */);

        /// The filter column is the single output the expression adds on top of its inputs.
        ExpressionActions filter_actions(filter_actions_dag.clone(), ExpressionActionsSettings(context));
        NamesAndTypesList added;
        NamesAndTypesList deleted;
        filter_actions.getSampleBlock().getNamesAndTypesList().getDifference(
            filter_actions.getRequiredColumnsWithTypes(), added, deleted);
        if (!deleted.empty() || added.size() != 1)
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "Cannot determine row policy filter column for projection `{}` of table {}",
                projection->name, parent_storage_id.getNameForLogs());

        query_info.row_level_filter = std::make_shared<FilterDAGInfo>(
            FilterDAGInfo{std::move(filter_actions_dag), added.front().name, true /* do_remove_column */});
    }

    /// A UNIQUE KEY parent rejects projection reads in the MergeTreeDataSelectExecutor
    /// constructor below (the universal projection-read chokepoint), since reading a
    /// projection part bypasses the parent's delete-bitmap filter.

    const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data);
    const auto & parts = snapshot_data.parts;

    RangesInDataParts projection_parts;
    for (const auto & part : *parts)
    {
        const auto & created_projections = part.data_part->getProjectionParts();
        auto it = created_projections.find(projection->name);
        if (it != created_projections.end())
        {
            projection_parts.push_back(
                RangesInDataPart(it->second, part.data_part, part.part_index_in_query, part.part_starting_offset_in_query));
        }
    }

    auto step = MergeTreeDataSelectExecutor(merge_tree, projection)
                    .readFromParts(
                        std::make_shared<RangesInDataParts>(projection_parts),
                        snapshot_data.mutations_snapshot->cloneEmpty(),
                        column_names,
                        storage_snapshot,
                        query_info,
                        context,
                        max_block_size,
                        num_streams);

    if (step)
    {
        query_plan.addStep(std::move(step));
    }
    else
    {
        auto read_nothing = std::make_unique<ReadNothingStep>(std::make_shared<const Block>(projection->sample_block));
        read_nothing->setStepDescription("Read from NullSource (Projection)");
        query_plan.addStep(std::move(read_nothing));
    }
}

StorageSnapshotPtr
StorageFromMergeTreeProjection::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    auto parent_storage_snapshot = merge_tree.getStorageSnapshot(metadata_snapshot, query_context);
    const auto & parent_snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*parent_storage_snapshot->data);

    auto data = std::make_unique<MergeTreeData::SnapshotData>();
    data->storage = parent_snapshot_data.storage;
    data->parts = parent_snapshot_data.parts;
    data->mutations_snapshot = parent_snapshot_data.mutations_snapshot;

    return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, std::move(data));
}

}
