#include <Storages/MergeTree/StorageFromMergeTreeProjection.h>

#include <Access/Common/AccessFlags.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/EnabledRowPolicies.h>
#include <Interpreters/Context.h>
#include <Planner/Utils.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/Sources/NullSource.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/SelectQueryInfo.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
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
    context->checkAccess(AccessType::SELECT, parent_storage->getStorageID());

    const auto parent_storage_id = parent_storage->getStorageID();
    auto row_policy_filter = context->getRowPolicyFilter(
        parent_storage_id.getDatabaseName(), parent_storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);

    Names read_column_names = column_names;
    if (row_policy_filter && !row_policy_filter->isAlwaysTrue())
    {
        /// an aggregate projection stores states built from many parent rows, so a per-row policy
        /// cannot be enforced after aggregation - refuse rather than expose hidden rows in the state
        if (projection->type != ProjectionDescription::Type::Normal)
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "Cannot read from projection `{}` of table {} under a row policy: it is not a normal "
                "projection, so the policy cannot be enforced before aggregation",
                projection->name, parent_storage_id.getNameForLogs());

        /// the policy is on the parent table; enforce it here or the projection leaks hidden rows
        if (!query_info.planner_context || !query_info.table_expression)
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "Cannot enforce the row policy of table {} on projection `{}` without the analyzer",
                parent_storage_id.getNameForLogs(), projection->name);

        for (const auto & policy : row_policy_filter->policies)
            if (context->hasQueryContext())
                context->getQueryContext()->addUsedRowPolicy(policy->getFullName().toString());

        FilterDAGInfo filter_info;
        try
        {
            /// resolve against the projection's own columns; anything it can't provide throws below
            filter_info = buildFilterInfo(
                row_policy_filter->expression->clone(), query_info.table_expression, query_info.planner_context);
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::UNKNOWN_IDENTIFIER && e.code() != ErrorCodes::NO_SUCH_COLUMN_IN_TABLE)
                throw;
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "Cannot read from projection `{}` of table {} because its row policy references a column "
                "the projection does not store, so it cannot be enforced",
                projection->name, parent_storage_id.getNameForLogs());
        }

        /// read the policy's columns even if the query didn't ask for them; refuse virtuals the projection
        /// does not preserve (position-relative ones - it can reorder rows) or exposes under a
        /// projection-only name absent on the parent (`_parent_part_offset`), since a parent policy binding
        /// to those does not carry parent-row semantics
        static const NameSet not_row_preserving{
            "_part_offset", "_part_index", "_part_granule_offset", "_block_offset", "_block_number", "_parent_part_offset"};
        for (const auto & name : filter_info.actions.getRequiredColumnsNames())
        {
            if (not_row_preserving.contains(name))
                throw Exception(ErrorCodes::ACCESS_DENIED,
                    "Cannot read from projection `{}` of table {} because its row policy uses virtual column "
                    "`{}`, whose value the projection does not preserve, so it cannot be enforced",
                    projection->name, parent_storage_id.getNameForLogs(), name);
            if (std::find(read_column_names.begin(), read_column_names.end(), name) == read_column_names.end())
                read_column_names.push_back(name);
        }

        /// the planner may have already installed a filter for a policy on the table function itself
        /// (`_table_function.*`); overwriting it would drop that restriction, so refuse rather than
        /// silently apply only the parent policy
        if (query_info.row_level_filter)
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "Cannot read from projection `{}` of table {}: a row policy already applies to the table "
                "function itself and cannot be combined with the parent table's row policy",
                projection->name, parent_storage_id.getNameForLogs());

        /// row-level filter runs before any user PREWHERE (a post-read filter would let PREWHERE see hidden rows)
        query_info.row_level_filter = std::make_shared<FilterDAGInfo>(std::move(filter_info));

        /// planner left trivial-LIMIT on (it checks this tf's storage id, which has no policy); n rows then filter could yield < n
        query_info.trivial_limit = 0;
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
                        read_column_names,
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
