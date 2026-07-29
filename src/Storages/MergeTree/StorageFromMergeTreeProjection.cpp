#include <Storages/MergeTree/StorageFromMergeTreeProjection.h>

#include <Access/Common/AccessFlags.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/EnabledRowPolicies.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/replaceAliasColumnsInQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/Sources/NullSource.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
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

/// A row policy resolved against a projection's columns, ready to be applied as a FilterStep.
struct MergeTreeProjectionRowPolicyFilter
{
    ActionsDAG dag;
    String filter_column_name;
    Names required_columns;
};

/// Rewrite references to the parent `_part_offset` into the projection's stored `_parent_part_offset`.
static void remapPartOffsetToParent(ASTPtr & ast)
{
    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        if (identifier->name() == "_part_offset")
            ast = make_intrusive<ASTIdentifier>("_parent_part_offset");
        return;
    }
    for (auto & child : ast->children)
        remapPartOffsetToParent(child);
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

    /// row policies live on the parent table, so enforce them here or the projection leaks hidden rows
    auto row_policy = buildRowPolicyFilter(context);

    Names read_column_names = column_names;
    if (row_policy)
    {
        /// read the policy columns too, even if the query did not ask for them
        for (const auto & name : row_policy->required_columns)
            if (std::find(read_column_names.begin(), read_column_names.end(), name) == read_column_names.end())
                read_column_names.push_back(name);

        /// Apply as a row-level filter so it runs ahead of any user PREWHERE, exactly like a normal
        /// MergeTree read. A post-read FilterStep would let a PREWHERE predicate observe hidden rows first.
        /// Keep the filter column if the query selects it too (a bare-column policy like `USING flag`).
        const bool remove_filter_column
            = std::find(column_names.begin(), column_names.end(), row_policy->filter_column_name) == column_names.end();
        query_info.row_level_filter = std::make_shared<FilterDAGInfo>(
            FilterDAGInfo{std::move(row_policy->dag), row_policy->filter_column_name, remove_filter_column});
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

    /// drop the policy columns we added on top of what the query requested
    if (row_policy && read_column_names.size() != column_names.size())
    {
        auto target = storage_snapshot->getSampleBlockForColumns(column_names);
        auto convert = ActionsDAG::makeConvertingActions(
            query_plan.getCurrentHeader()->getColumnsWithTypeAndName(),
            target.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            context);
        query_plan.addStep(std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(convert)));
    }
}

std::unique_ptr<MergeTreeProjectionRowPolicyFilter> StorageFromMergeTreeProjection::buildRowPolicyFilter(const ContextPtr & context) const
{
    const auto parent_storage_id = parent_storage->getStorageID();
    auto row_policy_filter = context->getRowPolicyFilter(
        parent_storage_id.getDatabaseName(), parent_storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);

    if (!row_policy_filter || row_policy_filter->isAlwaysTrue())
        return nullptr;

    ASTPtr expr = row_policy_filter->expression->clone();

    /// Expand parent ALIAS/DEFAULT columns to their physical dependencies, which the projection may
    /// store even though it does not expose the alias itself (e.g. `c ALIAS b + 1` -> reads `b`).
    replaceAliasColumnsInQuery(expr, parent_metadata->getColumns(), {}, context);

    /// The parent `_part_offset` is materialized as `_parent_part_offset` in projections that carry it,
    /// so a policy on `_part_offset` keeps its parent-row semantics when read through such a projection.
    if (projection->with_parent_part_offset)
        remapPartOffsetToParent(expr);

    /// Resolve against exactly what the projection read can serve. Anything else fails to resolve, and we
    /// fail closed with a clear ACCESS_DENIED. What the projection read can serve:
    ///  - its stored physical columns;
    ///  - part-identity virtuals (`_part`, `_partition_id`, ...): a projection part maps back to its parent
    ///    part (`RangesInDataPart::getDescription`), so these keep parent-row semantics;
    ///  - the parent offsets it preserves: `_part_starting_offset`, and the parent `_part_offset` remapped
    ///    to `_parent_part_offset` above.
    /// Position-relative virtuals (`_part_offset`, `_part_index`, ...) are intentionally NOT offered: on a
    /// reordered projection they point at different rows than the parent, so enforcing a policy on them
    /// would filter the wrong rows. That is why we use this explicit set instead of the whole snapshot.
    auto available_columns = projection->metadata->getColumns().getAllPhysical();
    available_columns.emplace_back("_part_starting_offset", std::make_shared<DataTypeUInt64>());
    if (projection->with_parent_part_offset)
        available_columns.emplace_back("_parent_part_offset", std::make_shared<DataTypeUInt64>());

    auto parent_snapshot = merge_tree.getStorageSnapshot(parent_metadata, context);
    auto virtual_options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::All);
    for (const auto & name : MergeTreeData::virtuals_useful_for_filter)
        if (auto virtual_column = parent_snapshot->tryGetColumn(virtual_options, name))
            available_columns.push_back(*virtual_column);

    ActionsDAG dag = [&]
    {
        try
        {
            auto syntax_result = TreeRewriter(context).analyze(expr, available_columns);
            ExpressionAnalyzer analyzer(expr, syntax_result, context);
            return analyzer.getActionsDAG(false /* add_aliases */, false /* remove_unused_result */);
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::UNKNOWN_IDENTIFIER && e.code() != ErrorCodes::NO_SUCH_COLUMN_IN_TABLE)
                throw;
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "Cannot read from projection `{}` of table {} because its row policy references a column "
                "that the projection does not store, so the row policy cannot be enforced",
                projection->name, parent_storage_id.getNameForLogs());
        }
    }();

    /// The filter column is the policy expression's own output node (which may be a bare input column,
    /// e.g. `USING c0`), mirroring `generateFilterActions`/`buildFilterInfo` rather than assuming a new node.
    String filter_column_name = expr->getColumnName();
    if (!dag.tryFindInOutputs(filter_column_name))
        throw Exception(ErrorCodes::ACCESS_DENIED,
            "Cannot determine row policy filter column for projection `{}` of table {}",
            projection->name, parent_storage_id.getNameForLogs());

    /// record the applied policies so they show up in system.query_log, like a normal table read
    if (context->hasQueryContext())
        for (const auto & row_policy : row_policy_filter->policies)
            context->getQueryContext()->addUsedRowPolicy(row_policy->getFullName().toString());

    auto result = std::make_unique<MergeTreeProjectionRowPolicyFilter>();
    result->required_columns = dag.getRequiredColumnsNames();
    result->filter_column_name = std::move(filter_column_name);
    result->dag = std::move(dag);
    return result;
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
