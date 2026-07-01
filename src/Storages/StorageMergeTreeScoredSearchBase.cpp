#include <Storages/StorageMergeTreeScoredSearchBase.h>

#include <Access/Common/AccessFlags.h>
#include <Access/EnabledRowPolicies.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/IAST.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinLazyColumnsStep.h>
#include <Processors/QueryPlan/LazilyReadFromMergeTree.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromMergeTreeScoredSearch.h>
#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/ScoredSearch/DelayedCreatingBitmapsStep.h>
#include <Storages/MergeTree/ScoredSearch/ScoredSearchUtils.h>
#include <Storages/MergeTree/ScoredSearch/IScorer.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsUInt64 search_topk_prefilter_max_rows;
    extern const SettingsUInt64 select_sequential_consistency;
}

void StorageMergeTreeScoredSearchBase::checkSourceColumnsForReservedNames(const ColumnsDescription & source_columns, const StorageID & source_storage_id)
{
    for (const auto * reserved_name : {"_score", "_part", "_part_index", "_part_offset", "__global_row_index"})
    {
        if (source_columns.has(reserved_name))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot use a scored-search table function with table {}: its column {} collides with a reserved scored-search column name",
                source_storage_id.getNameForLogs(), backQuote(reserved_name));
        }
    }
}

StorageMergeTreeScoredSearchBase::StorageMergeTreeScoredSearchBase(
    const StorageID & table_id_,
    StoragePtr source_table_,
    const ColumnsDescription & columns,
    const std::vector<MergeTreeIndexPtr> & scorer_indexes_)
    : IStorage(table_id_)
    , source_table(std::move(source_table_))
    , scorer_indexes(scorer_indexes_)
{
    if (dynamic_cast<const MergeTreeData *>(source_table.get()) == nullptr)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "StorageMergeTreeScoredSearchBase expected a MergeTree source table, got {}",
            source_table->getName());
    }

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    setInMemoryMetadata(storage_metadata);
}

QueryProcessingStage::Enum StorageMergeTreeScoredSearchBase::getQueryProcessingStage(
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*to_stage*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & /*query_info*/) const
{
    return QueryProcessingStage::FetchColumns;
}

std::pair<std::optional<FilterDAGInfo>, PreparedSetsPtr> StorageMergeTreeScoredSearchBase::buildRowPolicyFilterInfo(
    ContextPtr context,
    const ColumnsDescription & source_columns,
    const StorageID & source_storage_id)
{
    auto row_policy_filter = context->getRowPolicyFilter(source_storage_id.getDatabaseName(), source_storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);

    if (!row_policy_filter || row_policy_filter->isAlwaysTrue())
        return {};

    /// Record the applied policies in the query access info, so that
    /// `system.query_log.used_row_policies` is populated, the same way the
    /// planner does it in `buildRowPolicyFilterIfNeeded`.
    if (context->hasQueryContext())
    {
        for (const auto & row_policy : row_policy_filter->policies)
            context->getQueryContext()->addUsedRowPolicy(row_policy->getFullName().toString());
    }

    ASTPtr expression = row_policy_filter->expression->clone();
    auto syntax_result = TreeRewriter(context).analyze(expression, source_columns.getAll());
    ExpressionAnalyzer analyzer(expression, syntax_result, context);
    auto policy_dag = analyzer.getActionsDAG(/*add_aliases=*/ false, /*remove_unused_result=*/ false);
    String filter_column_name = expression->getColumnName();

    /// A policy like `tenant IN (SELECT ...)` registers a future set in the
    /// analyzer's prepared sets, but the DAG bypasses the interpreter,
    /// so no outer plan step would build it.
    PreparedSetsPtr row_policy_sets = analyzer.getPreparedSets();

    if (!policy_dag.tryFindInOutputs(filter_column_name))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot find result column {} of the row policy filter for table {} in the built filter DAG",
            filter_column_name, source_storage_id.getNameForLogs());
    }

    return {FilterDAGInfo{std::move(policy_dag), std::move(filter_column_name), /*do_remove_column=*/ true}, std::move(row_policy_sets)};
}

PartitionIdToMaxBlockPtr StorageMergeTreeScoredSearchBase::getMaxBlockNumbersToRead(ContextPtr context) const
{
    PartitionIdToMaxBlockPtr max_block_numbers_to_read = context->getPartitionIdToMaxBlock(source_table->getStorageID().uuid);
    const auto * replicated = dynamic_cast<const StorageReplicatedMergeTree *>(source_table.get());

    if (replicated && context->getSettingsRef()[Setting::select_sequential_consistency])
    {
        auto max_added_blocks = std::make_shared<PartitionIdToMaxBlock>(replicated->getMaxAddedBlocks());

        if (max_block_numbers_to_read)
        {
            for (const auto & [partition_id, max_block] : *max_block_numbers_to_read)
            {
                auto [it, inserted] = max_added_blocks->emplace(partition_id, max_block);
                if (!inserted)
                    it->second = std::min(it->second, max_block);
            }
        }

        return max_added_blocks;
    }

    return max_block_numbers_to_read;
}

void StorageMergeTreeScoredSearchBase::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processing_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    auto source_storage_id = source_table->getStorageID();
    auto source_metadata = source_table->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
    const auto & source_columns = source_metadata->getColumns();
    /// The table function exposes the source table's rows, so check
    /// SELECT on the source for every source column the query reads.
    NameSet source_columns_to_check;

    for (const auto & name : column_names)
    {
        if (auto column = source_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, name))
            source_columns_to_check.insert(column->getNameInStorage());
    }

    for (const auto & index : getScorerIndexes())
    {
        for (const auto & name : index->getColumnsRequiredForIndexCalc())
            source_columns_to_check.insert(name);
    }

    if (source_columns_to_check.empty())
        context->checkAccess(AccessType::SELECT, source_storage_id);
    else
        context->checkAccess(AccessType::SELECT, source_storage_id, Names(source_columns_to_check.begin(), source_columns_to_check.end()));

    auto [row_policy_filter_info, row_policy_sets] = buildRowPolicyFilterInfo(context, source_columns, source_storage_id);

    auto source_snapshot = source_table->getStorageSnapshot(source_metadata, context);
    const auto & source_snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*source_snapshot->data);
    auto mutations_snapshot = source_snapshot_data.mutations_snapshot;
    /// Match the read-consistency boundary of a normal SELECT from the source table.
    auto max_block_numbers_to_read = getMaxBlockNumbersToRead(context);

    auto ranges = std::make_shared<RangesInDataParts>();
    ranges->reserve(source_snapshot_data.parts->size());
    size_t starting_offset = 0;

    for (const auto & snapshot_part : *source_snapshot_data.parts)
    {
        const auto & part = snapshot_part.data_part;
        if (part->isEmpty())
            continue;

        /// The same check as in `MergeTreeDataSelectExecutor::selectPartsToRead`.
        if (max_block_numbers_to_read)
        {
            auto blocks_iterator = max_block_numbers_to_read->find(part->info.getPartitionId());
            if (blocks_iterator == max_block_numbers_to_read->end() || part->info.max_block > blocks_iterator->second)
                continue;
        }

        ranges->emplace_back(part, /*parent_part=*/ nullptr, /*part_index_in_query=*/ ranges->size(), /*part_starting_offset_in_query=*/ starting_offset);
        starting_offset += part->rows_count;
    }

    auto bitmap_state = std::make_shared<LazyBitmapSubqueryState>();
    bitmap_state->rows_budget = context->getSettingsRef()[Setting::search_topk_prefilter_max_rows];
    auto this_ptr = std::static_pointer_cast<StorageMergeTreeScoredSearchBase>(shared_from_this());

    /// Two-plan structure:
    ///
    ///   scorer_plan = ReadFromMergeTreeScoredSearch                   (__global_row_index, _score)
    ///                 + DelayedCreatingBitmapsStep                    (pass-through when no WHERE)
    ///   lazy_plan   = LazilyReadFromMergeTree                         (source_cols...)

    auto scorer_output_header = std::make_shared<const Block>(Block
    {
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "__global_row_index"),
        ColumnWithTypeAndName(std::make_shared<DataTypeFloat32>(), "_score"),
    });

    QueryPlan scorer_plan;
    auto scorer_read_step = std::make_unique<ReadFromMergeTreeScoredSearch>(
        column_names,
        query_info,
        storage_snapshot,
        context,
        scorer_output_header,
        this_ptr,
        ranges,
        bitmap_state,
        source_snapshot,
        mutations_snapshot,
        std::move(row_policy_filter_info),
        std::move(row_policy_sets),
        num_streams);

    scorer_plan.addStep(std::move(scorer_read_step));

    /// Planted unconditionally; at pipeline-build time it either gates the
    /// scorer pipeline behind the bitmap subquery or passes it through.
    auto bitmaps_step = std::make_unique<DelayedCreatingBitmapsStep>(scorer_plan.getCurrentHeader(), bitmap_state, context);
    scorer_plan.addStep(std::move(bitmaps_step));

    Names lazy_column_names;
    lazy_column_names.reserve(column_names.size());

    for (const auto & name : column_names)
    {
        if (name == "_score" || name == "__global_row_index")
            continue;

        lazy_column_names.push_back(name);
    }

    if (lazy_column_names.empty())
    {
        /// Only scorer-produced columns are requested (e.g. `SELECT _score FROM vectorSearch(...)`).
        const auto & scorer_header = *scorer_plan.getCurrentHeader();

        ColumnsWithTypeAndName result_columns;
        result_columns.reserve(scorer_header.columns() - 1);

        for (const auto & column : scorer_header)
        {
            if (column.name != "__global_row_index")
                result_columns.push_back(column);
        }

        auto drop_row_index_dag = ActionsDAG::makeConvertingActions(
            scorer_header.getColumnsWithTypeAndName(),
            result_columns,
            ActionsDAG::MatchColumnsMode::Name,
            context);

        auto drop_step = std::make_unique<ExpressionStep>(scorer_plan.getCurrentHeader(), std::move(drop_row_index_dag));
        drop_step->setStepDescription("Drop __global_row_index");
        scorer_plan.addStep(std::move(drop_step));
        query_plan = std::move(scorer_plan);
        return;
    }

    /// Build the lazy sub-plan.
    const auto & settings = context->getSettingsRef();
    auto reader_settings = MergeTreeReaderSettings::createFromContext(context);
    auto data_settings = getSourceTable()->getSettings();
    const PartRangesReadInfo part_ranges_read_info(*ranges, settings, *data_settings);
    const size_t min_marks_for_concurrent_read = part_ranges_read_info.min_marks_for_concurrent_read;

    /// Shared `LazyMaterializingRows` threaded into both the lazy reader below and the `JoinLazyColumnsStep`.
    auto lazy_rows = std::make_shared<LazyMaterializingRows>(*ranges);
    auto lazy_header = std::make_shared<const Block>(source_snapshot->getSampleBlockForColumns(lazy_column_names));

    auto lazy_step = std::make_unique<LazilyReadFromMergeTree>(
        lazy_header,
        max_block_size,
        min_marks_for_concurrent_read,
        reader_settings,
        mutations_snapshot,
        source_snapshot,
        context,
        getSourceTable()->getLogName());

    lazy_step->setLazyMaterializingRows(lazy_rows);
    lazy_step->setStorageLimits(query_info.storage_limits);

    QueryPlan lazy_plan;
    lazy_plan.addStep(std::move(lazy_step));

    auto join_step = std::make_unique<JoinLazyColumnsStep>(
        scorer_plan.getCurrentHeader(),
        lazy_plan.getCurrentHeader(),
        lazy_rows);

    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(scorer_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(lazy_plan)));
    query_plan.unitePlans(std::move(join_step), std::move(plans));
}

}
