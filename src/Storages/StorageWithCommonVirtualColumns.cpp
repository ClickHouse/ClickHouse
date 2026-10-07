#include <Core/QueryProcessingStage.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Storages/VirtualColumnsDescription.h>
#include <Storages/VirtualColumnUtils.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <Interpreters/ActionsDAG.h>

#include <Analyzer/TableNode.h>

#include <base/scope_guard.h>
#include <Common/Exception.h>

namespace DB
{

namespace
{

void materializeConstantColumn(QueryPlan & query_plan, const std::string & name, const DataTypePtr & type, const Field & value)
{
    auto step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), ActionsDAG::makeAddingConstantColumnActions(name, type, value));
    step->setStepDescription(fmt::format("Materialize {} virtual column", name), 100);
    query_plan.addStep(std::move(step));
}

void convertToHeader(QueryPlan & query_plan, const Block & header, ContextPtr context)
{
    auto converting_dag = ActionsDAG::makeConvertingActions(
        query_plan.getCurrentHeader()->getColumnsWithTypeAndName(),
        header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context);

    auto converting = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(converting_dag));
    converting->setStepDescription("Reorder columns to match requested columns sequence");
    query_plan.addStep(std::move(converting));
}

}

void StorageWithCommonVirtualColumns::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    if (processed_stage > QueryProcessingStage::FetchColumns)
    {
        readImpl(query_plan, column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
        return;
    }

    /// Proxy to underlying storage.
    auto filtered_columns = VirtualColumnUtils::filterVirtualColumns(column_names, storage_snapshot->metadata, VirtualsKind::Ephemeral, VirtualsMaterializationPlace::Plan);
    readImpl(query_plan, filtered_columns, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);

    /// Materialize constant virtuals.
    if (query_plan.isInitialized())
    {
        if (std::ranges::contains(column_names, "_database") && !query_plan.getCurrentHeader()->has("_database"))
            materializeConstantColumn(query_plan, "_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), getStorageID().getDatabaseName());

        if (std::ranges::contains(column_names, "_table") && !query_plan.getCurrentHeader()->has("_table"))
        {
            std::string table_name = getStorageID().getTableName();
            if (query_info.table_expression)
                if (auto * table_node = query_info.table_expression->as<TableNode>())
                    if (table_node->isTemporaryTable())
                        table_name = table_node->getTemporaryTableName();

            materializeConstantColumn(query_plan, "_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), table_name);
        }
    }

    /// Match column_names sequence
    if (query_plan.isInitialized())
        if (filtered_columns != column_names)
            convertToHeader(query_plan, storage_snapshot->getSampleBlockForColumns(column_names), context);
}

void StorageWithCommonVirtualColumns::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    IStorage::read(query_plan, column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
}

}
