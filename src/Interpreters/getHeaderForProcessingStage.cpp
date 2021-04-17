#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Storages/IStorage.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Rewrite original query removing joined tables from it
bool removeJoin(ASTSelectQuery & select)
{
    const auto & tables = select.tables();
    if (!tables || tables->children.size() < 2)
        return false;

    const auto & joined_table = tables->children[1]->as<ASTTablesInSelectQueryElement &>();
    if (!joined_table.table_join)
        return false;

    /// The most simple temporary solution: leave only the first table in query.
    /// TODO: we also need to remove joined columns and related functions (taking in account aliases if any).
    tables->children.resize(1);
    return true;
}

Block getHeaderForProcessingStage(
        const IStorage & storage,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage)
{
    switch (processed_stage)
    {
        case QueryProcessingStage::FetchColumns:
        {
            Block header = metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID());
            if (query_info.prewhere_info)
            {
                query_info.prewhere_info->prewhere_actions->execute(header);
                if (query_info.prewhere_info->remove_prewhere_column)
                    header.erase(query_info.prewhere_info->prewhere_column_name);
            }
            return header;
        }
        case QueryProcessingStage::WithMergeableState:
        case QueryProcessingStage::Complete:
        case QueryProcessingStage::WithMergeableStateAfterAggregation:
        case QueryProcessingStage::MAX:
        {
            auto query = query_info.query->clone();
            removeJoin(*query->as<ASTSelectQuery>());

            auto stream = std::make_shared<OneBlockInputStream>(
                    metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID()));
            return InterpreterSelectQuery(query, context, stream, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();
        }
    }
    throw Exception("Logical Error: unknown processed stage.", ErrorCodes::LOGICAL_ERROR);
}

}

