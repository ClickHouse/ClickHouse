#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Rewrite original query removing joined tables from it
bool removeJoin(ASTSelectQuery & select, const IdentifierMembershipCollector & membership_collector)
{
    const auto & tables = select.tables();
    if (!tables || tables->children.size() < 2)
        return false;

    const auto & joined_table = tables->children[1]->as<ASTTablesInSelectQueryElement &>();
    if (!joined_table.table_join)
        return false;

    /// We need to remove joined columns and related functions (taking in account aliases if any).
    auto * select_list = select.select()->as<ASTExpressionList>();
    if (select_list)
    {
        ASTs new_children;
        for (const auto & elem : select_list->children)
        {
            auto table_no = membership_collector.getIdentsMembership(elem);
            if (!table_no.has_value() || *table_no < 1)
                new_children.push_back(elem);
        }

        select_list->children = std::move(new_children);
    }

    /// The most simple temporary solution: leave only the first table in query.
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
                auto & prewhere_info = *query_info.prewhere_info;

                if (prewhere_info.row_level_filter)
                {
                    prewhere_info.row_level_filter->execute(header);
                    header.erase(prewhere_info.row_level_column_name);
                }

                if (prewhere_info.prewhere_actions)
                    prewhere_info.prewhere_actions->execute(header);

                if (prewhere_info.remove_prewhere_column)
                    header.erase(prewhere_info.prewhere_column_name);
            }
            return header;
        }
        case QueryProcessingStage::WithMergeableState:
        case QueryProcessingStage::Complete:
        case QueryProcessingStage::WithMergeableStateAfterAggregation:
        case QueryProcessingStage::MAX:
        {
            auto query = query_info.query->clone();
            auto & select = *query->as<ASTSelectQuery>();
            removeJoin(select, IdentifierMembershipCollector{select, context});

            auto stream = std::make_shared<OneBlockInputStream>(
                    metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID()));
            return InterpreterSelectQuery(query, context, stream, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();
        }
    }
    throw Exception("Logical Error: unknown processed stage.", ErrorCodes::LOGICAL_ERROR);
}

}

