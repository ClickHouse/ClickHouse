#include <Interpreters/Context.h>
#include <Interpreters/InterpreterAnalyzeQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>
#include <QueryCoordination/Optimizer/Statistics/IStatisticsStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_TABLE;
extern const int UNKNOWN_COLUMN;
extern const int UNSUPPORTED_METHOD;
}

BlockIO InterpreterAnalyzeQuery::executeAnalyzeTable()
{
    auto * query = query_ptr->as<ASTAnalyzeQuery>();
    auto statistics_storage = context->getStatisticsStorage();

    auto database = query->database == nullptr ? context->getCurrentDatabase() : query->database->as<ASTIdentifier>()->name();
    auto table = query->table->as<ASTIdentifier>()->name();

    StorageID storage_id(database, table);
    auto storage = DatabaseCatalog::instance().tryGetTable(storage_id, context);

    if (!storage)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} does not exist", storage_id.getFullNameNotQuoted());

    if (storage->isRemote())
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD, "Analyzing is unsupported for non local table {}", storage_id.getFullNameNotQuoted());

    if (storage->as<MergeTreeData>())
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD, "Analyzing is unsupported for non merge tree table {}", storage_id.getFullNameNotQuoted());

    Names column_names;
    auto table_columns = storage->getInMemoryMetadata().getColumns();

    if (query->column_list)
    {
        auto * column_exprs = query->column_list->as<ASTExpressionList>();

        for (auto & column_expr : column_exprs->children)
        {
            if (auto * column_name = column_expr->as<ASTIdentifier>())
            {
                if (!table_columns.has(column_name->name()))
                    throw Exception(ErrorCodes::UNKNOWN_COLUMN, "Column {} does not exist", column_name->name());
                column_names.push_back(column_name->name());
            }
        }
    }
    else
    {
        for (const auto & column : table_columns.getAll())
            column_names.push_back(column.name);
    }

    if (query->settings)
    {
        if (auto * set_query = query->settings->as<ASTSetQuery>())
            context->applySettingsChanges(set_query->changes);
    }

    statistics_storage->collect(storage_id, column_names, context);
    return {};
}

BlockIO InterpreterAnalyzeQuery::execute()
{
    return executeAnalyzeTable();
}

}
