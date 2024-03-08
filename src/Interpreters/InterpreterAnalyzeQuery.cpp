#include <Access/Common/AccessRightsElement.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterAnalyzeQuery.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/InterpreterFactory.h>
#include <Optimizer/Statistics/IStatisticsStorage.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageDistributed.h>


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
    /// TODO add access
    AccessRightsElements access_rights_elements;
    /// access_rights_elements.emplace_back(AccessType::xxx);

    auto * query = query_ptr->as<ASTAnalyzeQuery>();
    auto statistics_storage = context->getStatisticsStorage();

    auto database = query->database == nullptr ? context->getCurrentDatabase() : query->database->as<ASTIdentifier>()->name();
    auto table = query->table->as<ASTIdentifier>()->name();

    StorageID storage_id(database, table);
    auto storage = DatabaseCatalog::instance().tryGetTable(storage_id, context);

    if (!storage)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} does not exist", storage_id.getFullNameNotQuoted());

    /// If analyzing a distributed table we schedule a DDL task and use cluster of the distributed table.
    if (auto * distributed_storage = storage->as<StorageDistributed>())
    {
        auto cloned = query_ptr->clone();
        auto * cloned_analyze_query = cloned->as<ASTAnalyzeQuery>();

        /// Set cluster to cluster of distributed table
        cloned_analyze_query->cluster = distributed_storage->getCluster()->getName();

        /// Replace distributed table to local table
        if (database != distributed_storage->getRemoteDatabaseName())
            cloned_analyze_query->database = std::make_shared<ASTIdentifier>(distributed_storage->getRemoteDatabaseName());
        cloned_analyze_query->table = std::make_shared<ASTIdentifier>(distributed_storage->getRemoteTableName());

        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(cloned, context, params);
    }

    if (!storage->isMergeTree())
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD, "Analyzing is unsupported for non merge tree table {}", storage_id.getFullNameNotQuoted());

    if (storage->isRemote())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Analyzing is unsupported the remote table {}", storage_id.getFullNameNotQuoted());

    context->checkAccess(access_rights_elements);

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

void registerInterpreterAnalyzeQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterAnalyzeQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterAnalyzeQuery", create_fn);
}

}
