#include <Storages/IStorage.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/InterpreterOptimizeQuery.h>
#include <Access/AccessRightsElement.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTExpressionList.h>

#include <Interpreters/processColumnTransformers.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
}


BlockIO InterpreterOptimizeQuery::execute()
{
    const auto & ast = query_ptr->as<ASTOptimizeQuery &>();

    if (!ast.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, getRequiredAccess());

    context.checkAccess(getRequiredAccess());

    auto table_id = context.resolveStorageID(ast, Context::ResolveOrdinary);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, context);
    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    // Empty list of names means we deduplicate by all columns, but user can explicitly state which columns to use.
    Names column_names;
    if (ast.deduplicate_by_columns)
    {
        // User requested custom set of columns for deduplication, possibly with Column Transformer expression.
        {
            const auto cols = processColumnTransformers(context.getCurrentDatabase(), table, metadata_snapshot, ast.deduplicate_by_columns);
            for (const auto & col : cols->children)
                column_names.emplace_back(col->getColumnName());
        }

        metadata_snapshot->check(column_names, NamesAndTypesList{}, table_id);
        // TODO: validate that deduplicate_by_columns contains all primary key columns.
        for (const auto & primary_key : metadata_snapshot->getPrimaryKeyColumns())
        {
            if (std::find(column_names.begin(), column_names.end(), primary_key) == column_names.end())
                throw Exception("Deduplicate expression doesn't contain primary key column: " + primary_key,
                        ErrorCodes::THERE_IS_NO_COLUMN);
        }
    }

    table->optimize(query_ptr, metadata_snapshot, ast.partition, ast.final, ast.deduplicate, column_names, context);

    return {};
}


AccessRightsElements InterpreterOptimizeQuery::getRequiredAccess() const
{
    const auto & optimize = query_ptr->as<const ASTOptimizeQuery &>();
    AccessRightsElements required_access;
    required_access.emplace_back(AccessType::OPTIMIZE, optimize.database, optimize.table);
    return required_access;
}

}
