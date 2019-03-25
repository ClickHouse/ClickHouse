#include <Interpreters/InJoinSubqueriesPreprocessor.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Storages/StorageDistributed.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED;
    extern const int LOGICAL_ERROR;
}


namespace
{

/** Call a function for each non-GLOBAL subquery in IN or JOIN.
  * Pass to function: AST node with subquery, and AST node with corresponding IN function or JOIN.
  * Consider only first-level subqueries (do not go recursively into subqueries).
  */
template <typename F>
void forEachNonGlobalSubquery(IAST * node, F && f)
{
    if (auto * function = node->as<ASTFunction>())
    {
        if (function->name == "in" || function->name == "notIn")
        {
            f(function->arguments->children.at(1).get(), function, nullptr);
            return;
        }

        /// Pass into other functions, as subquery could be in aggregate or in lambda functions.
    }
    else if (const auto * join = node->as<ASTTablesInSelectQueryElement>())
    {
        if (join->table_join && join->table_expression)
        {
            auto & table_join = join->table_join->as<ASTTableJoin &>();
            if (table_join.locality != ASTTableJoin::Locality::Global)
            {
                auto & subquery = join->table_expression->as<ASTTableExpression>()->subquery;
                if (subquery)
                    f(subquery.get(), nullptr, &table_join);
            }
            return;
        }

        /// Pass into other kind of JOINs, as subquery could be in ARRAY JOIN.
    }

    /// Descent into all children, but not into subqueries of other kind (scalar subqueries), that are irrelevant to us.
    for (auto & child : node->children)
        if (!child->as<ASTSelectQuery>())
            forEachNonGlobalSubquery(child.get(), f);
}


/** Find all (ordinary) tables in any nesting level in AST.
  */
template <typename F>
void forEachTable(IAST * node, F && f)
{
    if (auto * table_expression = node->as<ASTTableExpression>())
    {
        auto & database_and_table = table_expression->database_and_table_name;
        if (database_and_table)
            f(database_and_table);
    }

    for (auto & child : node->children)
        forEachTable(child.get(), f);
}


StoragePtr tryGetTable(const ASTPtr & database_and_table, const Context & context)
{
    DatabaseAndTableWithAlias db_and_table(database_and_table);
    return context.tryGetTable(db_and_table.database, db_and_table.table);
}

}


void InJoinSubqueriesPreprocessor::process(ASTSelectQuery * query) const
{
    if (!query)
        return;

    const SettingDistributedProductMode distributed_product_mode = context.getSettingsRef().distributed_product_mode;

    if (distributed_product_mode == DistributedProductMode::ALLOW)
        return;

    if (!query->tables)
        return;

    const auto & tables_in_select_query = query->tables->as<ASTTablesInSelectQuery &>();
    if (tables_in_select_query.children.empty())
        return;

    const auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();
    if (!tables_element.table_expression)
        return;

    const auto * table_expression = tables_element.table_expression->as<ASTTableExpression>();

    /// If not ordinary table, skip it.
    if (!table_expression->database_and_table_name)
        return;

    /// If not really distributed table, skip it.
    {
        StoragePtr storage = tryGetTable(table_expression->database_and_table_name, context);
        if (!storage || !hasAtLeastTwoShards(*storage))
            return;
    }

    forEachNonGlobalSubquery(query, [&] (IAST * subquery, IAST * function, IAST * table_join)
    {
        forEachTable(subquery, [&] (ASTPtr & database_and_table)
        {
            StoragePtr storage = tryGetTable(database_and_table, context);

            if (!storage || !hasAtLeastTwoShards(*storage))
                return;

            if (distributed_product_mode == DistributedProductMode::DENY)
            {
                throw Exception("Double-distributed IN/JOIN subqueries is denied (distributed_product_mode = 'deny')."
                    " You may rewrite query to use local tables in subqueries, or use GLOBAL keyword, or set distributed_product_mode to suitable value.",
                    ErrorCodes::DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED);
            }
            else if (distributed_product_mode == DistributedProductMode::GLOBAL)
            {
                if (function)
                {
                    auto * concrete = function->as<ASTFunction>();

                    if (concrete->name == "in")
                        concrete->name = "globalIn";
                    else if (concrete->name == "notIn")
                        concrete->name = "globalNotIn";
                    else if (concrete->name == "globalIn" || concrete->name == "globalNotIn")
                    {
                        /// Already processed.
                    }
                    else
                        throw Exception("Logical error: unexpected function name " + concrete->name, ErrorCodes::LOGICAL_ERROR);
                }
                else if (table_join)
                    table_join->as<ASTTableJoin &>().locality = ASTTableJoin::Locality::Global;
                else
                    throw Exception("Logical error: unexpected AST node", ErrorCodes::LOGICAL_ERROR);
            }
            else if (distributed_product_mode == DistributedProductMode::LOCAL)
            {
                /// Convert distributed table to corresponding remote table.

                std::string database;
                std::string table;
                std::tie(database, table) = getRemoteDatabaseAndTableName(*storage);

                database_and_table = createTableIdentifier(database, table);
            }
            else
                throw Exception("InJoinSubqueriesPreprocessor: unexpected value of 'distributed_product_mode' setting", ErrorCodes::LOGICAL_ERROR);
        });
    });
}


bool InJoinSubqueriesPreprocessor::hasAtLeastTwoShards(const IStorage & table) const
{
    const StorageDistributed * distributed = dynamic_cast<const StorageDistributed *>(&table);
    if (!distributed)
        return false;

    return distributed->getShardCount() >= 2;
}


std::pair<std::string, std::string>
InJoinSubqueriesPreprocessor::getRemoteDatabaseAndTableName(const IStorage & table) const
{
    const StorageDistributed & distributed = dynamic_cast<const StorageDistributed &>(table);
    return { distributed.getRemoteDatabaseName(), distributed.getRemoteTableName() };
}


}
