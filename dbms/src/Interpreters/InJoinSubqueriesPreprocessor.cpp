#include <Interpreters/InJoinSubqueriesPreprocessor.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InDepthNodeVisitor.h>
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

StoragePtr tryGetTable(const ASTPtr & database_and_table, const Context & context)
{
    DatabaseAndTableWithAlias db_and_table(database_and_table);
    return context.tryGetTable(db_and_table.database, db_and_table.table);
}

using CheckShardsAndTables = InJoinSubqueriesPreprocessor::CheckShardsAndTables;

struct NonGlobalTableData
{
    using TypeToVisit = ASTTableExpression;

    const CheckShardsAndTables & checker;
    const Context & context;
    ASTFunction * function = nullptr;
    ASTTableJoin * table_join = nullptr;

    void visit(ASTTableExpression & node, ASTPtr &)
    {
        ASTPtr & database_and_table = node.database_and_table_name;
        if (database_and_table)
            renameIfNeeded(database_and_table);
    }

private:
    void renameIfNeeded(ASTPtr & database_and_table)
    {
        const SettingDistributedProductMode distributed_product_mode = context.getSettingsRef().distributed_product_mode;

        StoragePtr storage = tryGetTable(database_and_table, context);
        if (!storage || !checker.hasAtLeastTwoShards(*storage))
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
                table_join->locality = ASTTableJoin::Locality::Global;
            else
                throw Exception("Logical error: unexpected AST node", ErrorCodes::LOGICAL_ERROR);
        }
        else if (distributed_product_mode == DistributedProductMode::LOCAL)
        {
            /// Convert distributed table to corresponding remote table.

            std::string database;
            std::string table;
            std::tie(database, table) = checker.getRemoteDatabaseAndTableName(*storage);

            String alias = database_and_table->tryGetAlias();
            if (alias.empty())
                throw Exception("Distributed table should have an alias when distributed_product_mode set to local.",
                                ErrorCodes::DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED);

            database_and_table = createTableIdentifier(database, table);
            database_and_table->setAlias(alias);
        }
        else
            throw Exception("InJoinSubqueriesPreprocessor: unexpected value of 'distributed_product_mode' setting",
                            ErrorCodes::LOGICAL_ERROR);
    }
};

using NonGlobalTableMatcher = OneTypeMatcher<NonGlobalTableData>;
using NonGlobalTableVisitor = InDepthNodeVisitor<NonGlobalTableMatcher, true>;


class NonGlobalSubqueryMatcher
{
public:
    struct Data
    {
        const CheckShardsAndTables & checker;
        const Context & context;
    };

    static void visit(ASTPtr & node, Data & data)
    {
        if (auto * function = node->as<ASTFunction>())
            visit(*function, node, data);
        if (const auto * tables = node->as<ASTTablesInSelectQueryElement>())
            visit(*tables, node, data);
    }

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child)
    {
        if (auto * function = node->as<ASTFunction>())
            if (function->name == "in" || function->name == "notIn")
                return false; /// Processed, process others

        if (const auto * t = node->as<ASTTablesInSelectQueryElement>())
            if (t->table_join && t->table_expression)
                return false; /// Processed, process others

        /// Descent into all children, but not into subqueries of other kind (scalar subqueries), that are irrelevant to us.
        if (child->as<ASTSelectQuery>())
            return false;
        return true;
    }

private:
    static void visit(ASTFunction & node, ASTPtr &, Data & data)
    {
        if (node.name == "in" || node.name == "notIn")
        {
            auto & subquery = node.arguments->children.at(1);
            NonGlobalTableVisitor::Data table_data{data.checker, data.context, &node, nullptr};
            NonGlobalTableVisitor(table_data).visit(subquery);
        }
    }

    static void visit(const ASTTablesInSelectQueryElement & node, ASTPtr &, Data & data)
    {
        if (!node.table_join || !node.table_expression)
            return;

        ASTTableJoin * table_join = node.table_join->as<ASTTableJoin>();
        if (table_join->locality != ASTTableJoin::Locality::Global)
        {
            if (auto & subquery = node.table_expression->as<ASTTableExpression>()->subquery)
            {
                NonGlobalTableVisitor::Data table_data{data.checker, data.context, nullptr, table_join};
                NonGlobalTableVisitor(table_data).visit(subquery);
            }
        }
    }
};

using NonGlobalSubqueryVisitor = InDepthNodeVisitor<NonGlobalSubqueryMatcher, true>;

}


void InJoinSubqueriesPreprocessor::visit(ASTPtr & ast) const
{
    if (!ast)
        return;

    ASTSelectQuery * query = ast->as<ASTSelectQuery>();
    if (!query || !query->tables())
        return;

    if (context.getSettingsRef().distributed_product_mode == DistributedProductMode::ALLOW)
        return;

    const auto & tables_in_select_query = query->tables()->as<ASTTablesInSelectQuery &>();
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
        if (!storage || !checker->hasAtLeastTwoShards(*storage))
            return;
    }

    NonGlobalSubqueryVisitor::Data visitor_data{*checker, context};
    NonGlobalSubqueryVisitor(visitor_data).visit(ast);
}


bool InJoinSubqueriesPreprocessor::CheckShardsAndTables::hasAtLeastTwoShards(const IStorage & table) const
{
    const StorageDistributed * distributed = dynamic_cast<const StorageDistributed *>(&table);
    if (!distributed)
        return false;

    return distributed->getShardCount() >= 2;
}


std::pair<std::string, std::string>
InJoinSubqueriesPreprocessor::CheckShardsAndTables::getRemoteDatabaseAndTableName(const IStorage & table) const
{
    const StorageDistributed & distributed = dynamic_cast<const StorageDistributed &>(table);
    return { distributed.getRemoteDatabaseName(), distributed.getRemoteTableName() };
}


}
