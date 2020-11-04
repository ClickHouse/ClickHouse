#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>

namespace DB
{
void ApplyWithSubqueryVisitor::visit(ASTPtr & ast, const Data & data)
{
    if (auto * node_select = ast->as<ASTSelectQuery>())
    {
        std::optional<Data> new_data;
        if (auto with = node_select->with())
        {
            for (auto & child : with->children)
            {
                visit(child, new_data ? *new_data : data);
                if (auto * ast_with_elem = child->as<ASTWithElement>())
                {
                    if (!new_data)
                        new_data = data;
                    new_data->subqueries[ast_with_elem->name] = ast_with_elem->subquery;
                }
            }
        }

        for (auto & child : node_select->children)
        {
            if (child != node_select->with())
                visit(child, new_data ? *new_data : data);
        }
    }
    else
    {
        for (auto & child : ast->children)
            visit(child, data);
        if (auto * node_func = ast->as<ASTFunction>())
            visit(*node_func, data);
        else if (auto * node_table = ast->as<ASTTableExpression>())
            visit(*node_table, data);
    }
}

void ApplyWithSubqueryVisitor::visit(ASTTableExpression & table, const Data & data)
{
    if (table.database_and_table_name)
    {
        auto table_id = IdentifierSemantic::extractDatabaseAndTable(table.database_and_table_name->as<ASTIdentifier &>());
        if (table_id.database_name.empty())
        {
            auto subquery_it = data.subqueries.find(table_id.table_name);
            if (subquery_it != data.subqueries.end())
            {
                table.children.clear();
                table.database_and_table_name.reset();
                table.subquery = subquery_it->second->clone();
                dynamic_cast<ASTWithAlias &>(*table.subquery).alias = table_id.table_name;
                table.children.emplace_back(table.subquery);
            }
        }
    }
}

void ApplyWithSubqueryVisitor::visit(ASTFunction & func, const Data & data)
{
    if (checkFunctionIsInOrGlobalInOperator(func))
    {
        auto & ast = func.arguments->children.at(1);
        if (const auto * ident = ast->as<ASTIdentifier>())
        {
            auto table_id = IdentifierSemantic::extractDatabaseAndTable(*ident);
            if (table_id.database_name.empty())
            {
                auto subquery_it = data.subqueries.find(table_id.table_name);
                if (subquery_it != data.subqueries.end())
                {
                    func.arguments->children[1] = subquery_it->second->clone();
                    dynamic_cast<ASTWithAlias &>(*func.arguments->children[1]).alias = table_id.table_name;
                }
            }
        }
    }
}

}
