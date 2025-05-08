#include <Core/Settings.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTLiteral.h>
#include <Common/checkStackSize.h>


namespace DB
{

namespace Setting
{
extern const SettingsBool allow_experimental_analyzer;
}

namespace ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
}

ApplyWithSubqueryVisitor::ApplyWithSubqueryVisitor(ContextPtr context_)
    : use_analyzer(context_->getSettingsRef()[Setting::allow_experimental_analyzer])
{
}

void ApplyWithSubqueryVisitor::visit(ASTPtr & ast, const Data & data)
{
    checkStackSize();

    if (auto * node_select = ast->as<ASTSelectQuery>())
        visit(*node_select, data);
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

void ApplyWithSubqueryVisitor::visit(ASTSelectQuery & ast, const Data & data)
{
    /// This is probably not the best place to check this, but it's just to throw a proper error to the user
    if (!use_analyzer && ast.recursive_with)
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD, "WITH RECURSIVE is not supported with the old analyzer. Please use `enable_analyzer=1`");

    std::optional<Data> new_data;
    if (auto with = ast.with())
    {
        for (auto & child : with->children)
        {
            visit(child, new_data ? *new_data : data);
            auto * ast_with_elem = child->as<ASTWithElement>();
            auto * ast_literal = child->as<ASTLiteral>();
            if (ast_with_elem || ast_literal)
            {
                if (!new_data)
                    new_data = data;
                if (ast_with_elem)
                    new_data->subqueries[ast_with_elem->name] = ast_with_elem->subquery;
                else
                    new_data->literals[ast_literal->alias] = child;
            }
        }
    }

    for (auto & child : ast.children)
    {
        if (child != ast.with())
            visit(child, new_data ? *new_data : data);
    }
}

void ApplyWithSubqueryVisitor::visit(ASTSelectWithUnionQuery & ast, const Data & data)
{
    for (auto & child : ast.children)
        visit(child, data);
}

void ApplyWithSubqueryVisitor::visit(ASTTableExpression & table, const Data & data)
{
    if (table.database_and_table_name)
    {
        auto table_id = table.database_and_table_name->as<ASTTableIdentifier>()->getTableId();
        if (table_id.database_name.empty())
        {
            auto subquery_it = data.subqueries.find(table_id.table_name);
            if (subquery_it != data.subqueries.end())
            {
                auto old_alias = table.database_and_table_name->tryGetAlias();
                table.children.clear();
                table.database_and_table_name.reset();
                table.subquery = subquery_it->second->clone();
                table.subquery->as<ASTSubquery &>().cte_name = table_id.table_name;
                if (!old_alias.empty())
                    table.subquery->setAlias(old_alias);
                table.children.emplace_back(table.subquery);
            }
        }
    }
}

void ApplyWithSubqueryVisitor::visit(ASTFunction & func, const Data & data)
{
    /// Special CTE case, where the right argument of IN is alias (ASTIdentifier) from WITH clause.

    if (checkFunctionIsInOrGlobalInOperator(func))
    {
        auto & ast = func.arguments->children.at(1);
        if (const auto * identifier = ast->as<ASTIdentifier>())
        {
            if (identifier->isShort())
            {
                /// Clang-tidy is wrong on this line, because `func.arguments->children.at(1)` gets replaced before last use of `name`.
                auto name = identifier->shortName();  // NOLINT

                auto subquery_it = data.subqueries.find(name);
                if (subquery_it != data.subqueries.end())
                {
                    auto old_alias = func.arguments->children[1]->tryGetAlias();
                    func.arguments->children[1] = subquery_it->second->clone();
                    func.arguments->children[1]->as<ASTSubquery>()->cte_name = name;
                    if (!old_alias.empty())
                        func.arguments->children[1]->setAlias(old_alias);
                }
                else
                {
                    auto literal_it = data.literals.find(name);
                    if (literal_it != data.literals.end())
                    {
                        auto old_alias = func.arguments->children[1]->tryGetAlias();
                        func.arguments->children[1] = literal_it->second->clone();
                        if (!old_alias.empty())
                            func.arguments->children[1]->setAlias(old_alias);
                    }
                }
            }
        }
    }
}

}
