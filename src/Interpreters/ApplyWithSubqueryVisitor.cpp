#include <Interpreters/ApplyWithSubqueryVisitor.h>
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
    std::optional<Data> new_data;
    if (auto with = ast.with())
    {
        for (auto & child : with->children)
        {
            auto * ast_with_elem = child->as<ASTWithElement>();
            if (ast.recursive_with && ast_with_elem)
                visitRecursiveWithElement(*ast_with_elem, new_data ? *new_data : data);
            else
                visit(child, new_data ? *new_data : data);
            auto child_alias = child->tryGetAlias();
            if (ast_with_elem || !child_alias.empty())
            {
                if (!new_data)
                    new_data = data;
                if (ast_with_elem)
                    new_data->subqueries[ast_with_elem->name] = ast_with_elem->subquery;
                else
                    new_data->literals[child_alias] = child;
            }
        }
    }

    for (auto & child : ast.children)
    {
        if (child != ast.with())
            visit(child, new_data ? *new_data : data);
    }
}

/// The recursive members of a recursive element, every `UNION` branch after the first, reference
/// the element itself, so there its name must not be replaced by the body of a same-named element
/// of an enclosing `SELECT`. The first branch is the seed, which the analyzer resolves like any
/// other query, so an enclosing element stays visible in it.
void ApplyWithSubqueryVisitor::visitRecursiveWithElement(ASTWithElement & with_element, const Data & data)
{
    auto * union_query = with_element.subquery && !with_element.subquery->children.empty()
        ? with_element.subquery->children.front()->as<ASTSelectWithUnionQuery>()
        : nullptr;
    bool shadows = data.subqueries.contains(with_element.name) || data.literals.contains(with_element.name);
    if (!union_query || !union_query->list_of_selects || union_query->list_of_selects->children.size() < 2 || !shadows)
    {
        visit(with_element.subquery, data);
        return;
    }

    Data shadowed = data;
    shadowed.subqueries.erase(with_element.name);
    shadowed.literals.erase(with_element.name);

    auto & branches = union_query->list_of_selects->children;
    visit(branches.front(), data);
    for (size_t i = 1; i < branches.size(); ++i)
        visit(branches[i], shadowed);
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
    /// Rewrite dictionary name in dictGet*()
    else if (functionIsDictGet(func.name) && !func.arguments->children.empty())
    {
        auto & dict_name_arg = func.arguments->children.at(0);
        if (const auto * identifier = dict_name_arg->as<ASTIdentifier>(); identifier && identifier->isShort())
        {
            auto name = identifier->shortName();

            auto literal_it = data.literals.find(name);
            if (literal_it != data.literals.end())
            {
                auto old_alias = dict_name_arg->tryGetAlias();
                dict_name_arg = literal_it->second->clone();
                /// Always reset the alias name, otherwise the aliases will not match after AddDefaultDatabaseVisitor
                dict_name_arg->setAlias(old_alias);
            }
        }
    }
}

}
