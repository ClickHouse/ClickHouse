#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTLiteral.h>
#include <Common/SettingSource.h>
#include <Common/checkStackSize.h>
#include <Core/Settings.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_global_with_statement;
}

namespace
{

/// A name is looked up in an enclosing scope only while `enable_global_with_statement` holds in the
/// subquery's own context, so a `WITH` element that a subquery does not see is a table name there.
/// The clause is clamped rather than rejected, so a subquery cannot widen the reader's constraints,
/// and it is applied to a copy, so the AST keeps the clause as written.
ContextPtr getSubqueryContext(const ASTSelectQuery & select, const ContextPtr & context)
{
    auto settings_ast = select.settings();
    if (!settings_ast)
        return context;

    auto changes = settings_ast->as<const ASTSetQuery &>().changes;
    auto subquery_context = Context::createCopy(context);
    subquery_context->clampToSettingsConstraints(changes, SettingSource::QUERY);
    subquery_context->applySettingsChanges(changes);
    return subquery_context;
}

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
    /// The elements this select declares itself are registered below either way: only the inherited
    /// ones are out of scope here.
    std::optional<Data> scope_data;
    if (data.context)
    {
        scope_data = data;
        scope_data->context = getSubqueryContext(ast, data.context);
        if (!scope_data->context->getSettingsRef()[Setting::enable_global_with_statement])
            scope_data->subqueries.clear();
    }
    const Data & scope = scope_data ? *scope_data : data;

    std::optional<Data> new_data;
    if (auto with = ast.with())
    {
        for (auto & child : with->children)
        {
            visit(child, new_data ? *new_data : scope);
            auto * ast_with_elem = child->as<ASTWithElement>();
            auto child_alias = child->tryGetAlias();
            if (ast_with_elem || !child_alias.empty())
            {
                if (!new_data)
                    new_data = scope;
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
            visit(child, new_data ? *new_data : scope);
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
