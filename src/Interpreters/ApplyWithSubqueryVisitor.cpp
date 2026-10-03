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

#include <algorithm>


namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_global_with_statement;
    extern const SettingsBool enable_scopes_for_with_statement;
}

namespace
{

/// A name is looked up in an enclosing scope only while `enable_global_with_statement` holds in the
/// subquery's own context, so a CTE name that a subquery does not see is a table name there.
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

bool hasOwnAliasScope(const IAST & ast)
{
    if (ast.as<ASTSubquery>() || ast.as<ASTSelectQuery>() || ast.as<ASTSelectWithUnionQuery>())
        return true;
    const auto * function = ast.as<ASTFunction>();
    return function && function->name == "lambda";
}

void forEachDescendantAlias(const ASTPtr & ast, const std::function<void(const String &, const ASTPtr &)> & callback)
{
    checkStackSize();

    for (const auto & child : ast->children)
    {
        if (auto alias = child->tryGetAlias(); !alias.empty())
            callback(alias, child);
        if (!hasOwnAliasScope(*child))
            forEachDescendantAlias(child, callback);
    }
}

void registerExpressionAlias(const ASTPtr & node, ApplyWithSubqueryVisitor::Data & data, bool export_aliases)
{
    auto alias = node->tryGetAlias();
    if (alias.empty())
        return;
    data.literals[alias] = node;
    if (export_aliases)
        data.exported_literals[alias] = node;
}

}

void ApplyWithSubqueryVisitor::forEachExpressionAlias(
    const ASTPtr & expression, const std::function<void(const String &, const ASTPtr &)> & callback)
{
    if (!hasOwnAliasScope(*expression))
        forEachDescendantAlias(expression, callback);
    if (auto alias = expression->tryGetAlias(); !alias.empty())
        callback(alias, expression);
}

void ApplyWithSubqueryVisitor::visit(ASTPtr & ast, const Data & data)
{
    checkStackSize();

    if (auto * node_select = ast->as<ASTSelectQuery>())
        visit(*node_select, data);
    else
    {
        /// A lambda parameter and an alias declared in the lambda body hide an inherited alias of the same name.
        std::optional<Data> lambda_data;
        if (const auto * lambda = ast->as<ASTFunction>(); lambda && isASTLambdaFunction(*lambda) && !data.literals.empty())
        {
            std::vector<String> names;
            for (const auto & parameter : lambda->arguments->children[0]->as<ASTFunction &>().arguments->children)
            {
                if (const auto * identifier = parameter->as<ASTIdentifier>())
                    names.push_back(identifier->name());
            }
            forEachExpressionAlias(lambda->arguments->children[1], [&](const String & alias, const ASTPtr &) { names.push_back(alias); });
            for (const auto & name : names)
            {
                if (!data.literals.contains(name))
                    continue;
                if (!lambda_data)
                    lambda_data = data;
                lambda_data->literals.erase(name);
            }
        }
        const Data & scope = lambda_data ? *lambda_data : data;

        for (auto & child : ast->children)
            visit(child, scope);
        if (auto * node_func = ast->as<ASTFunction>())
            visit(*node_func, scope);
        else if (auto * node_table = ast->as<ASTTableExpression>())
            visit(*node_table, scope);
    }
}

/// Like `visit`, and registers each alias the expression declares as soon as the aliased node is visited, so that
/// the rest of the expression sees it.
void ApplyWithSubqueryVisitor::visitWithExpression(ASTPtr & ast, Data & data, bool export_aliases)
{
    checkStackSize();

    if (hasOwnAliasScope(*ast))
        visit(ast, data);
    else
    {
        for (auto & child : ast->children)
            visitWithExpression(child, data, export_aliases);
        if (auto * node_func = ast->as<ASTFunction>())
        {
            visit(*node_func, data);
            /// `visit` can replace an argument with a copy of a registered node.
            if (node_func->arguments)
            {
                for (const auto & argument : node_func->arguments->children)
                    registerExpressionAlias(argument, data, export_aliases);
            }
        }
        else if (auto * node_table = ast->as<ASTTableExpression>())
            visit(*node_table, data);
    }

    registerExpressionAlias(ast, data, export_aliases);
}

void ApplyWithSubqueryVisitor::visit(ASTSelectQuery & ast, const Data & data)
{
    /// The elements this select declares itself are registered below either way: only the inherited
    /// ones are out of scope here.
    /// An alias this select declares itself hides an inherited one of the same name.
    std::vector<String> own_aliases;
    auto add_own_alias = [&](const String & alias, const ASTPtr &) { own_aliases.push_back(alias); };
    for (const auto & child : ast.children)
    {
        if (child != ast.tables())
            forEachExpressionAlias(child, add_own_alias);
    }
    /// So do `ARRAY JOIN` and `JOIN ... ON`, unlike a table expression.
    if (ast.tables())
    {
        for (const auto & element : ast.tables()->children)
        {
            const auto * table_element = element->as<ASTTablesInSelectQueryElement>();
            if (!table_element)
                continue;
            if (const auto * array_join = table_element->array_join ? table_element->array_join->as<ASTArrayJoin>() : nullptr;
                array_join && array_join->expression_list)
                forEachExpressionAlias(array_join->expression_list, add_own_alias);
            if (const auto * table_join = table_element->table_join ? table_element->table_join->as<ASTTableJoin>() : nullptr;
                table_join && table_join->on_expression)
                forEachExpressionAlias(table_join->on_expression, add_own_alias);
        }
    }

    std::optional<Data> scope_data;
    if (data.context || std::ranges::any_of(own_aliases, [&](const String & alias) { return data.literals.contains(alias); }))
    {
        scope_data = data;
        for (const auto & alias : own_aliases)
            scope_data->literals.erase(alias);
    }
    if (data.context)
    {
        scope_data->context = getSubqueryContext(ast, data.context);
        const auto & scope_settings = scope_data->context->getSettingsRef();
        /// A common table expression is reached by looking into an enclosing scope, so a select that does
        /// not look there cannot name one. An expression alias declared with scopes disabled is instead
        /// copied down, and a select reads that copy when it disables them too.
        if (!scope_settings[Setting::enable_global_with_statement])
        {
            scope_data->subqueries.clear();
            scope_data->literals.clear();
        }
        if (!scope_settings[Setting::enable_scopes_for_with_statement])
        {
            for (const auto & [name, node] : scope_data->exported_literals)
                scope_data->literals[name] = node;
        }
    }
    const Data & scope = scope_data ? *scope_data : data;

    std::optional<Data> new_data;
    if (auto with = ast.with())
    {
        const bool export_aliases
            = scope.context && !scope.context->getSettingsRef()[Setting::enable_scopes_for_with_statement];
        for (auto & child : with->children)
        {
            if (auto * ast_with_elem = child->as<ASTWithElement>())
            {
                visit(child, new_data ? *new_data : scope);
                if (!new_data)
                    new_data = scope;
                new_data->subqueries[ast_with_elem->name] = ast_with_elem->subquery;
            }
            else
            {
                if (!new_data)
                    new_data = scope;
                visitWithExpression(child, *new_data, export_aliases);
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
