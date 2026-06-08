#include <Core/Names.h>
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

namespace
{

/// Recursively replace short identifiers referring to `WITH` literal aliases with the corresponding literals.
/// This is used for the `eval` table function argument: it is evaluated as a constant expression while the
/// table function is resolved, which happens before the query normalizer (the usual place where alias
/// substitution occurs) runs. Subqueries are skipped, because they are separate queries with their own
/// alias scope and are handled by the select-query traversal.
void substituteWithLiterals(ASTPtr & ast, const std::map<String, ASTPtr> & literals)
{
    if (ast->as<ASTSelectWithUnionQuery>() || ast->as<ASTSelectQuery>() || ast->as<ASTSubquery>())
        return;

    if (const auto * function = ast->as<ASTFunction>(); function && isASTLambdaFunction(*function))
    {
        const auto & lambda_arguments = function->arguments->children;
        const auto * lambda_argument_tuple = lambda_arguments.at(0)->as<ASTFunction>();
        chassert(lambda_argument_tuple);

        NameSet lambda_argument_names;
        for (const auto & lambda_argument : lambda_argument_tuple->arguments->children)
            if (auto name = tryGetIdentifierName(lambda_argument))
                lambda_argument_names.insert(*name);

        auto scoped_literals = literals;
        for (const auto & name : lambda_argument_names)
            scoped_literals.erase(name);

        substituteWithLiterals(function->arguments->children.at(1), scoped_literals);
        return;
    }

    if (const auto * identifier = ast->as<ASTIdentifier>(); identifier && identifier->isShort())
    {
        auto literal_it = literals.find(identifier->shortName());
        if (literal_it != literals.end())
        {
            auto old_alias = ast->tryGetAlias();
            ast = literal_it->second->clone();
            /// Reset the alias coming from the `WITH` element, keeping the original alias of the identifier (if any).
            ast->setAlias(old_alias);
            return;
        }
    }

    for (auto & child : ast->children)
        substituteWithLiterals(child, literals);
}

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
    /// Substitute `WITH` literal aliases into the `eval` table function argument expression.
    /// `eval`'s argument is evaluated as a constant expression before the query normalizer runs,
    /// so the normalizer's alias substitution would otherwise be missed (e.g. `WITH 'SELECT 1' AS q SELECT * FROM eval(q)`).
    else if (func.name == "eval" && func.arguments)
    {
        for (auto & argument : func.arguments->children)
            substituteWithLiterals(argument, data.literals);
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
