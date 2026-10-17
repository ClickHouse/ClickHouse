#include <Interpreters/ColumnAliasesVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/replaceSubcolumnsToGetSubcolumnFunctionInQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <IO/WriteHelpers.h>

#include <algorithm>

namespace DB
{

namespace
{

void collectNames(const IAST & ast, NameSet & names)
{
    if (const auto * identifier = ast.as<ASTIdentifier>())
    {
        names.insert(identifier->name());
        if (identifier->compound())
            names.insert(identifier->name_parts.front());
    }

    if (auto alias = ast.tryGetAlias(); !alias.empty())
        names.insert(alias);

    for (const auto & child : ast.children)
        collectNames(*child, names);
}

/// The names @ast reads from its enclosing scope; names bound to its own lambda parameters are left out.
void collectFreeNames(const IAST & ast, const ColumnsDescription & columns, NameSet & bound, NameSet & names)
{
    if (const auto * function = ast.as<ASTFunction>(); function && function->name == "lambda")
    {
        Names added;
        for (auto & name : RequiredSourceColumnsMatcher::extractNamesFromLambda(*function))
            if (bound.insert(name).second)
                added.push_back(std::move(name));

        collectFreeNames(*function->arguments->children[1], columns, bound, names);

        for (const auto & name : added)
            bound.erase(name);
        return;
    }

    if (const auto * identifier = ast.as<ASTIdentifier>())
    {
        const auto & name = identifier->name();
        const auto & root = identifier->compound() ? identifier->name_parts.front() : name;
        if (!bound.contains(name) && !bound.contains(root))
        {
            names.insert(name);
            /// An ALIAS named `p.f` is expanded, so it does not read `p`.
            const auto * column = columns.tryGet(name);
            if (!identifier->compound() || !column || column->default_desc.kind != ColumnDefaultKind::Alias)
                names.insert(root);
        }
    }

    for (const auto & child : ast.children)
        collectFreeNames(*child, columns, bound, names);
}

ASTPtr expandedDefinition(const ColumnDescription & column, const ColumnAliasesMatcher::Data & data)
{
    auto definition = column.default_desc.expression->clone();
    /// Only the fields of lambda parameters: with no table columns the rewrite leaves every other identifier.
    if (data.rename_lambda_parameters)
        replaceSubcolumnsToGetSubcolumnFunctionInQuery(definition, {});
    return addTypeConversionToAST(std::move(definition), column.type->getName(), data.columns.getAll(), data.context);
}

/// The free names the ALIAS definitions reachable from @ast bring along when expanded.
/// An identifier bound to a lambda parameter is not an ALIAS reference (a field of a parameter only forces the rename).
void collectReachableAliasNames(const IAST & ast, const ColumnAliasesMatcher::Data & data, NameSet & bound, NameSet & names, NameSet & visited)
{
    if (const auto * function = ast.as<ASTFunction>(); function && function->name == "lambda")
    {
        Names added;
        for (auto & name : RequiredSourceColumnsMatcher::extractNamesFromLambda(*function))
            if (bound.insert(name).second)
                added.push_back(std::move(name));

        collectReachableAliasNames(*function->arguments->children[1], data, bound, names, visited);

        for (const auto & name : added)
            bound.erase(name);
        return;
    }

    if (const auto * identifier = ast.as<ASTIdentifier>())
    {
        const auto & name = identifier->name();
        if (bound.contains(name) || !data.columns.has(name))
            return;

        const auto & column = data.columns.get(name);
        if (column.default_desc.kind != ColumnDefaultKind::Alias || !column.default_desc.expression)
            return;

        if (identifier->compound() && bound.contains(identifier->name_parts.front()))
        {
            names.insert(identifier->name_parts.front());
            return;
        }

        if (visited.contains(name))
            return;

        visited.insert(name);
        names.insert(name);

        /// The expanded copy: inlining a SQL UDF while analysing it can add names the stored definition lacks.
        auto definition = expandedDefinition(column, data);
        NameSet table_scope;
        collectFreeNames(*definition, data.columns, table_scope, names);
        collectReachableAliasNames(*definition, data, table_scope, names, visited);
        return;
    }

    for (const auto & child : ast.children)
        collectReachableAliasNames(*child, data, bound, names, visited);
}

void renameBoundIdentifiers(ASTPtr & ast, const String & from, const String & to)
{
    if (const auto * function = ast->as<ASTFunction>(); function && function->name == "lambda")
    {
        auto parameters = RequiredSourceColumnsMatcher::extractNamesFromLambda(*function);
        if (std::ranges::find(parameters, from) != parameters.end())
            return;
    }

    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        if (identifier->name_parts.empty() || identifier->name_parts.front() != from)
            return;

        if (identifier->isShort())
        {
            identifier->setShortName(to);
        }
        else
        {
            auto parts = identifier->name_parts;
            parts.front() = to;
            auto renamed = make_intrusive<ASTIdentifier>(std::move(parts));
            renamed->setAlias(identifier->tryGetAlias());
            ast = std::move(renamed);
        }
        return;
    }

    for (auto & child : ast->children)
        renameBoundIdentifiers(child, from, to);
}

void renameCapturingParameters(ASTFunction & lambda, ColumnAliasesMatcher::Data & data)
{
    auto parameters = RequiredSourceColumnsMatcher::extractNamesFromLambda(lambda);
    auto & body = lambda.arguments->children[1];

    NameSet bound = data.private_aliases;
    bound.insert(parameters.begin(), parameters.end());

    NameSet capturable;
    NameSet visited;
    collectReachableAliasNames(*body, data, bound, capturable, visited);

    if (std::ranges::none_of(parameters, [&](const auto & parameter) { return capturable.contains(parameter); }))
        return;

    NameSet taken = capturable;
    collectNames(lambda, taken);

    for (auto & parameter : lambda.arguments->children[0]->as<ASTFunction &>().arguments->children)
    {
        auto & identifier = parameter->as<ASTIdentifier &>();
        const String name = identifier.name();
        if (!capturable.contains(name))
            continue;

        String fresh;
        for (size_t n = 1;; ++n)
        {
            fresh = name + "_" + toString(n);
            if (!taken.contains(fresh) && !data.generated_names.contains(fresh) && !data.columns.has(fresh))
                break;
        }

        data.generated_names.insert(fresh);
        renameBoundIdentifiers(body, name, fresh);
        identifier.setShortName(fresh);
    }
}

}

bool ColumnAliasesMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &, const Data & data)
{
    if (data.excluded_nodes.contains(node.get()))
        return false;

    if (const auto * f = node->as<ASTFunction>())
    {
        /// "lambda" visits children itself.
        if (f->name == "lambda")
            return false;
    }

    return !(node->as<ASTTableExpression>()
            || node->as<ASTSubquery>()
            || node->as<ASTArrayJoin>());
}

void ColumnAliasesMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * func = ast->as<ASTFunction>())
        visit(*func, ast, data);
    else if (auto * ident = ast->as<ASTIdentifier>())
        visit(*ident, ast, data);
}

void ColumnAliasesMatcher::visit(ASTFunction & node, ASTPtr & /*ast*/, Data & data)
{
    /// Do not add formal parameters of the lambda expression
    if (node.name == "lambda")
    {
        if (data.rename_lambda_parameters)
            renameCapturingParameters(node, data);

        Names local_aliases;
        auto names_from_lambda = RequiredSourceColumnsMatcher::extractNamesFromLambda(node);
        for (const auto & name : names_from_lambda)
        {
            if (data.private_aliases.insert(name).second)
            {
                local_aliases.push_back(name);
            }
        }
        /// visit child with masked local aliases
        Visitor(data).visit(node.arguments->children[1]);
        for (const auto & name : local_aliases)
            data.private_aliases.erase(name);
    }
}

void ColumnAliasesMatcher::visit(ASTIdentifier & node, ASTPtr & ast, Data & data)
{
    if (auto column_name = IdentifierSemantic::getColumnName(node))
    {
        if (data.array_join_result_columns.contains(*column_name) || data.array_join_source_columns.contains(*column_name)
            || data.private_aliases.contains(*column_name) || !data.columns.has(*column_name))
            return;

        const auto & col = data.columns.get(*column_name);
        if (col.default_desc.kind == ColumnDefaultKind::Alias)
        {
            auto alias = node.tryGetAlias();
            auto original_column = col.default_desc.expression->getColumnName();
            // If expanded alias is used in array join, avoid expansion, otherwise the column will be mis-array joined
            if (data.array_join_result_columns.contains(original_column) || data.array_join_source_columns.contains(original_column))
                return;
            ast = expandedDefinition(col, data);
            // We need to set back the original column name, or else the process of naming resolution will complain.
            if (!alias.empty())
                ast->setAlias(alias);
            else
                ast->setAlias(*column_name);

            data.changed = true;
            // revisit ast to track recursive alias columns
            Visitor(data).visit(ast);
        }
    }
}


}
