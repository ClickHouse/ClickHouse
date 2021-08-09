#include <Interpreters/ColumnAliasesVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

bool ColumnAliasesMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &)
{
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
        if (data.array_join_result_columns.count(*column_name) || data.array_join_source_columns.count(*column_name)
            || data.private_aliases.count(*column_name) || !data.columns.has(*column_name))
            return;

        const auto & col = data.columns.get(*column_name);
        if (col.default_desc.kind == ColumnDefaultKind::Alias)
        {
            auto alias = node.tryGetAlias();
            auto alias_expr = col.default_desc.expression->clone();
            auto original_column = alias_expr->getColumnName();
            // If expanded alias is used in array join, avoid expansion, otherwise the column will be mis-array joined
            if (data.array_join_result_columns.count(original_column) || data.array_join_source_columns.count(original_column))
                return;
            ast = addTypeConversionToAST(std::move(alias_expr), col.type->getName(), data.columns.getAll(), data.context);
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
