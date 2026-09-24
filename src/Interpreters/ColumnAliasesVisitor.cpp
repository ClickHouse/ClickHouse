#include <Interpreters/ColumnAliasesVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

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
        Names local_aliases;
        auto names_from_lambda = getASTLambdaArgumentNames(node);
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
            auto alias_expr = cloneAndExpandColumnDefaultExpression(col.default_desc, data.columns);
            validateNoCyclicAliasesAfterExpansion(*column_name, alias_expr, data.columns);
            auto original_column = alias_expr->getColumnName();
            // If expanded alias is used in array join, avoid expansion, otherwise the column will be mis-array joined
            if (data.array_join_result_columns.contains(original_column) || data.array_join_source_columns.contains(original_column))
                return;

            /// Normalize the alias body outside the caller lambda scope.
            /// Lambdas inside the alias body will add their own private aliases.
            auto alias_data = data;
            alias_data.private_aliases.clear();
            alias_data.changed = false;
            Visitor(alias_data).visit(alias_expr);

            /// The alias body was written at table scope, so its identifiers must keep referring
            /// to table columns. Raw AST substitution inside a lambda cannot express that: a name
            /// matching a parameter of an enclosing lambda would be captured and change meaning.
            /// Throw instead of computing wrong values.
            ///
            /// `IndexAnalysis` is exempt: skip index expressions accepted such definitions before,
            /// so `ATTACH` of an existing table must keep working, and the capture stays
            /// self-consistent there - the index is both built and analyzed over the same
            /// substituted expression.
            if (data.replacement_mode == ColumnAliasReplacementMode::QueryAnalysis && !data.private_aliases.empty())
                validateAliasExpansionNotCapturedByLambda(*column_name, alias_expr, data.private_aliases);

            if (data.replacement_mode == ColumnAliasReplacementMode::QueryAnalysis)
            {
                ast = addTypeConversionToAST(std::move(alias_expr), col.type->getName(), data.columns.getAll(), data.context);
                // We need to set back the original column name, or else the process of naming resolution will complain.
                if (!alias.empty())
                    ast->setAlias(alias);
                else
                    ast->setAlias(*column_name);
            }
            else
            {
                /// See the comment on `ColumnAliasReplacementMode::IndexAnalysis`: neither the
                /// type conversion nor the result name may be added for index expressions.
                ast = std::move(alias_expr);
            }

            data.changed = true;
        }
    }
}


}
