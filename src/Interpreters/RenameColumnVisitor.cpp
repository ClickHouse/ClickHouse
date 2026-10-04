#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/RenameColumnVisitor.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

#include <algorithm>

namespace DB
{

bool RenameColumnMatcher::needChildVisit(const ASTPtr & node, const ASTPtr & /*child*/, const Data & data)
{
    /// A lambda whose argument shadows the renamed column is descended into by `visit` instead,
    /// with the renaming of identifiers turned off.
    return !isShadowingLambda(*node, data);
}

bool RenameColumnMatcher::isShadowingLambda(const IAST & node, const Data & data)
{
    if (!data.rename_identifiers)
        return false;
    const auto * function = node.as<ASTFunction>();
    return function && function->isLambdaFunction()
        && std::ranges::contains(getASTLambdaArgumentNames(*function), data.column_name);
}

void RenameColumnMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        if (!data.rename_identifiers)
            return;
        // TODO(ilezhankin): make proper rename
        std::optional<String> identifier_column_name = IdentifierSemantic::getColumnName(*identifier);
        if (identifier_column_name && identifier_column_name == data.column_name)
            identifier->setShortName(data.rename_to);
        return;
    }

    if (isShadowingLambda(*ast, data))
    {
        /// The lambda argument is a local binding, so an identifier with the column's name inside the
        /// lambda refers to the argument. But the raw column names kept by matcher transformers, like
        /// `REPLACE (0 AS a)` in `arrayMap(a -> tuple(* REPLACE (0 AS a)), [1])`, still name table columns.
        RenameColumnData nested_data = data;
        nested_data.rename_identifiers = false;
        RenameColumnVisitor visitor(nested_data);
        for (auto & child : ast->children)
            visitor.visit(child);
        return;
    }

    if (auto * replacement = ast->as<ASTColumnsReplaceTransformer::Replacement>())
    {
        /// The name of the column the replacement applies to, kept as a raw string.
        if (replacement->name == data.column_name)
            replacement->name = data.rename_to;
        return;
    }

    if (auto * apply = ast->as<ASTColumnsApplyTransformer>())
    {
        /// `lambda` and `parameters` are members rather than children, so the in-depth traversal
        /// does not reach them on its own. `lambda_arg` is a local binding and is never renamed,
        /// and the identifiers it shadows inside the lambda are left alone.
        if (apply->lambda)
        {
            RenameColumnData lambda_data = data;
            if (apply->lambda_arg == data.column_name)
                lambda_data.rename_identifiers = false;
            RenameColumnVisitor visitor(lambda_data);
            visitor.visit(apply->lambda);
        }
        if (apply->parameters)
        {
            RenameColumnVisitor visitor(data);
            visitor.visit(apply->parameters);
        }
    }
}

}
