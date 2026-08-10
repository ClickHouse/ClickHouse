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
    /// A lambda argument is a local binding that shadows the column of the same name, so nothing
    /// inside such a lambda refers to the renamed column.
    if (const auto * function = node->as<ASTFunction>(); function && function->isLambdaFunction())
        return !std::ranges::contains(getASTLambdaArgumentNames(*function), data.column_name);

    return true;
}

void RenameColumnMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        // TODO(ilezhankin): make proper rename
        std::optional<String> identifier_column_name = IdentifierSemantic::getColumnName(*identifier);
        if (identifier_column_name && identifier_column_name == data.column_name)
            identifier->setShortName(data.rename_to);
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
        /// does not reach them on its own. `lambda_arg` is a local binding and is never renamed.
        if (apply->lambda && apply->lambda_arg != data.column_name)
        {
            RenameColumnVisitor visitor(data);
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
