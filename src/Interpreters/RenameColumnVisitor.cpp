#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/RenameColumnVisitor.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>

#include <algorithm>

namespace DB
{

namespace
{

void renameIdentifier(ASTIdentifier & identifier, const RenameColumnData & data)
{
    // TODO(ilezhankin): make proper rename
    std::optional<String> identifier_column_name = IdentifierSemantic::getColumnName(identifier);
    if (identifier_column_name && identifier_column_name == data.column_name)
        identifier.setShortName(data.rename_to);
}

/// Renames the column names kept by the `EXCEPT` and `REPLACE` transformers of a matcher, like `a` in
/// `* EXCEPT a` or in `* REPLACE (0 AS a)`.
void renameTransformerColumnNames(const ASTPtr & transformers, const RenameColumnData & data)
{
    if (!transformers)
        return;
    for (const auto & transformer : transformers->children)
    {
        if (transformer->as<ASTColumnsExceptTransformer>())
        {
            for (const auto & column_name : transformer->children)
                if (auto * identifier = column_name->as<ASTIdentifier>())
                    renameIdentifier(*identifier, data);
        }
        else if (transformer->as<ASTColumnsReplaceTransformer>())
        {
            for (const auto & child : transformer->children)
                if (auto * replacement = child->as<ASTColumnsReplaceTransformer::Replacement>())
                    if (replacement->name == data.column_name)
                        replacement->name = data.rename_to;
        }
    }
}

/// The transformers of a matcher that expands to table columns: `*` and `COLUMNS('regexp')`.
/// Unlike them, `COLUMNS(a, b)` lists identifiers that are resolved in the enclosing scope.
ASTPtr getTableColumnsMatcherTransformers(const IAST & ast)
{
    if (const auto * asterisk = ast.as<ASTAsterisk>())
        return asterisk->transformers;
    if (const auto * qualified_asterisk = ast.as<ASTQualifiedAsterisk>())
        return qualified_asterisk->transformers;
    if (const auto * regexp_matcher = ast.as<ASTColumnsRegexpMatcher>())
        return regexp_matcher->transformers;
    if (const auto * qualified_regexp_matcher = ast.as<ASTQualifiedColumnsRegexpMatcher>())
        return qualified_regexp_matcher->transformers;
    return nullptr;
}

}

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
        if (data.rename_identifiers)
            renameIdentifier(*identifier, data);
        return;
    }

    if (isShadowingLambda(*ast, data))
    {
        /// The lambda argument is a local binding, so an identifier with the column's name inside the
        /// lambda refers to the argument. But the column names kept by the transformers of `*`, like
        /// `REPLACE (0 AS a)` in `arrayMap(a -> tuple(* REPLACE (0 AS a)), [1])` or `EXCEPT a` in
        /// `arrayMap(a -> tuple(* EXCEPT a), [1])`, still name table columns.
        RenameColumnData nested_data = data;
        nested_data.rename_identifiers = false;
        RenameColumnVisitor visitor(nested_data);
        for (auto & child : ast->children)
            visitor.visit(child);
        return;
    }

    if (!data.rename_identifiers)
    {
        /// Inside a shadowing lambda, the transformers of `*` and `COLUMNS('regexp')` still name table columns.
        /// The transformers of `COLUMNS(a, b)` name the listed identifiers, and `a` among them refers to the
        /// lambda argument - `arrayMap(a -> tuple(COLUMNS(a, b) EXCEPT a), [1])` excludes the argument - so
        /// they are left alone, like the list itself.
        renameTransformerColumnNames(getTableColumnsMatcherTransformers(*ast), data);
    }
    else if (auto * replacement = ast->as<ASTColumnsReplaceTransformer::Replacement>())
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
