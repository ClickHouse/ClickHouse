#pragma once

#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

/// Data for RenameColumnVisitor which traverse tree and rename all columns with
/// name column_name to rename_to
struct RenameColumnData
{
    String column_name;
    String rename_to;
};

/// Besides plain identifiers, a stored expression can name columns in the raw string fields of the
/// column matcher transformers - `* REPLACE (expr AS name)` keeps the replaced column in
/// `ASTColumnsReplaceTransformer::Replacement::name` - so a rename has to rewrite those too,
/// otherwise the transformer silently stops matching and the expression changes meaning.
/// An `APPLY (x -> ...)` lambda hangs off `ASTColumnsApplyTransformer::lambda` rather than off
/// `children`, so it is descended into explicitly. Lambda arguments are local bindings, not
/// columns: a lambda whose argument shadows the renamed column is left alone entirely.
struct RenameColumnMatcher
{
    using Data = RenameColumnData;

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child, const Data & data);
    static void visit(ASTPtr & ast, Data & data);
};

using RenameColumnVisitor = InDepthNodeVisitor<RenameColumnMatcher, true, true>;
}
