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
    /// Off inside a lambda whose argument shadows `column_name`: only the raw column names kept by
    /// matcher transformers are renamed there.
    bool rename_identifiers = true;
};

/// Besides plain identifiers, a stored expression can name columns in the raw string fields of the
/// column matcher transformers - `* REPLACE (expr AS name)` keeps the replaced column in
/// `ASTColumnsReplaceTransformer::Replacement::name` - so a rename has to rewrite those too,
/// otherwise the transformer silently stops matching and the expression changes meaning.
/// An `APPLY (x -> ...)` lambda hangs off `ASTColumnsApplyTransformer::lambda` rather than off
/// `children`, so it is descended into explicitly. Lambda arguments are local bindings, not
/// columns: inside a lambda whose argument shadows the renamed column, identifiers are left alone,
/// but the transformer column names are still renamed.
struct RenameColumnMatcher
{
    using Data = RenameColumnData;

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child, const Data & data);
    static void visit(ASTPtr & ast, Data & data);

private:
    static bool isShadowingLambda(const IAST & node, const Data & data);
};

using RenameColumnVisitor = InDepthNodeVisitor<RenameColumnMatcher, true, true>;
}
