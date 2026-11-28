#pragma once

#include <Core/Names.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTIdentifier;
class ColumnsDescription;

/// Data for RenameColumnVisitor which traverse tree and rename all columns with
/// name column_name to rename_to
struct RenameColumnData
{
    String column_name;
    String rename_to;
    /// The columns of the table before and after the rename. A reference such as `column_name.x` is renamed too if it refers
    /// to a subcolumn of `column_name` before the rename. With `columns_after`, a renamed reference must not refer to another
    /// column or to a lambda parameter after the rename.
    const ColumnsDescription * columns_before = nullptr;
    const ColumnsDescription * columns_after = nullptr;
    /// The parameters of the lambdas around the visited node. A reference to one of them is not renamed.
    Names lambda_parameters;

    void visit(ASTIdentifier & identifier, ASTPtr & ast) const;
};

class RenameColumnMatcher
{
public:
    using Data = RenameColumnData;

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
    static void visit(ASTPtr & ast, Data & data);
};

using RenameColumnVisitor = InDepthNodeVisitor<RenameColumnMatcher, true>;
}
