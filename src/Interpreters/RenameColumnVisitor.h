#pragma once

#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTIdentifier;
class ColumnsDescription;

/// Data for RenameColumnVisitor which traverse tree and rename all columns with
/// name column_name to rename_to
struct RenameColumnData
{
    using TypeToVisit = ASTIdentifier;

    String column_name;
    String rename_to;
    /// The columns of the table before and after the rename. A reference such as `column_name.x` is renamed too if it refers
    /// to a subcolumn of `column_name` before the rename, and must not refer to another column after it.
    const ColumnsDescription * columns_before = nullptr;
    const ColumnsDescription * columns_after = nullptr;

    void visit(ASTIdentifier & identifier, ASTPtr & ast) const;
};

using RenameColumnMatcher = OneTypeMatcher<RenameColumnData>;
using RenameColumnVisitor = InDepthNodeVisitor<RenameColumnMatcher, true>;
}
