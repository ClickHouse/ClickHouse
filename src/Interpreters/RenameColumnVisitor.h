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
    /// Also rename the column in a reference to its subcolumn, such as `column_name.x`.
    bool rename_subcolumns = false;
    /// With `rename_subcolumns`: the columns of the table. A name `column_name.x` is renamed only if it is not a column of
    /// the table itself or a subcolumn of one.
    const ColumnsDescription * columns = nullptr;

    void visit(ASTIdentifier & identifier, ASTPtr & ast) const;
};

using RenameColumnMatcher = OneTypeMatcher<RenameColumnData>;
using RenameColumnVisitor = InDepthNodeVisitor<RenameColumnMatcher, true>;
}
