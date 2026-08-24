#pragma once

#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTIdentifier;

/// Data for RenameColumnVisitor which traverse tree and rename all columns with
/// name column_name to rename_to
struct RenameColumnData
{
    using TypeToVisit = ASTIdentifier;

    String column_name;
    String rename_to;

    void visit(ASTIdentifier & identifier, ASTPtr & ast) const;
};

using RenameColumnMatcher = OneTypeMatcher<RenameColumnData>;
using RenameColumnVisitor = InDepthNodeVisitor<RenameColumnMatcher, true>;
}
