#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{
/// Rename ASTIdentifiers to column name
struct RenameColumnData
{
    using TypeToVisit = ASTIdentifier;

    String column_name;
    String rename_to;

    void visit(ASTIdentifier & identifier, ASTPtr & ast);
};

using RenameColumnMatcher = OneTypeMatcher<RenameColumnData>;
using RenameColumnVisitor = InDepthNodeVisitor<RenameColumnMatcher, true>;
}
