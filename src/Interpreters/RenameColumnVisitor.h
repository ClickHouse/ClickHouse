#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{
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

/// Data for RenameColumnsVisitor which traverse tree and rename all columns with
/// name inside columns_name vector to corresponding value in the renames_to vector
struct RenameColumnsData
{
    using TypeToVisit = ASTIdentifier;

    Strings columns_name;
    Strings renames_to;

    void visit(ASTIdentifier & identifier, ASTPtr & ast) const;
};

using RenameColumnsMatcher = OneTypeMatcher<RenameColumnsData>;
using RenameColumnsVisitor = InDepthNodeVisitor<RenameColumnsMatcher, true>;

}
