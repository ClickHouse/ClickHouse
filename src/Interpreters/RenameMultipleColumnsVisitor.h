#pragma once

#include <unordered_map>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTIdentifier;

struct RenameMultipleColumnsData
{
    using TypeToVisit = ASTIdentifier;

    std::unordered_map<String, String> column_rename_map;
    void visit(ASTIdentifier & identifier, ASTPtr & ast) const;
};

using RenameMultipleColumnsMatcher = OneTypeMatcher<RenameMultipleColumnsData>;
using RenameMultipleColumnsVisitor = InDepthNodeVisitor<RenameMultipleColumnsMatcher, true>;
}
