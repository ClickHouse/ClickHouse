#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>

namespace DB
{
/// Data for RenameFunctionAliasesVisitor which traverse tree and rename all function alias with
/// name inside aliases_name vector to corresponding value in the renames_to vector
struct RenameFunctionAliasesData
{
    using TypeToVisit = ASTFunction;

    Strings aliases_name;
    Strings renames_to;

    void visit(ASTFunction & function, ASTPtr & ast) const;
};

using RenameFunctionAliasesMatcher = OneTypeMatcher<RenameFunctionAliasesData>;
using RenameFunctionAliasesVisitor = InDepthNodeVisitor<RenameFunctionAliasesMatcher, true>;

}
