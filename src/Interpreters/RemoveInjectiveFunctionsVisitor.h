#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;

/// Removes unneeded injective functions inside `uniq*()`.
class RemoveInjectiveFunctionsMatcher
{
public:
    struct Data
    {
        const Context & context;
    };

    static void visit(ASTPtr & ast, const Data & data);
    static void visit(ASTFunction &, ASTPtr & ast, const Data & data);

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
};

using RemoveInjectiveFunctionsVisitor = InDepthNodeVisitor<RemoveInjectiveFunctionsMatcher, true>;

}
