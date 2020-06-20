#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class InjectiveFunctionsInsideUniqMatcher
{
public:
    struct Data
    {
        const Context & context;
    };

    static void visit(ASTPtr & ast, Data data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
};
using InjectiveFunctionsInsideUniqVisitor = InDepthNodeVisitor<InjectiveFunctionsInsideUniqMatcher, true>;
}
