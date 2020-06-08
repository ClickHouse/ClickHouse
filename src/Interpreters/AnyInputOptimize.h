#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

///This optimiser is similar to ArithmeticOperationsInAgrFunc optimizer, but for function any we can extract any functions.
class AnyInputMatcher
{
public:
    struct Data {};

    static void visit(ASTPtr & ast, Data data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
};
using AnyInputVisitor = InDepthNodeVisitor<AnyInputMatcher, true>;
}
