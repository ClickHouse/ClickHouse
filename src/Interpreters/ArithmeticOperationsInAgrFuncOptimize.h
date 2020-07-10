#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

/// It converts some arithmetic. Optimization due to the linearity property of some aggregate functions.
/// Function collects const and not const nodes and rebuilds old tree.
class ArithmeticOperationsInAgrFuncMatcher
{
public:
    struct Data {};

    static void visit(const ASTPtr & ast, Data data);
    static void visit(ASTFunction *, Data data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);

};
using ArithmeticOperationsInAgrFuncVisitor = InDepthNodeVisitor<ArithmeticOperationsInAgrFuncMatcher, true>;
}
