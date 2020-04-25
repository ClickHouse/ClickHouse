#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

/// It converts some arithmetic .
class ArithmeticOperationsInAgrFuncMatcher
{
public:
    struct Data
    {
        bool dont_know_why_I_need_it = false;
    };
    ArithmeticOperationsInAgrFuncMatcher() = default;

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);

};
using ArithmeticOperationsInAgrFuncVisitor = InDepthNodeVisitor<ArithmeticOperationsInAgrFuncMatcher, true>;
}
