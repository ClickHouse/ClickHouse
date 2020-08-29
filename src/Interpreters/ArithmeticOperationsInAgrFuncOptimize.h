#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;

/// Extract constant arguments out of aggregate functions from child functions
/// 'sum(a * 2)' -> 'sum(a) * 2'
/// Rewrites:   sum([multiply|divide]) -> [multiply|divide](sum)
///             [min|max]([multiply|divide|plus|minus]) -> [multiply|divide|plus|minus]([min|max])
/// TODO: groupBitAnd, groupBitOr, groupBitXor
/// TODO: better constant detection: f(const) is not detected as const.
/// TODO: 'f((2 * n) * n)' -> '2 * f(n * n)'
class ArithmeticOperationsInAgrFuncMatcher
{
public:
    struct Data {};

    static void visit(ASTPtr & ast, Data & data);
    static void visit(const ASTFunction &, ASTPtr & ast, Data & data);

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
};

using ArithmeticOperationsInAgrFuncVisitor = InDepthNodeVisitor<ArithmeticOperationsInAgrFuncMatcher, false>;

}
