#pragma once

#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;

/// Rewrite the predicates in place
class OptimizeDateFilterInPlaceData
{
public:
    using TypeToVisit = ASTFunction;
    void visit(ASTFunction & function, ASTPtr & ast) const;
};

using OptimizeDateFilterInPlaceMatcher = OneTypeMatcher<OptimizeDateFilterInPlaceData>;
using OptimizeDateFilterInPlaceVisitor = InDepthNodeVisitor<OptimizeDateFilterInPlaceMatcher, true>;
}
