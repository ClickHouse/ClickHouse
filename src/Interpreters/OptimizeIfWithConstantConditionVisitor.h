#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{
struct OptimizeIfWithConstantConditionVisitorData
{
    using TypeToVisit = ASTFunction;

    explicit OptimizeIfWithConstantConditionVisitorData(Aliases & aliases_)
        : aliases(aliases_)
    {}

    void visit(ASTFunction & function_node, ASTPtr & ast);
private:
    Aliases & aliases;
};

/// It removes Function_if node from AST if condition is constant.
using OptimizeIfWithConstantConditionVisitor = InDepthNodeVisitor<OneTypeMatcher<OptimizeIfWithConstantConditionVisitorData>, false>;

}
