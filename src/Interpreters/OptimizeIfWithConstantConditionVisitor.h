#pragma once

#include <Interpreters/Aliases.h>

namespace DB
{

/// It removes Function_if node from AST if condition is constant.
/// TODO: rewrite with InDepthNodeVisitor
class OptimizeIfWithConstantConditionVisitor
{
public:
    explicit OptimizeIfWithConstantConditionVisitor(Aliases & aliases_)
        : aliases(aliases_)
    {}

    void visit(ASTPtr & ast);

private:
    Aliases & aliases;
};

}
