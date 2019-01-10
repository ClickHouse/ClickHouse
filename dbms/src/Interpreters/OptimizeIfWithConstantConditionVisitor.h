#pragma once

#include <unordered_map>

#include <Parsers/IAST.h>

namespace DB
{

/// It removes Function_if node from AST if condition is constant.
/// TODO: rewrite with InDepthNodeVisitor
class OptimizeIfWithConstantConditionVisitor
{
public:
    using Aliases = std::unordered_map<String, ASTPtr>;

    OptimizeIfWithConstantConditionVisitor(Aliases & aliases_)
        : aliases(aliases_)
    {}

    void visit(ASTPtr & ast);

private:
    Aliases & aliases;
};

}
