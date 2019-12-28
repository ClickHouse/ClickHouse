#pragma once

#include <Interpreters/Aliases.h>

namespace DB
{

/// It converts if-chain to multiIf.
class OptimizeIfChainsVisitor
{
public:
    OptimizeIfChainsVisitor() = default;

    void visit(ASTPtr & ast);

    ASTs IfChain(ASTPtr & child);

};

}
