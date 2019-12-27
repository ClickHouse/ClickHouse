#pragma once

#include <Interpreters/Aliases.h>

namespace DB
{

/// It converts if-chain to multiIf.
class OptimizeIfChainsVisitor
{
public:
    OptimizeIfChainsVisitor(Aliases & aliases_)
            : aliases(aliases_)
    {}

    void visit(ASTPtr & ast);

    ASTs IfChain(ASTPtr & child);

private:
    Aliases & aliases;
};

}
