#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/// It converts if-chain to multiIf.
class OptimizeIfChainsVisitor
{
public:
    OptimizeIfChainsVisitor() = default;
    void visit(ASTPtr & ast);

private:
    ASTs ifChain(const ASTPtr & child);
};

}
