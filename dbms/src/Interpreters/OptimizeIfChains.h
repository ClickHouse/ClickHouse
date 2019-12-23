#ifndef CLICKHOUSE_OPTIMIZEIFCHAINS_H
#define CLICKHOUSE_OPTIMIZEIFCHAINS_H

#endif //CLICKHOUSE_OPTIMIZEIFCHAINS_H
#pragma once

#include <Interpreters/Aliases.h>

namespace DB
{

/// It removes Function_if node from AST if condition is constant.
/// TODO: rewrite with InDepthNodeVisitor
    class OptimizeIfChainsVisitor
    {
    public:
        OptimizeIfChainsVisitor(Aliases & aliases_)
                : aliases(aliases_)
        {}

        void visit(ASTPtr & ast);

        ASTs if_chain(ASTPtr & child);

    private:
        Aliases & aliases;
    };

}