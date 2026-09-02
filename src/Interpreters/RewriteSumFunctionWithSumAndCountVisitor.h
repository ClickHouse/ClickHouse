#pragma once

#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;

class RewriteSumFunctionWithSumAndCountMatcher
{
public:
    struct Data
    {
        const TablesWithColumns & tables;
    };

    static void visit(ASTPtr & ast, const Data & data);
    static void visit(const ASTFunction &, ASTPtr & ast, const Data & data);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

using RewriteSumFunctionWithSumAndCountVisitor = InDepthNodeVisitor<RewriteSumFunctionWithSumAndCountMatcher, true>;
}
