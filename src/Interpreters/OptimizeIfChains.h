#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

/// It converts if-chain to multiIf.
class OptimizeIfChainsVisitor
{
public:
    OptimizeIfChainsVisitor() = default;
    void visit(ASTPtr & ast);

private:
    ASTList ifChain(const ASTPtr & child);
};

/// Replaces multiIf with one condition to if,
/// because it's more efficient.
class OptimizeMultiIfToIfData
{
public:
    using TypeToVisit = ASTFunction;

    void visit(ASTFunction & function, ASTPtr &)
    {
        /// 3 args: condition, then branch, else branch.
        if (function.name == "multiIf" && (function.arguments && function.arguments->children.size() == 3))
            function.name = "if";
    }
};

using OptimizeMultiIfToIfMatcher = OneTypeMatcher<OptimizeMultiIfToIfData>;
using OptimizeMultiIfToIfVisitor = InDepthNodeVisitor<OptimizeMultiIfToIfMatcher, true>;

}
