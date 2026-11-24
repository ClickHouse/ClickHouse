#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

/// It converts if-chain to multiIf.
/// Note that this is a purely syntactic rewrite: the synthesized `multiIf` is resolved later, from the
/// settings of the query context. It therefore must not be applied when
/// `optimize_if_transform_const_strings_to_lowcardinality` is in effect, because a `multiIf` with constant
/// string branches resolves to `LowCardinality(String)` while the `if` chain it replaces resolves to plain
/// `String`. See the caller in `TreeRewriter`.
class OptimizeIfChainsVisitor
{
public:
    OptimizeIfChainsVisitor() = default;
    void visit(ASTPtr & ast);

private:
    ASTs ifChain(const ASTPtr & child);
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
