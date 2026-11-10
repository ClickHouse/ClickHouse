#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTFunction;

/// Removes unneeded injective functions inside `uniq*()`.
class RemoveInjectiveFunctionsMatcher
{
public:
    struct Data : public WithContext
    {
        explicit Data(ContextPtr context_) : WithContext(context_) {}
    };

    static void visit(ASTPtr & ast, const Data & data);
    static void visit(ASTFunction &, ASTPtr & ast, const Data & data);

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
};

using RemoveInjectiveFunctionsVisitor = InDepthNodeVisitor<RemoveInjectiveFunctionsMatcher, true>;

}
