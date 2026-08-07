#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/Aliases.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;
struct ASTTableExpression;

class MarkTableIdentifiersMatcher
{
public:
    using Visitor = InDepthNodeVisitor<MarkTableIdentifiersMatcher, true>;

    struct Data
    {
        const Aliases & aliases;
    };

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);
    static void visit(ASTPtr & ast, Data & data);

private:
    static void visit(ASTFunction & func, const Data & data);
};

using MarkTableIdentifiersVisitor = MarkTableIdentifiersMatcher::Visitor;

}
