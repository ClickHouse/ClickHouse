#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;
class ColumnsDescription;
class ASTIdentifier;

class ReplaceAliasToExprMatcher
{
public:
    struct Data
    {
        const ColumnsDescription & columns;
    };

    static void visit(ASTPtr & ast, Data &);
    static void visit(const ASTIdentifier &, ASTPtr & ast, Data &);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

using ReplaceAliasToExprVisitor = InDepthNodeVisitor<ReplaceAliasToExprMatcher, true>;
}
