#pragma once

#include <unordered_set>

#include <Parsers/IAST_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>

#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Core/SettingsEnums.h>


namespace DB
{

class ASTFunction;
class ASTSelectWithUnionQuery;

class SelectIntersectExceptQueryMatcher
{
public:
    struct Data
    {
        const SetOperationMode intersect_default_mode;
        const SetOperationMode except_default_mode;
    };

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & ast, Data &);
    static void visit(ASTSelectWithUnionQuery &, Data &);
};

/// Visit children first.
using SelectIntersectExceptQueryVisitor
    = InDepthNodeVisitor<SelectIntersectExceptQueryMatcher, false>;
}
