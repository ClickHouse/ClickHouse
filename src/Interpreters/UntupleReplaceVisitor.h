#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST.h>

namespace DB
{

class UntupleReplaceVisitorData
{
public:
    using TypeToVisit = ASTExpressionList;

    static void visit(ASTExpressionList & node, ASTPtr &);

    static bool needChild(const ASTPtr & node, const ASTPtr &)
    {
        if (node && node->as<TypeToVisit>())
            return false;

        return true;
    }
};

using UntupleReplaceMatcher = OneTypeMatcher<UntupleReplaceVisitorData, UntupleReplaceVisitorData::needChild>;
using UntupleReplaceVisitor = InDepthNodeVisitor<UntupleReplaceMatcher, false>;

}
