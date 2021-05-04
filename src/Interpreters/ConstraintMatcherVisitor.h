#pragma once

#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

struct ConstraintMatcher
{
    struct Data
    {
        std::unordered_map<UInt64, std::vector<ASTPtr>> constraints;
    };

    using Visitor = InDepthNodeVisitor<ConstraintMatcher, true>;

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &);
    static std::optional<bool> getASTValue(const ASTPtr & node, Data & data);
    static void visit(ASTPtr & ast, Data & data);
};

using ConstraintMatcherVisitor = InDepthNodeVisitor<ConstraintMatcher, true>;

}
