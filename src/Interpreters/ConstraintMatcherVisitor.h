#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST.h>
#include <Poco/Logger.h>

namespace DB
{

struct ConstraintMatcher
{
    struct Data
    {
        std::unordered_map<UInt64, std::vector<ASTPtr>> constraints;
    };

    using Visitor = InDepthNodeVisitor<ConstraintMatcher, true>;

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &) { return node->as<ASTFunction>() || node->as<ASTExpressionList>(); }

    static bool alwaysTrue(const ASTPtr & node, Data & data) {
        const auto it = data.constraints.find(node->getTreeHash().second);
        if (it != std::end(data.constraints)) {
            for (const auto & ast : it->second) {
                if (node->getColumnName() == ast->getColumnName()) {
                    return true;
                }
            }
        }
        return false;
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (alwaysTrue(ast, data)) {
            ast = std::make_shared<ASTLiteral>(static_cast<UInt8>(1));
        }
    }
};

using ConstraintMatcherVisitor = InDepthNodeVisitor<ConstraintMatcher, true>;

}
