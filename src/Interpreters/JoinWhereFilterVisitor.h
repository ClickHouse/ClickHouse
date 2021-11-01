#pragma once

#include <cstddef>
#include <vector>
#include <Core/Names.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/Aliases.h>


namespace DB
{

class JoinWhereFilterMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<JoinWhereFilterMatcher, true>;

    struct Data
    {
        const Aliases & aliases;
        std::vector<ASTPtr> filters{};
    };

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (const auto * func = ast->as<ASTFunction>())
            visit(*func, ast, data);
    }

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        const auto * func = node->as<ASTFunction>();
        return !func || func->name == "and";
    }

private:
    static void visit(const ASTFunction & func, const ASTPtr & ast, Data & data);

};

using JoinWhereFilterVisitor = JoinWhereFilterMatcher::Visitor;

}
