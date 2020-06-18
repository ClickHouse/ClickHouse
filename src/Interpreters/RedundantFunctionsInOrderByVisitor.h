#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{

class RedundantFunctionsInOrderByMatcher
{
public:
    struct Data
    {
        std::unordered_set<String> & keys;
        bool should_be_erased = false;
        bool done = false;
    };

    static void visit(const ASTPtr & ast, Data & data)
    {
        auto * ast_function = ast->as<ASTFunction>();
        if (ast_function)
        {
            visit(*ast_function, data);
        }
    }

    static void visit(ASTFunction & ast_function, Data & data)
    {
        if (data.done)
            return;

        auto arguments = ast_function.arguments;

        if (arguments->children.size() != 1)
        {
            data.done = true;
            return;
        }

        auto * identifier = arguments->children[0]->as<ASTIdentifier>();
        if (!identifier)
            return;

        if (data.keys.count(getIdentifierName(identifier)))
            data.should_be_erased = true;
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return true;
    }

};

using RedundantFunctionsInOrderByVisitor = InDepthNodeVisitor<RedundantFunctionsInOrderByMatcher, true>;

}
