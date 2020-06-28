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
        const Context & context;
        bool should_be_erased = true;
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

        if (ast_function.name == "lambda")
        {
            data.should_be_erased = false;
            data.done = true;
            return;
        }

        auto arguments = ast_function.arguments;

        if (!arguments || arguments->children.empty())
        {
            data.should_be_erased = false;
            data.done = true;
            return;
        }

        /// If we meet function as argument then we have already checked
        /// arguments of it and if it can be erased
        for (const auto & identifier_or_function : arguments->children)
        {
            auto * identifier = identifier_or_function->as<ASTIdentifier>();
            if (identifier && !data.keys.count(getIdentifierName(identifier)))
            {
                data.should_be_erased = false;
                data.done = true;
                return;
            }

            if (!identifier && !identifier_or_function->as<ASTFunction>())
            {
                data.should_be_erased = false;
                data.done = true;
                return;
            }
        }

        const auto & function = FunctionFactory::instance().tryGet(ast_function.name, data.context);
        if (!function->isDeterministicInScopeOfQuery())
        {
            data.should_be_erased = false;
            data.done = true;
        }
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return true;
    }

};

using RedundantFunctionsInOrderByVisitor = InDepthNodeVisitor<RedundantFunctionsInOrderByMatcher, true>;

}
