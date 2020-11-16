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
        bool redundant = true;
        bool done = false;

        void preventErase()
        {
            redundant = false;
            done = true;
        }
    };

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (const auto * func = ast->as<ASTFunction>())
            visit(*func, data);
    }

    static void visit(const ASTFunction & ast_function, Data & data)
    {
        if (data.done)
            return;

        bool is_lambda = (ast_function.name == "lambda");

        const auto & arguments = ast_function.arguments;
        bool has_arguments = arguments && !arguments->children.empty();

        if (is_lambda || !has_arguments)
        {
            data.preventErase();
            return;
        }

        /// If we meet function as argument then we have already checked
        /// arguments of it and if it can be erased
        for (const auto & arg : arguments->children)
        {
            /// Allow functions: visit them later
            if (arg->as<ASTFunction>())
                continue;

            /// Allow known identifiers: they are present in ORDER BY before current item
            if (auto * identifier = arg->as<ASTIdentifier>())
                if (data.keys.count(getIdentifierName(identifier)))
                    continue;

            /// Reject erase others
            data.preventErase();
            return;
        }

        const auto function = FunctionFactory::instance().tryGet(ast_function.name, data.context);
        if (!function || !function->isDeterministicInScopeOfQuery())
        {
            data.preventErase();
        }
    }

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        return node->as<ASTFunction>();
    }
};

using RedundantFunctionsInOrderByVisitor = ConstInDepthNodeVisitor<RedundantFunctionsInOrderByMatcher, true>;

}
