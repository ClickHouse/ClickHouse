#pragma once

#include <Functions/FunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>

namespace DB
{

/// Recursive traversal and check for optimizeAggregateFunctionsOfGroupByKeys
struct KeepAggregateFunctionMatcher
{
    struct Data
    {
        std::unordered_set<String> & group_by_keys;
        bool & keep_aggregator;
    };

    using Visitor = InDepthNodeVisitor<KeepAggregateFunctionMatcher, true>;

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        return !(node->as<ASTFunction>());
    }

    static void visit(ASTFunction & function_node, Data & data)
    {
        if ((function_node.arguments->children).empty())
        {
            data.keep_aggregator = true;
            return;
        }

        if (!data.group_by_keys.count(function_node.getColumnName()))
        {
            Visitor(data).visit(function_node.arguments);
        }
    }

    static void visit(ASTIdentifier & ident, Data & data)
    {
        if (!data.group_by_keys.count(ident.shortName()))
        {
            /// if variable of a function is not in GROUP BY keys, this function should not be deleted
            data.keep_aggregator = true;
            return;
        }
    }

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (data.keep_aggregator)
            return;

        if (auto * function_node = ast->as<ASTFunction>())
        {
            visit(*function_node, data);
        }
        else if (auto * ident = ast->as<ASTIdentifier>())
        {
            visit(*ident, data);
        }
        else if (!ast->as<ASTExpressionList>())
        {
            data.keep_aggregator = true;
        }
    }
};

using KeepAggregateFunctionVisitor = InDepthNodeVisitor<KeepAggregateFunctionMatcher, true>;

class SelectAggregateFunctionOfGroupByKeysMatcher
{
public:
    struct Data
    {
        std::unordered_set<String> & group_by_keys;
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        return !(node->as<ASTFunction>());
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        /// Check if function is min/max/any
        auto * function_node = ast->as<ASTFunction>();
        if (function_node && (function_node->name == "min" || function_node->name == "max" ||
                              function_node->name == "any" || function_node->name == "anyLast"))
        {
            bool keep_aggregator = false;
            KeepAggregateFunctionVisitor::Data keep_data{data.group_by_keys, keep_aggregator};
            KeepAggregateFunctionVisitor(keep_data).visit(function_node->arguments);

            /// Place argument of an aggregate function instead of function
            if (!keep_aggregator)
            {
                String alias = function_node->alias;
                ast = (function_node->arguments->children[0])->clone();
                ast->setAlias(alias);
            }
        }
    }
};

using SelectAggregateFunctionOfGroupByKeysVisitor = InDepthNodeVisitor<SelectAggregateFunctionOfGroupByKeysMatcher, true>;

}
