#pragma once

#include <Functions/FunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTSubquery.h>

namespace DB
{

/// Recursive traversal and check for optimizeAggregateFunctionsOfGroupByKeys
struct KeepAggregateFunctionMatcher
{
    struct Data
    {
        const NameSet & group_by_keys;
        bool keep_aggregator;
    };

    using Visitor = InDepthNodeVisitor<KeepAggregateFunctionMatcher, true>;

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        return !(node->as<ASTFunction>());
    }

    static void visit(ASTFunction & function_node, Data & data)
    {
        if (function_node.arguments->children.empty())
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
        /// if variable of a function is not in GROUP BY keys, this function should not be deleted
        if (!data.group_by_keys.count(ident.getColumnName()))
            data.keep_aggregator = true;
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

using KeepAggregateFunctionVisitor = KeepAggregateFunctionMatcher::Visitor;

class SelectAggregateFunctionOfGroupByKeysMatcher
{
public:
    struct Data
    {
        const NameSet & group_by_keys;
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        /// Don't descent into table functions and subqueries and special case for ArrayJoin.
        return !node->as<ASTSubquery>() && !node->as<ASTTableExpression>()
            && !node->as<ASTSelectWithUnionQuery>() && !node->as<ASTArrayJoin>();
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        /// Check if function is min/max/any
        auto * function_node = ast->as<ASTFunction>();
        if (function_node && (function_node->name == "min" || function_node->name == "max" ||
                              function_node->name == "any" || function_node->name == "anyLast"))
        {
            KeepAggregateFunctionVisitor::Data keep_data{data.group_by_keys, false};
            if (function_node->arguments) KeepAggregateFunctionVisitor(keep_data).visit(function_node->arguments);

            /// Place argument of an aggregate function instead of function
            if (!keep_data.keep_aggregator && function_node->arguments && !function_node->arguments->children.empty())
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
