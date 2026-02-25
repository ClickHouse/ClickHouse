#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_AGGREGATION;
}

class GetAggregatesMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<GetAggregatesMatcher, true>;

    struct Data
    {
        const char * assert_no_aggregates = nullptr;
        const char * assert_no_windows = nullptr;
        std::unordered_set<String> uniq_names {};
        ASTs aggregates{};
        ASTs window_functions{};
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child)
    {
        if (child->as<ASTSubquery>() || child->as<ASTSelectQuery>())
            return false;
        if (auto * select = node->as<ASTSelectQuery>())
        {
            // We don't analysis WITH statement because it might contain useless aggregates
            if (child == select->with())
                return false;
        }
        if (auto * func = node->as<ASTFunction>())
        {
            if (isAggregateFunction(*func))
            {
                return false;
            }

            // Window functions can contain aggregation results as arguments
            // to the window functions, or columns of PARTITION BY or ORDER BY
            // of the window.
        }
        return true;
    }

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (auto * func = ast->as<ASTFunction>())
            visit(*func, ast, data);
    }

private:
    static void visit(const ASTFunction & node, const ASTPtr & ast, Data & data)
    {
        if (isAggregateFunction(node))
        {
            if (data.assert_no_aggregates)
                throw Exception(ErrorCodes::ILLEGAL_AGGREGATION, "Aggregate function {} is found {} in query",
                                node.getColumnName(), String(data.assert_no_aggregates));

            String column_name = node.getColumnName();
            if (data.uniq_names.contains(column_name))
                return;

            data.uniq_names.insert(column_name);
            data.aggregates.push_back(ast);
        }
        else if (node.is_window_function)
        {
            if (data.assert_no_windows)
                throw Exception(ErrorCodes::ILLEGAL_AGGREGATION, "Window function {} is found {} in query",
                                node.getColumnName(), String(data.assert_no_windows));

            String column_name = node.getColumnName();
            if (data.uniq_names.contains(column_name))
                return;

            data.uniq_names.insert(column_name);
            data.window_functions.push_back(ast);
        }
    }

    static bool isAggregateFunction(const ASTFunction & node)
    {
        // Aggregate functions can also be calculated as window functions, but
        // here we are interested in aggregate functions calculated in GROUP BY.
        return !node.is_window_function && AggregateUtils::isAggregateFunction(node);
    }
};

using GetAggregatesVisitor = GetAggregatesMatcher::Visitor;


inline void assertNoWindows(const ASTPtr & ast, const char * description)
{
    GetAggregatesVisitor::Data data{.assert_no_windows = description};
    GetAggregatesVisitor(data).visit(ast);
}

inline void assertNoAggregates(const ASTPtr & ast, const char * description)
{
    GetAggregatesVisitor::Data data{.assert_no_aggregates = description};
    GetAggregatesVisitor(data).visit(ast);
}

ASTs getExpressionsWithWindowFunctions(ASTPtr & ast);

}
