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
        // Explicit empty initializers are needed to make designated initializers
        // work on GCC 10.
        std::unordered_set<String> uniq_names {};
        std::vector<const ASTFunction *> aggregates {};
        std::vector<const ASTFunction *> window_functions {};
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
    static void visit(const ASTFunction & node, const ASTPtr &, Data & data)
    {
        if (isAggregateFunction(node))
        {
            if (data.assert_no_aggregates)
                throw Exception("Aggregate function " + node.getColumnName()  + " is found " + String(data.assert_no_aggregates) + " in query",
                                ErrorCodes::ILLEGAL_AGGREGATION);

            String column_name = node.getColumnName();
            if (data.uniq_names.count(column_name))
                return;

            data.uniq_names.insert(column_name);
            data.aggregates.push_back(&node);
        }
        else if (node.is_window_function)
        {
            if (data.assert_no_windows)
                throw Exception("Window function " + node.getColumnName()  + " is found " + String(data.assert_no_windows) + " in query",
                                ErrorCodes::ILLEGAL_AGGREGATION);

            String column_name = node.getColumnName();
            if (data.uniq_names.count(column_name))
                return;

            data.uniq_names.insert(column_name);
            data.window_functions.push_back(&node);
        }
    }

    static bool isAggregateFunction(const ASTFunction & node)
    {
        // Aggregate functions can also be calculated as window functions, but
        // here we are interested in aggregate functions calculated in GROUP BY.
        return !node.is_window_function
            && AggregateFunctionFactory::instance().isAggregateFunctionName(
                node.name);
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

}
