#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

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
        std::unordered_set<String> uniq_names;
        std::vector<const ASTFunction *> aggregates;
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child)
    {
        if (child->as<ASTSubquery>() || child->as<ASTSelectQuery>())
            return false;
        if (auto * func = node->as<ASTFunction>())
            if (isAggregateFunction(func->name))
                return false;
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
        if (!isAggregateFunction(node.name))
            return;

        if (data.assert_no_aggregates)
            throw Exception("Aggregate function " + node.getColumnName()  + " is found " + String(data.assert_no_aggregates) + " in query",
                            ErrorCodes::ILLEGAL_AGGREGATION);

        String column_name = node.getColumnName();
        if (data.uniq_names.count(column_name))
            return;

        data.uniq_names.insert(column_name);
        data.aggregates.push_back(&node);
    }

    static bool isAggregateFunction(const String & name)
    {
        return AggregateFunctionFactory::instance().isAggregateFunctionName(name);
    }
};

using GetAggregatesVisitor = GetAggregatesMatcher::Visitor;


class GetEarlyWindowsMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<GetEarlyWindowsMatcher, true>;

    struct Data
    {
        const char * assert_no_early_windows = nullptr;
        std::unordered_set<String> uniq_names;
        std::unordered_set<String> column_names;
        std::vector<const ASTFunction *> early_windows;
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child)
    {
        if (child->as<ASTSubquery>() || child->as<ASTSelectQuery>())
            return false;
        if (auto * func = node->as<ASTFunction>())
            if (isEarlyWindowFunction(func->name))
                return false;
        return true;
    }

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (auto * func = ast->as<ASTFunction>())
            visit(*func, ast, data);
        else if (auto * identifier = ast->as<ASTIdentifier>())
            visit(*identifier, ast, data);
    }

private:
    static void visit(const ASTFunction & node, const ASTPtr &, Data & data)
    {
        if (!isEarlyWindowFunction(node.name))
            return;

        if (data.assert_no_early_windows)
            throw Exception("Early window function " + node.getColumnName()  + " is found " + String(data.assert_no_early_windows) + " in query",
                            ErrorCodes::ILLEGAL_AGGREGATION);

        String column_name = node.getColumnName();
        if (data.uniq_names.count(column_name))
            return;

        data.uniq_names.insert(column_name);
        data.early_windows.push_back(&node);
    }

    static bool isEarlyWindowFunction(const String & name)
    {
        return EarlyWindowFunctionFactory::instance().isAggregateFunctionName(name);
    }

    static void visit(const ASTIdentifier & node, const ASTPtr &, Data & data)
    {
        String column_name = node.getColumnName();
        if (column_name.empty())
            return;

        if (data.column_names.count(column_name))
            return;

        data.column_names.insert(column_name);
    }

};

using GetEarlyWindowsVisitor = GetEarlyWindowsMatcher::Visitor;


inline void assertNoAggregates(const ASTPtr & ast, const char * description)
{
    GetAggregatesVisitor::Data data{description, {}, {}};
    GetAggregatesVisitor(data).visit(ast);
}


inline void assertNoEarlyWindows(const ASTPtr & ast, const char * description)
{
    GetEarlyWindowsVisitor::Data data{description, {}, {}, {}};
    GetEarlyWindowsVisitor(data).visit(ast);
}

}
