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


inline void assertNoAggregates(const ASTPtr & ast, const char * description)
{
    GetAggregatesVisitor::Data data{description, {}, {}};
    GetAggregatesVisitor(data).visit(ast);
}

}
