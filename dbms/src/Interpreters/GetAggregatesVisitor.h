#pragma once

#include <Interpreters/InDepthNodeVisitor.h>

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
        bool assert_no_aggregates = false;
        const char * description = nullptr;
        std::unordered_set<String> uniq_names;
        std::vector<const ASTFunction *> aggregates;
    };

    static bool needChildVisit(const ASTPtr &, const ASTPtr & child)
    {
        if (child->as<ASTSubquery>() || child->as<ASTSelectQuery>())
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
        if (!AggregateFunctionFactory::instance().isAggregateFunctionName(node.name))
            return;

        if (data.assert_no_aggregates)
            throw Exception("Aggregate function " + node.getColumnName()  + " is found " + String(data.description) + " in query",
                            ErrorCodes::ILLEGAL_AGGREGATION);

        String column_name = node.getColumnName();
        if (data.uniq_names.count(column_name))
            return;

        data.aggregates.push_back(&node);
    }
};

using GetAggregatesVisitor = GetAggregatesMatcher::Visitor;

}
