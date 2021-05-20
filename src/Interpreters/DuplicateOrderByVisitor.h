#pragma once

#include <Functions/FunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
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

/// Checks if SELECT has stateful functions
class ASTFunctionStatefulData
{
public:
    using TypeToVisit = ASTFunction;

    const Context & context;
    bool & is_stateful;
    void visit(ASTFunction & ast_function, ASTPtr &)
    {
        auto aggregate_function_properties = AggregateFunctionFactory::instance().tryGetProperties(ast_function.name);

        if (aggregate_function_properties && aggregate_function_properties->is_order_dependent)
        {
            is_stateful = true;
            return;
        }

        const auto & function = FunctionFactory::instance().tryGet(ast_function.name, context);

        if (function && function->isStateful())
        {
            is_stateful = true;
            return;
        }
    }
};

using ASTFunctionStatefulMatcher = OneTypeMatcher<ASTFunctionStatefulData>;
using ASTFunctionStatefulVisitor = InDepthNodeVisitor<ASTFunctionStatefulMatcher, true>;


/// Erases unnecessary ORDER BY from subquery
class DuplicateOrderByFromSubqueriesData
{
public:
    using TypeToVisit = ASTSelectQuery;

    bool done = false;

    void visit(ASTSelectQuery & select_query, ASTPtr &)
    {
        if (done)
            return;

        if (select_query.orderBy() && !select_query.limitBy() && !select_query.limitByOffset() &&
            !select_query.limitByLength() && !select_query.limitLength() && !select_query.limitOffset())
        {
            select_query.setExpression(ASTSelectQuery::Expression::ORDER_BY, nullptr);
        }

        done = true;
    }
};

using DuplicateOrderByFromSubqueriesMatcher = OneTypeMatcher<DuplicateOrderByFromSubqueriesData>;
using DuplicateOrderByFromSubqueriesVisitor = InDepthNodeVisitor<DuplicateOrderByFromSubqueriesMatcher, true>;


/// Finds SELECT that can be optimized
class DuplicateOrderByData
{
public:
    using TypeToVisit = ASTSelectQuery;

    const Context & context;

    void visit(ASTSelectQuery & select_query, ASTPtr &)
    {
        if (select_query.orderBy() || select_query.groupBy())
        {
            for (auto & elem : select_query.children)
            {
                if (elem->as<ASTExpressionList>())
                {
                    bool is_stateful = false;
                    ASTFunctionStatefulVisitor::Data data{context, is_stateful};
                    ASTFunctionStatefulVisitor(data).visit(elem);
                    if (is_stateful)
                        return;
                }
            }

            if (auto select_table_ptr = select_query.tables())
            {
                if (auto * select_table = select_table_ptr->as<ASTTablesInSelectQuery>())
                {
                    if (!select_table->children.empty())
                    {
                        DuplicateOrderByFromSubqueriesVisitor::Data data{false};
                        DuplicateOrderByFromSubqueriesVisitor(data).visit(select_table->children[0]);
                    }
                }
            }
        }
    }
};

using DuplicateOrderByMatcher = OneTypeMatcher<DuplicateOrderByData>;
using DuplicateOrderByVisitor = InDepthNodeVisitor<DuplicateOrderByMatcher, true>;

}
