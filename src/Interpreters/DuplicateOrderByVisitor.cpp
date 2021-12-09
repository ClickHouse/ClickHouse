#include <Interpreters/DuplicateOrderByVisitor.h>
#include <Functions/FunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTOrderByElement.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
}


namespace
{

/// Checks if SELECT has stateful functions
class ASTFunctionStatefulData
{
public:
    using TypeToVisit = ASTFunction;

    ContextPtr context;
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

}


void DuplicateOrderByFromSubqueriesData::visit(ASTSelectQuery & select_query, ASTPtr &)
{
    if (done)
        return;
    done = true;

    if (select_query.orderBy())
    {
        /// If we have limits then the ORDER BY is non-removable.
        if (select_query.limitBy()
            || select_query.limitByOffset()
            || select_query.limitByLength()
            || select_query.limitLength()
            || select_query.limitOffset())
        {
            return;
        }

        /// If ORDER BY contains filling (in addition to sorting) it is non-removable.
        for (const auto & child : select_query.orderBy()->children)
        {
            auto * ast = child->as<ASTOrderByElement>();
            if (!ast || ast->children.empty())
                throw Exception("Bad ORDER BY expression AST", ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE);

            if (ast->with_fill)
                return;
        }

        select_query.setExpression(ASTSelectQuery::Expression::ORDER_BY, nullptr);
    }
}

void DuplicateOrderByData::visit(ASTSelectQuery & select_query, ASTPtr &)
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
                if (is_stateful) //-V547
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

}

