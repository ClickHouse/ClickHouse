#pragma once

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>

namespace DB
{

using Monotonicity = IFunctionBase::Monotonicity;

class MonotonicityCheckMatcher
{
public:
    struct Data
    {
        const TablesWithColumns & tables;
        const Context & context;
        ASTPtr & identifier;
        Monotonicity & monotonicity;
        DataTypePtr data_type = nullptr;
        bool first = true;
    };

    static void visit(const ASTPtr & ast, Data & data)
    {
        auto * ast_function = ast->as<ASTFunction>();
        if (ast_function)
        {
            visit(*ast_function, data);
        }
    }


    static void visit(ASTFunction & ast_function, Data & data)
    {
        if (!data.monotonicity.is_always_monotonic)
            return;

        auto arguments = ast_function.arguments;
        if (arguments->children.size() != 1)
        {
            data.monotonicity.is_always_monotonic = false;
            return;
        }

        if (data.first)
        {
            data.identifier = ast_function.arguments->children[0];
            if (!data.identifier->as<ASTIdentifier>())
            {
                data.monotonicity.is_always_monotonic = false;
                return;
            }
        }

        auto * identifier_ptr = data.identifier->as<ASTIdentifier>();

        if (AggregateFunctionFactory::instance().isAggregateFunctionName(ast_function.name))
        {
            data.monotonicity.is_always_monotonic = false;
            return;
        }

        const auto & function = FunctionFactory::instance().tryGet(ast_function.name, data.context);
        auto pos = IdentifierSemantic::getMembership(*identifier_ptr->as<ASTIdentifier>());
        if (pos == NULL)
        {
            data.monotonicity.is_always_monotonic = false;
            return;
        }

        auto data_type_and_name = data.tables[*pos].columns.tryGetByName(identifier_ptr->shortName());

        if (!data_type_and_name)
        {
            data.monotonicity.is_always_monotonic = false;
            return;
        }

        if (data.first)
        {
            data.data_type = data_type_and_name->type;
            data.first = false;
        }
        auto name = data_type_and_name->name;

        ColumnsWithTypeAndName args;
        args.emplace_back(data.data_type, name);
        auto function_base = function->build(args);

        auto cur_data = data.data_type;
        data.data_type = function_base->getReturnType();

        if (!function_base->hasInformationAboutMonotonicity())
        {
            data.monotonicity.is_always_monotonic = false;
            return;
        }

        bool is_positive = data.monotonicity.is_positive;
        data.monotonicity = function_base->getMonotonicityForRange(*cur_data, Field(), Field());

        if (!is_positive)
            data.monotonicity.is_positive = !data.monotonicity.is_positive;
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return true;
    }

};

using MonotonicityCheckVisitor = InDepthNodeVisitor<MonotonicityCheckMatcher, false>;

/// Finds SELECT that can be optimized
class MonotonousOrderByData
{
public:
    using TypeToVisit = ASTSelectQuery;

    const TablesWithColumns & tables;
    const Context & context;
    bool done = false;

    void visit(ASTSelectQuery & select_query, ASTPtr &)
    {
        if (done)
            return;

        auto order_by = select_query.orderBy();

        if (!order_by)
            return;

        for (size_t i = 0; i < order_by->children.size(); ++i)
        {
            auto child = order_by->children[i];
            ASTPtr identifier;
            Monotonicity monotonicity;
            monotonicity.is_always_monotonic = true;
            monotonicity.is_positive = true;
            monotonicity.is_monotonic = true;

            MonotonicityCheckVisitor::Data monotonicity_checker_data{tables, context, identifier, monotonicity};
            MonotonicityCheckVisitor(monotonicity_checker_data).visit(child);
            if (monotonicity.is_always_monotonic)
            {
                auto * order_by_element = child->as<ASTOrderByElement>();
                order_by_element->children[0] = identifier;
                if (!monotonicity.is_positive)
                    order_by_element->direction *= -1;
            }
        }

        done = true;
    }
};

using MonotonousOrderByMatcher = OneTypeMatcher<MonotonousOrderByData>;
using MonotonousOrderByVisitor = InDepthNodeVisitor<MonotonousOrderByMatcher, true>;

}
