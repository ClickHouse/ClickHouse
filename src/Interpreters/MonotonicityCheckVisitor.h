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

/// Checks from bottom to top if function composition is monotonous
class MonotonicityCheckMatcher
{
public:
    struct Data
    {
        const TablesWithColumns & tables;
        const Context & context;
        ASTIdentifier * identifier = nullptr;
        Monotonicity monotonicity{true, true, true};
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
        if (!data.monotonicity.is_monotonic)
            return;

        auto arguments = ast_function.arguments;
        if (arguments->children.size() != 1)
        {
            data.monotonicity.is_monotonic = false;
            return;
        }

        if (data.first)
        {
            data.identifier = ast_function.arguments->children[0]->as<ASTIdentifier>();
        }

        if (!data.identifier)
        {
            data.monotonicity.is_monotonic = false;
            return;
        }

        if (AggregateFunctionFactory::instance().isAggregateFunctionName(ast_function.name))
        {
            data.monotonicity.is_monotonic = false;
            return;
        }

        const auto & function = FunctionFactory::instance().tryGet(ast_function.name, data.context);
        auto pos = IdentifierSemantic::getMembership(*data.identifier);
        if (!pos)
        {
            pos = IdentifierSemantic::chooseTableColumnMatch(*data.identifier, data.tables, true);
            if (!pos)
            {
                data.monotonicity.is_monotonic = false;
                return;
            }
        }

        auto data_type_and_name = data.tables[*pos].columns.tryGetByName(data.identifier->shortName());

        if (!data_type_and_name)
        {
            data.monotonicity.is_monotonic = false;
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
            data.monotonicity.is_monotonic = false;
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

}
