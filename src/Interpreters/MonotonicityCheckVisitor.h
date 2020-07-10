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
        std::unordered_map<String, ASTPtr> & group_by_function_hashes;
        ASTIdentifier * identifier = nullptr;
        Monotonicity monotonicity{true, true, true};
        DataTypePtr data_type = nullptr;
        String name{};
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

        if (!data.identifier)
        {
            data.identifier = ast_function.arguments->children[0]->as<ASTIdentifier>();

            if (!data.identifier)
            {
                data.monotonicity.is_monotonic = false;
                return;
            }
        }

        /// if GROUP BY contains the same function ORDER BY shouldn't be optimized
        auto hash = ast_function.getTreeHash();
        String key = toString(hash.first) + '_' + toString(hash.second);
        if (data.group_by_function_hashes.find(key) != data.group_by_function_hashes.end())
        {
            data.monotonicity.is_monotonic = false;
            return;
        }

        /// if ORDER BY contains aggregate function it shouldn't be optimized
        if (AggregateFunctionFactory::instance().isAggregateFunctionName(ast_function.name))
        {
            data.monotonicity.is_monotonic = false;
            return;
        }

        if (data.identifier && !data.data_type)
        {
            auto data_type_and_name = getIdentifierTypeAndName(data);

            if (!data_type_and_name)
            {
                data.monotonicity.is_monotonic = false;
                return;
            }

            if (!data.data_type)
            {
                data.data_type = data_type_and_name->type;
                data.name = data_type_and_name->name;
            }
        }

        ColumnsWithTypeAndName args;
        args.emplace_back(data.data_type, data.name);
        const auto & function = FunctionFactory::instance().tryGet(ast_function.name, data.context);
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

    static std::optional<NameAndTypePair> getIdentifierTypeAndName(Data & data)
    {
        auto pos = IdentifierSemantic::getMembership(*data.identifier);
        if (!pos)
        {
            pos = IdentifierSemantic::chooseTableColumnMatch(*data.identifier, data.tables, true);
            if (!pos)
            {
                data.monotonicity.is_monotonic = false;
                return {};
            }
        }

        return data.tables[*pos].columns.tryGetByName(data.identifier->shortName());
    }
};

using MonotonicityCheckVisitor = InDepthNodeVisitor<MonotonicityCheckMatcher, false>;

}
