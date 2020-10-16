#pragma once

#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/NumberTraits.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <ext/map.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


enum class LeastGreatest
{
    Least,
    Greatest
};


template <LeastGreatest kind>
class FunctionLeastGreatestGeneric : public IFunction
{
public:
    static constexpr auto name = kind == LeastGreatest::Least ? "least" : "greatest";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionLeastGreatestGeneric<kind>>(); }

private:
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & types) const override
    {
        if (types.empty())
            throw Exception("Function " + getName() + " cannot be called without arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return getLeastSupertype(types);
    }

    void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        size_t num_arguments = arguments.size();
        if (1 == num_arguments)
        {
            columns[result].column = columns[arguments[0]].column;
            return;
        }

        auto result_type = columns[result].type;

        Columns converted_columns(num_arguments);
        for (size_t arg = 0; arg < num_arguments; ++arg)
            converted_columns[arg] = castColumn(columns[arguments[arg]], result_type)->convertToFullColumnIfConst();

        auto result_column = result_type->createColumn();
        result_column->reserve(input_rows_count);

        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            size_t best_arg = 0;
            for (size_t arg = 1; arg < num_arguments; ++arg)
            {
                auto cmp_result = converted_columns[arg]->compareAt(row_num, row_num, *converted_columns[best_arg], 1);

                if constexpr (kind == LeastGreatest::Least)
                {
                    if (cmp_result < 0)
                        best_arg = arg;
                }
                else
                {
                    if (cmp_result > 0)
                        best_arg = arg;
                }
            }

            result_column->insertFrom(*converted_columns[best_arg], row_num);
        }

        columns[result].column = std::move(result_column);
    }
};


template <LeastGreatest kind, typename SpecializedFunction>
class LeastGreatestOverloadResolver : public IFunctionOverloadResolverImpl
{
public:
    static constexpr auto name = kind == LeastGreatest::Least ? "least" : "greatest";

    static FunctionOverloadResolverImplPtr create(const Context & context)
    {
        return std::make_unique<LeastGreatestOverloadResolver<kind, SpecializedFunction>>(context);
    }

    explicit LeastGreatestOverloadResolver(const Context & context_) : context(context_) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        DataTypes argument_types;

        /// More efficient specialization for two numeric arguments.
        if (arguments.size() == 2 && isNumber(arguments[0].type) && isNumber(arguments[1].type))
            return std::make_unique<DefaultFunction>(SpecializedFunction::create(context), argument_types, return_type);

        return std::make_unique<DefaultFunction>(
            FunctionLeastGreatestGeneric<kind>::create(context), argument_types, return_type);
    }

    DataTypePtr getReturnType(const DataTypes & types) const override
    {
        if (types.empty())
            throw Exception("Function " + getName() + " cannot be called without arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (types.size() == 2 && isNumber(types[0]) && isNumber(types[1]))
            return SpecializedFunction::create(context)->getReturnTypeImpl(types);

        return getLeastSupertype(types);
    }

private:
    const Context & context;
};

}


