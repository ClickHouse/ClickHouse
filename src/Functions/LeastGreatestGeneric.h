#pragma once

#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/NumberTraits.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool least_greatest_legacy_null_behavior;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


enum class LeastGreatest : uint8_t
{
    Least,
    Greatest
};


template <LeastGreatest kind>
class FunctionLeastGreatestGeneric : public IFunction
{
public:
    static constexpr auto name = kind == LeastGreatest::Least ? "least" : "greatest";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionLeastGreatestGeneric<kind>>(context); }

    /// TODO Remove support for legacy NULL behavior (can be done end of 2026)

    explicit FunctionLeastGreatestGeneric(ContextPtr context)
        : legacy_null_behavior(context->getSettingsRef()[Setting::least_greatest_legacy_null_behavior])
    {
    }

private:
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return legacy_null_behavior; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & types) const override
    {
        if (types.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} cannot be called without arguments", getName());

        return getLeastSupertype(types);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (arguments.size() == 1)
            return arguments[0].column;

        Columns converted_columns;
        for (const auto & argument : arguments)
        {
            if (!legacy_null_behavior && argument.type->onlyNull())
                continue; /// ignore NULL arguments
            auto converted_col = castColumn(argument, result_type)->convertToFullColumnIfConst();
            converted_columns.push_back(converted_col);
        }

        if (!legacy_null_behavior && converted_columns.empty())
            return arguments[0].column;
        else if (!legacy_null_behavior && converted_columns.size() == 1)
            return converted_columns[0];

        auto result_column = result_type->createColumn();
        result_column->reserve(input_rows_count);

        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            size_t best_arg = 0;
            for (size_t arg = 1; arg < converted_columns.size(); ++arg)
            {
                if constexpr (kind == LeastGreatest::Least)
                {
                    auto cmp_result = converted_columns[arg]->compareAt(row_num, row_num, *converted_columns[best_arg], 1);
                    if (cmp_result < 0)
                        best_arg = arg;
                }
                else
                {
                    auto cmp_result = converted_columns[arg]->compareAt(row_num, row_num, *converted_columns[best_arg], -1);
                    if (cmp_result > 0)
                        best_arg = arg;
                }
            }

            result_column->insertFrom(*converted_columns[best_arg], row_num);
        }

        return result_column;
    }

    bool legacy_null_behavior;
};

template <LeastGreatest kind, typename SpecializedFunction>
class LeastGreatestOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = kind == LeastGreatest::Least ? "least" : "greatest";
    static FunctionOverloadResolverPtr create(ContextPtr context) { return std::make_unique<LeastGreatestOverloadResolver<kind, SpecializedFunction>>(context); }

    explicit LeastGreatestOverloadResolver(ContextPtr context_)
        : context(context_)
        , legacy_null_behavior(context->getSettingsRef()[Setting::least_greatest_legacy_null_behavior])
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return legacy_null_behavior; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        DataTypes argument_types;
        for (const auto & argument : arguments)
            argument_types.push_back(argument.type);

        /// More efficient specialization for two numeric arguments.
        if (arguments.size() == 2)
        {
            auto arg_0_type = legacy_null_behavior ? removeNullable(arguments[0].type) : arguments[0].type;
            auto arg_1_type = legacy_null_behavior ? removeNullable(arguments[1].type) : arguments[1].type;
            if (isNumber(arg_0_type) && isNumber(arg_1_type))
                return std::make_unique<FunctionToFunctionBaseAdaptor>(SpecializedFunction::create(context), argument_types, return_type);
        }

        return std::make_unique<FunctionToFunctionBaseAdaptor>(
            FunctionLeastGreatestGeneric<kind>::create(context), argument_types, return_type);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & types) const override
    {
        if (types.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} cannot be called without arguments", getName());

        if (types.size() == 2)
        {
            auto arg_0_type = legacy_null_behavior ? removeNullable(types[0]) : types[0];
            auto arg_1_type = legacy_null_behavior ? removeNullable(types[1]) : types[1];
            if (isNumber(arg_0_type) && isNumber(arg_1_type))
                return SpecializedFunction::create(context)->getReturnTypeImpl(types);
        }

        return getLeastSupertype(types);
    }

private:
    ContextPtr context;
    bool legacy_null_behavior;
};

}
