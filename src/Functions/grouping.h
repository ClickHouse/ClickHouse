#pragma once

#include <base/defines.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

class FunctionGroupingBase : public IFunction
{
protected:
    static constexpr UInt64 ONE = 1;

    const ColumnNumbers arguments_indexes;
    // Initial implementation of GROUPING function returned 1 if the argument is used as an aggregation key.
    // This differs from the behavior described in the standard and other DBMS.
    const bool force_compatibility;

    static constexpr UInt64 COMPATIBLE_MODE[]   = {1, 0};
    static constexpr UInt64 INCOMPATIBLE_MODE[] = {0, 1};

public:
    FunctionGroupingBase(ColumnNumbers arguments_indexes_, bool force_compatibility_)
        : arguments_indexes(std::move(arguments_indexes_))
        , force_compatibility(force_compatibility_)
    {}

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool isSuitableForConstantFolding() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    /// Change it to never return LowCardinality, making it consistent when using groupingForRollup / groupingForforCube
    /// with __grouping_set
    bool canBeExecutedOnLowCardinalityDictionary() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    template <typename AggregationKeyChecker>
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, size_t input_rows_count, AggregationKeyChecker checker) const
    {
        const auto & grouping_set_column = checkAndGetColumn<ColumnUInt64>(*arguments[0].column);

        auto result = ColumnUInt64::create();
        auto & result_data = result->getData();
        result_data.reserve(input_rows_count);

        const auto * result_table = likely(force_compatibility) ? COMPATIBLE_MODE : INCOMPATIBLE_MODE;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            UInt64 set_index = grouping_set_column.getElement(i);

            UInt64 value = 0;
            for (auto index : arguments_indexes)
                value = (value << 1) + result_table[checker(set_index, index) ? 1 : 0];

            result_data.push_back(value);
        }
        return result;
    }
};

class FunctionGrouping : public FunctionGroupingBase
{
public:
    explicit FunctionGrouping(bool force_compatibility_)
        : FunctionGroupingBase(ColumnNumbers(), force_compatibility_)
    {}

    String getName() const override { return "grouping"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t) const override
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Method executeImpl is not supported for 'grouping' function");
    }
};

class FunctionGroupingOrdinary : public FunctionGroupingBase
{
public:
    FunctionGroupingOrdinary(ColumnNumbers arguments_indexes_, bool force_compatibility_)
        : FunctionGroupingBase(std::move(arguments_indexes_), force_compatibility_)
    {}

    String getName() const override { return "groupingOrdinary"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (likely(force_compatibility))
            return ColumnUInt64::create(input_rows_count, 0);
        UInt64 value = (ONE << arguments_indexes.size()) - 1;
        return ColumnUInt64::create(input_rows_count, value);
    }
};

class FunctionGroupingForRollup : public FunctionGroupingBase
{
    const UInt64 aggregation_keys_number;

public:
    FunctionGroupingForRollup(ColumnNumbers arguments_indexes_, UInt64 aggregation_keys_number_, bool force_compatibility_)
        : FunctionGroupingBase(std::move(arguments_indexes_), force_compatibility_)
        , aggregation_keys_number(aggregation_keys_number_)
    {}

    String getName() const override { return "groupingForRollup"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return FunctionGroupingBase::executeImpl(arguments, input_rows_count,
            [this](UInt64 set_index, UInt64 arg_index)
            {
                // For ROLLUP(a, b, c) there will be following grouping set indexes:
                // | GROUPING SET | INDEX |
                // | (a, b, c)    |   0   |
                // | (a, b)       |   1   |
                // | (a)          |   2   |
                // | ()           |   3   |
                return arg_index < aggregation_keys_number - set_index;
            }
        );
    }
};

class FunctionGroupingForCube : public FunctionGroupingBase
{
    const UInt64 aggregation_keys_number;

public:

    FunctionGroupingForCube(ColumnNumbers arguments_indexes_, UInt64 aggregation_keys_number_, bool force_compatibility_)
        : FunctionGroupingBase(arguments_indexes_, force_compatibility_)
        , aggregation_keys_number(aggregation_keys_number_)
    {}

    String getName() const override { return "groupingForCube"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return FunctionGroupingBase::executeImpl(arguments, input_rows_count,
            [this](UInt64 set_index, UInt64 arg_index)
            {
                // For CUBE(a, b) there will be following grouping set indexes:
                // | GROUPING SET | INDEX |
                // | (a, b)       |   0   |
                // | (a)          |   1   |
                // | (b)          |   2   |
                // | ()           |   3   |
                auto set_mask = (ONE << aggregation_keys_number) - 1 - set_index;
                return set_mask & (ONE << (aggregation_keys_number - arg_index - 1));
            }
        );
    }
};

class FunctionGroupingForGroupingSets : public FunctionGroupingBase
{
    ColumnNumbersSetList grouping_sets;
public:
    FunctionGroupingForGroupingSets(ColumnNumbers arguments_indexes_, ColumnNumbersList const & grouping_sets_, bool force_compatibility_)
        : FunctionGroupingBase(std::move(arguments_indexes_), force_compatibility_)
    {
        for (auto const & set : grouping_sets_)
            grouping_sets.emplace_back(set.begin(), set.end());
    }

    String getName() const override { return "groupingForGroupingSets"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return FunctionGroupingBase::executeImpl(arguments, input_rows_count,
            [this](UInt64 set_index, UInt64 arg_index)
            {
                return grouping_sets[set_index].contains(arg_index);
            }
        );
    }
};

}
