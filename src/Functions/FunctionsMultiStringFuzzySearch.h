#pragma once

#include <Core/Settings.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>

#include <optional>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

/**
  * multiFuzzyMatchAny(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiFuzzyMatchAnyIndex(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiFuzzyMatchAllIndices(haystack, [pattern_1, pattern_2, ..., pattern_n])
  *
  */

template <typename Impl>
class FunctionsMultiStringFuzzySearch : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr context)
    {
        const auto & settings = context->getSettingsRef();
        return std::make_shared<FunctionsMultiStringFuzzySearch>(settings.allow_hyperscan, settings.max_hyperscan_regexp_length, settings.max_hyperscan_regexp_total_length, settings.reject_expensive_hyperscan_regexps);
    }

    FunctionsMultiStringFuzzySearch(bool allow_hyperscan_, size_t max_hyperscan_regexp_length_, size_t max_hyperscan_regexp_total_length_, bool reject_expensive_hyperscan_regexps_)
        : allow_hyperscan(allow_hyperscan_)
        , max_hyperscan_regexp_length(max_hyperscan_regexp_length_)
        , max_hyperscan_regexp_total_length(max_hyperscan_regexp_total_length_)
        , reject_expensive_hyperscan_regexps(reject_expensive_hyperscan_regexps_)
    {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        if (!isUInt(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[1]->getName(), getName());

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[2].get());
        if (!array_type || !checkAndGetDataType<DataTypeString>(array_type->getNestedType().get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[2]->getName(), getName());

        return Impl::getReturnType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & haystack_ptr = arguments[0].column;
        const ColumnPtr & edit_distance_ptr = arguments[1].column;
        const ColumnPtr & needles_ptr = arguments[2].column;

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*haystack_ptr);
        const ColumnConst * col_haystack_const = checkAndGetColumnConst<ColumnString>(&*haystack_ptr);
        assert(static_cast<bool>(col_haystack_vector) ^ static_cast<bool>(col_haystack_const));

        UInt32 edit_distance = 0;
        if (const auto * col_const_uint8 = checkAndGetColumnConst<ColumnUInt8>(edit_distance_ptr.get()))
            edit_distance = col_const_uint8->getValue<UInt8>();
        else if (const auto * col_const_uint16 = checkAndGetColumnConst<ColumnUInt16>(edit_distance_ptr.get()))
            edit_distance = col_const_uint16->getValue<UInt16>();
        else if (const auto * col_const_uint32 = checkAndGetColumnConst<ColumnUInt32>(edit_distance_ptr.get()))
            edit_distance = col_const_uint32->getValue<UInt32>();
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal column {}. The number is not const or does not fit in UInt32",
                            arguments[1].column->getName());

        const ColumnArray * col_needles_vector = checkAndGetColumn<ColumnArray>(needles_ptr.get());
        const ColumnConst * col_needles_const = checkAndGetColumnConst<ColumnArray>(needles_ptr.get());
        assert(static_cast<bool>(col_needles_vector) ^ static_cast<bool>(col_needles_const));

        if (col_haystack_const && col_needles_vector)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support search with non-constant needles in constant haystack", name);

        using ResultType = typename Impl::ResultType;
        auto col_res = ColumnVector<ResultType>::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create();

        auto & vec_res = col_res->getData();
        auto & offsets_res = col_offsets->getData();
        /// the implementations are responsible for resizing the output column

        if (col_needles_const)
            Impl::vectorConstant(
                col_haystack_vector->getChars(), col_haystack_vector->getOffsets(),
                col_needles_const->getValue<Array>(),
                vec_res, offsets_res,
                edit_distance,
                allow_hyperscan, max_hyperscan_regexp_length, max_hyperscan_regexp_total_length, reject_expensive_hyperscan_regexps,
                input_rows_count);
        else
            Impl::vectorVector(
                col_haystack_vector->getChars(), col_haystack_vector->getOffsets(),
                col_needles_vector->getData(), col_needles_vector->getOffsets(),
                vec_res, offsets_res,
                edit_distance,
                allow_hyperscan, max_hyperscan_regexp_length, max_hyperscan_regexp_total_length, reject_expensive_hyperscan_regexps,
                input_rows_count);

        // the combination of const haystack + const needle is not implemented because
        // useDefaultImplementationForConstants() == true makes upper layers convert both to
        // non-const columns

        if constexpr (Impl::is_column_array)
            return ColumnArray::create(std::move(col_res), std::move(col_offsets));
        else
            return col_res;
    }

private:
    const bool allow_hyperscan;
    const size_t max_hyperscan_regexp_length;
    const size_t max_hyperscan_regexp_total_length;
    const bool reject_expensive_hyperscan_regexps;
};

}
