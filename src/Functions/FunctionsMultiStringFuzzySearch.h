#pragma once

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


template <typename Impl>
class FunctionsMultiStringFuzzySearch : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr context)
    {
        const auto & settings = context->getSettingsRef();
        return std::make_shared<FunctionsMultiStringFuzzySearch>(settings.allow_hyperscan, settings.max_hyperscan_regexp_length, settings.max_hyperscan_regexp_total_length);
    }

    FunctionsMultiStringFuzzySearch(bool allow_hyperscan_, size_t max_hyperscan_regexp_length_, size_t max_hyperscan_regexp_total_length_)
        : allow_hyperscan(allow_hyperscan_)
        , max_hyperscan_regexp_length(max_hyperscan_regexp_length_)
        , max_hyperscan_regexp_total_length(max_hyperscan_regexp_total_length_)
    {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        if (!isUnsignedInteger(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[1]->getName(), getName());

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[2].get());
        if (!array_type || !checkAndGetDataType<DataTypeString>(array_type->getNestedType().get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[2]->getName(), getName());

        return Impl::getReturnType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr & column_haystack = arguments[0].column;
        const ColumnPtr & num_ptr = arguments[1].column;
        const ColumnPtr & arr_ptr = arguments[2].column;

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);
        assert(col_haystack_vector); // getReturnTypeImpl() checks the data type

        UInt32 edit_distance = 0;
        if (const auto * col_const_uint8 = checkAndGetColumnConst<ColumnUInt8>(num_ptr.get()))
            edit_distance = col_const_uint8->getValue<UInt8>();
        else if (const auto * col_const_uint16 = checkAndGetColumnConst<ColumnUInt16>(num_ptr.get()))
            edit_distance = col_const_uint16->getValue<UInt16>();
        else if (const auto * col_const_uint32 = checkAndGetColumnConst<ColumnUInt32>(num_ptr.get()))
            edit_distance = col_const_uint32->getValue<UInt32>();
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {}. The number is not const or does not fit in UInt32", arguments[1].column->getName());

        const ColumnConst * col_const_arr = checkAndGetColumnConst<ColumnArray>(arr_ptr.get());
        if (!col_const_arr)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {}. The array is not const", arguments[2].column->getName());

        using ResultType = typename Impl::ResultType;
        auto col_res = ColumnVector<ResultType>::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create();

        auto & vec_res = col_res->getData();
        auto & offsets_res = col_offsets->getData();
        // the implementations are responsible for resizing the output column

        Array needles_arr = col_const_arr->getValue<Array>();
        Impl::vectorConstant(
            col_haystack_vector->getChars(), col_haystack_vector->getOffsets(), needles_arr, vec_res, offsets_res, edit_distance,
            allow_hyperscan, max_hyperscan_regexp_length, max_hyperscan_regexp_total_length);

        if constexpr (Impl::is_column_array)
            return ColumnArray::create(std::move(col_res), std::move(col_offsets));
        else
            return col_res;
    }

private:
    const bool allow_hyperscan;
    const size_t max_hyperscan_regexp_length;
    const size_t max_hyperscan_regexp_total_length;
};

}
