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
#include <common/StringRef.h>

#include <optional>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int FUNCTION_NOT_ALLOWED;
}


template <typename Impl, typename Name, size_t LimitArgs>
class FunctionsMultiStringFuzzySearch : public IFunction
{
    static_assert(LimitArgs > 0);

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr context)
    {
        if (Impl::is_using_hyperscan && !context->getSettingsRef().allow_hyperscan)
            throw Exception(
                "Hyperscan functions are disabled, because setting 'allow_hyperscan' is set to 0", ErrorCodes::FUNCTION_NOT_ALLOWED);

        return std::make_shared<FunctionsMultiStringFuzzySearch>();
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isUnsignedInteger(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[2].get());
        if (!array_type || !checkAndGetDataType<DataTypeString>(array_type->getNestedType().get()))
            throw Exception(
                "Illegal type " + arguments[2]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return Impl::getReturnType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column_haystack = arguments[0].column;

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);

        const ColumnPtr & num_ptr = arguments[1].column;
        const ColumnConst * col_const_num = nullptr;
        UInt32 edit_distance = 0;

        if ((col_const_num = checkAndGetColumnConst<ColumnUInt8>(num_ptr.get())))
            edit_distance = col_const_num->getValue<UInt8>();
        else if ((col_const_num = checkAndGetColumnConst<ColumnUInt16>(num_ptr.get())))
            edit_distance = col_const_num->getValue<UInt16>();
        else if ((col_const_num = checkAndGetColumnConst<ColumnUInt32>(num_ptr.get())))
            edit_distance = col_const_num->getValue<UInt32>();
        else
            throw Exception(
                "Illegal column " + arguments[1].column->getName()
                    + ". The number is not const or does not fit in UInt32",
                ErrorCodes::ILLEGAL_COLUMN);


        const ColumnPtr & arr_ptr = arguments[2].column;
        const ColumnConst * col_const_arr = checkAndGetColumnConst<ColumnArray>(arr_ptr.get());

        if (!col_const_arr)
            throw Exception(
                "Illegal column " + arguments[2].column->getName() + ". The array is not const",
                ErrorCodes::ILLEGAL_COLUMN);

        Array src_arr = col_const_arr->getValue<Array>();

        if (src_arr.size() > LimitArgs)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + std::to_string(src_arr.size())
                    + ", should be at most " + std::to_string(LimitArgs),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        std::vector<StringRef> refs;
        refs.reserve(src_arr.size());

        for (const auto & el : src_arr)
            refs.emplace_back(el.get<String>());

        auto col_res = ColumnVector<ResultType>::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create();

        auto & vec_res = col_res->getData();
        auto & offsets_res = col_offsets->getData();

        /// The blame for resizing output is for the callee.
        if (col_haystack_vector)
            Impl::vectorConstant(
                col_haystack_vector->getChars(), col_haystack_vector->getOffsets(), refs, vec_res, offsets_res, edit_distance);
        else
            throw Exception("Illegal column " + arguments[0].column->getName(), ErrorCodes::ILLEGAL_COLUMN);

        if constexpr (Impl::is_column_array)
            return ColumnArray::create(std::move(col_res), std::move(col_offsets));
        else
            return col_res;
    }
};

}
