#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <common/StringRef.h>


namespace DB
{
/**
  * multiSearchAny(haystack, [pattern_1, pattern_2, ..., pattern_n]) -- find any of the const patterns inside haystack and return 0 or 1
  * multiSearchAnyUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchAnyCaseInsensitive(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchAnyCaseInsensitiveUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])

  * multiSearchFirstIndex(haystack, [pattern_1, pattern_2, ..., pattern_n]) -- returns the first index of the matched string or zero if nothing was found
  * multiSearchFirstIndexUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchFirstIndexCaseInsensitive(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchFirstIndexCaseInsensitiveUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
  */

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int FUNCTION_NOT_ALLOWED;
}


/// The argument limiting raises from Volnitsky searcher -- it is performance crucial to save only one byte for pattern number.
/// But some other searchers use this function, for example, multiMatchAny -- hyperscan does not have such restrictions
template <typename Impl, typename Name, size_t LimitArgs = std::numeric_limits<UInt8>::max()>
class FunctionsMultiStringSearch : public IFunction
{
    static_assert(LimitArgs > 0);

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context)
    {
        if (Impl::is_using_hyperscan && !context.getSettingsRef().allow_hyperscan)
            throw Exception(
                "Hyperscan functions are disabled, because setting 'allow_hyperscan' is set to 0", ErrorCodes::FUNCTION_NOT_ALLOWED);

        return std::make_shared<FunctionsMultiStringSearch>();
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());
        if (!array_type || !checkAndGetDataType<DataTypeString>(array_type->getNestedType().get()))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return Impl::getReturnType();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column_haystack = block.getByPosition(arguments[0]).column;

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);

        const ColumnPtr & arr_ptr = block.getByPosition(arguments[1]).column;
        const ColumnConst * col_const_arr = checkAndGetColumnConst<ColumnArray>(arr_ptr.get());

        if (!col_const_arr)
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[1]).column->getName() + ". The array is not const",
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
            Impl::vectorConstant(col_haystack_vector->getChars(), col_haystack_vector->getOffsets(), refs, vec_res, offsets_res);
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName(), ErrorCodes::ILLEGAL_COLUMN);

        if constexpr (Impl::is_column_array)
            block.getByPosition(result).column = ColumnArray::create(std::move(col_res), std::move(col_offsets));
        else
            block.getByPosition(result).column = std::move(col_res);
    }
};

}
