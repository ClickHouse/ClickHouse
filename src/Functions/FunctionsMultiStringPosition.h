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
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>


namespace DB
{
/** multiSearchAllPositions(haystack, [pattern_1, pattern_2, ..., pattern_n]) -- find first occurrences (positions) of all the const patterns inside haystack
  * multiSearchAllPositionsUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchAllPositionsCaseInsensitive(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchAllPositionsCaseInsensitiveUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
  *
  * multiSearchFirstPosition(haystack, [pattern_1, pattern_2, ..., pattern_n]) -- returns the first position of the haystack matched by strings or zero if nothing was found
  * multiSearchFirstPositionUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchFirstPositionCaseInsensitive(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchFirstPositionCaseInsensitiveUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
  *
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
}


template <typename Impl, typename Name>
class FunctionsMultiStringPosition : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionsMultiStringPosition>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());
        if (!array_type || !checkAndGetDataType<DataTypeString>(array_type->getNestedType().get()))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column_haystack = arguments[0].column;

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);

        const ColumnPtr & arr_ptr = arguments[1].column;
        const ColumnConst * col_const_arr = checkAndGetColumnConst<ColumnArray>(arr_ptr.get());

        if (!col_const_arr)
            throw Exception(
                "Illegal column " + arguments[1].column->getName() + ". The array is not const",
                ErrorCodes::ILLEGAL_COLUMN);

        Array src_arr = col_const_arr->getValue<Array>();

        if (src_arr.size() > std::numeric_limits<UInt8>::max())
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + std::to_string(src_arr.size())
                    + ", should be at most 255",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        std::vector<std::string_view> refs;
        refs.reserve(src_arr.size());
        for (const auto & el : src_arr)
            refs.emplace_back(el.get<String>());

        const size_t column_haystack_size = column_haystack->size();

        auto col_res = ColumnVector<ResultType>::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create(column_haystack_size);

        auto & vec_res = col_res->getData();
        auto & offsets_res = col_offsets->getData();

        vec_res.resize(column_haystack_size * refs.size());

        if (col_haystack_vector)
            Impl::vectorConstant(col_haystack_vector->getChars(), col_haystack_vector->getOffsets(), refs, vec_res);
        else
            throw Exception("Illegal column " + arguments[0].column->getName(), ErrorCodes::ILLEGAL_COLUMN);

        size_t refs_size = refs.size();
        size_t accum = refs_size;

        for (size_t i = 0; i < column_haystack_size; ++i, accum += refs_size)
            offsets_res[i] = accum;

        return ColumnArray::create(std::move(col_res), std::move(col_offsets));
    }
};

}
