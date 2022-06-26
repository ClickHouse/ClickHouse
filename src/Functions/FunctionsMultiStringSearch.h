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
/**
  * multiMatchAny(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiMatchAnyIndex(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiMatchAllIndices(haystack, [pattern_1, pattern_2, ..., pattern_n])
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
}


template <typename Impl>
class FunctionsMultiStringSearch : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr context)
    {
        const auto & settings = context->getSettingsRef();
        return std::make_shared<FunctionsMultiStringSearch>(settings.allow_hyperscan, settings.max_hyperscan_regexp_length, settings.max_hyperscan_regexp_total_length);
    }

    FunctionsMultiStringSearch(bool allow_hyperscan_, size_t max_hyperscan_regexp_length_, size_t max_hyperscan_regexp_total_length_)
        : allow_hyperscan(allow_hyperscan_)
        , max_hyperscan_regexp_length(max_hyperscan_regexp_length_)
        , max_hyperscan_regexp_total_length(max_hyperscan_regexp_total_length_)
    {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());
        if (!array_type || !checkAndGetDataType<DataTypeString>(array_type->getNestedType().get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[1]->getName(), getName());

        return Impl::getReturnType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr & column_haystack = arguments[0].column;
        const ColumnPtr & arr_ptr = arguments[1].column;

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);
        const ColumnConst * col_haystack_const = typeid_cast<const ColumnConst *>(&*column_haystack);
        assert(static_cast<bool>(col_haystack_vector) ^ static_cast<bool>(col_haystack_const));

        const ColumnArray * col_needles = checkAndGetColumn<ColumnArray>(arr_ptr.get());
        const ColumnConst * col_needles_const = checkAndGetColumnConst<ColumnArray>(arr_ptr.get());
        assert(static_cast<bool>(col_needles) ^ static_cast<bool>(col_needles_const));

        if (col_haystack_const && col_needles)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support search with non-constant needles in constant haystack", name);

        using ResultType = typename Impl::ResultType;
        auto col_res = ColumnVector<ResultType>::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create();

        auto & vec_res = col_res->getData();
        auto & offsets_res = col_offsets->getData();
        // the implementations are responsible for resizing the output column

        if (col_needles_const)
        {
            Array needles_arr = col_needles_const->getValue<Array>();
            Impl::vectorConstant(
                col_haystack_vector->getChars(), col_haystack_vector->getOffsets(), needles_arr, vec_res, offsets_res,
                allow_hyperscan, max_hyperscan_regexp_length, max_hyperscan_regexp_total_length);
        }
        else
        {
            Impl::vectorVector(
                col_haystack_vector->getChars(), col_haystack_vector->getOffsets(), *col_needles, vec_res, offsets_res,
                allow_hyperscan, max_hyperscan_regexp_length, max_hyperscan_regexp_total_length);
        }


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
