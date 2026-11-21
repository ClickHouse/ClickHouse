#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_hyperscan;
    extern const SettingsUInt64 max_hyperscan_regexp_length;
    extern const SettingsUInt64 max_hyperscan_regexp_total_length;
    extern const SettingsBool reject_expensive_hyperscan_regexps;
}

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
  *
  * multiSearchFirstPosition(haystack, [pattern_1, pattern_2, ..., pattern_n]) -- returns the leftmost offset of the matched string or zero if nothing was found
  * multiSearchFirstPositionUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchFirstPositionCaseInsensitive(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchFirstPositionCaseInsensitiveUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
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
        return std::make_shared<FunctionsMultiStringSearch>(settings[Setting::allow_hyperscan], settings[Setting::max_hyperscan_regexp_length], settings[Setting::max_hyperscan_regexp_total_length], settings[Setting::reject_expensive_hyperscan_regexps]);
    }

    FunctionsMultiStringSearch(bool allow_hyperscan_, size_t max_hyperscan_regexp_length_, size_t max_hyperscan_regexp_total_length_, bool reject_expensive_hyperscan_regexps_)
        : allow_hyperscan(allow_hyperscan_)
        , max_hyperscan_regexp_length(max_hyperscan_regexp_length_)
        , max_hyperscan_regexp_total_length(max_hyperscan_regexp_total_length_)
        , reject_expensive_hyperscan_regexps(reject_expensive_hyperscan_regexps_)
    {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());
        if (!array_type || !checkAndGetDataType<DataTypeString>(array_type->getNestedType().get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[1]->getName(), getName());

        return Impl::getReturnType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & haystack_ptr = arguments[0].column;
        const ColumnPtr & needles_ptr = arguments[1].column;

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*haystack_ptr);
        const ColumnConst * col_haystack_const = checkAndGetColumnConst<ColumnString>(&*haystack_ptr);
        assert(static_cast<bool>(col_haystack_vector) ^ static_cast<bool>(col_haystack_const));

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
                allow_hyperscan, max_hyperscan_regexp_length, max_hyperscan_regexp_total_length, reject_expensive_hyperscan_regexps,
                input_rows_count);
        else
            Impl::vectorVector(
                col_haystack_vector->getChars(), col_haystack_vector->getOffsets(),
                col_needles_vector->getData(), col_needles_vector->getOffsets(),
                vec_res, offsets_res,
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
