#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <base/memcmpSmall.h>

#include <algorithm>
#include <limits>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

// usage:
// stringCompare(s, t)
//  Compare s with t.
// stringCompare(s, t, offset_s, offset_t, n)
//  Compare s from offset_s to offset_s + n with t from offset_t to offset_t + n.
class FunctionStringCompare : public IFunction
{
public:
    static constexpr auto name = "stringCompare";
    static FunctionPtr create(const ContextPtr /*context*/) { return std::make_shared<FunctionStringCompare>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 5)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires 2 or 5 arguments", getName());

        FunctionArgumentDescriptors mandatory_args{
            {"string1", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String"},
            {"string2", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String"},
        };
        FunctionArgumentDescriptors optional_args{
            {"string1_offset", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt), static_cast<FunctionArgumentDescriptor::ColumnValidator>(&FunctionStringCompare::isConstColumn), "const UInt*"},
            {"string2_offset", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt), static_cast<FunctionArgumentDescriptor::ColumnValidator>(&FunctionStringCompare::isConstColumn), "const UInt*"},
            {"num_bytes", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt), static_cast<FunctionArgumentDescriptor::ColumnValidator>(&FunctionStringCompare::isConstColumn), "const UInt*"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeInt8>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        chassert(arguments[0].column->size() == input_rows_count);
        auto col_result = ColumnInt8::create();
        if (!input_rows_count)
            return std::move(col_result);

        auto prepare_string_args
            = [](const ColumnPtr & column) -> std::tuple<const ColumnString *, const ColumnFixedString *, const ColumnConst *>
        {
            return {
                checkAndGetColumn<ColumnString>(column.get()),
                checkAndGetColumn<ColumnFixedString>(column.get()),
                checkAndGetColumn<ColumnConst>(column.get())};
        };
        auto [col_str1, fixed_str1_col, col_str1_const] = prepare_string_args(arguments[0].column);
        auto [col_str2, fixed_str2_col, col_str2_const] = prepare_string_args(arguments[1].column);

        auto str1_offset = arguments.size() > 2 ? arguments[2].column->getUInt(0) : 0;
        auto str2_offset = arguments.size() > 2 ? arguments[3].column->getUInt(0) : 0;

        auto num_bytes = arguments.size() > 2 ? arguments[4].column->getUInt(0) : std::numeric_limits<size_t>::max();
        if (num_bytes == 0)
        {
            col_result->insertMany(0, arguments[0].column->size());
            return std::move(col_result);
        }

        col_result->getData().resize(arguments[0].column->size());
        auto & result_array = col_result->getData();

        if (col_str1 && col_str2_const)
            executeStringConst<false>(*col_str1, str1_offset, String(col_str2_const->getDataAt(0)), str2_offset, num_bytes, result_array);
        else if (col_str1_const && col_str2)
            executeStringConst<true>(*col_str2, str2_offset, String(col_str1_const->getDataAt(0)), str1_offset, num_bytes, result_array);
        else if (col_str1 && col_str2)
            executeStringString(*col_str1, str1_offset, *col_str2, str2_offset, num_bytes, result_array);
        else if (fixed_str1_col && fixed_str2_col)
            executeFixedStringFixedString(*fixed_str1_col, str1_offset, *fixed_str2_col, str2_offset, num_bytes, result_array);
        else if (fixed_str1_col && col_str2)
            executeFixedStringString<false>(*fixed_str1_col, str1_offset, *col_str2, str2_offset, num_bytes, result_array);
        else if (col_str1 && fixed_str2_col)
            executeFixedStringString<true>(*fixed_str2_col, str2_offset, *col_str1, str1_offset, num_bytes, result_array);
        else if (fixed_str1_col && col_str2_const)
            executeFixedStringConst<false>(*fixed_str1_col, str1_offset, String(col_str2_const->getDataAt(0)), str2_offset, num_bytes, result_array);
        else if (fixed_str2_col && col_str1_const)
            executeFixedStringConst<true>(*fixed_str2_col, str2_offset, String(col_str1_const->getDataAt(0)), str1_offset, num_bytes, result_array);
        else if (col_str1_const && col_str2_const)
            executeConstConst(
                String(col_str1_const->getDataAt(0)),
                str1_offset,
                String(col_str2_const->getDataAt(0)),
                str2_offset,
                num_bytes,
                arguments[0].column->size(),
                result_array);
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal columns {} and {} of arguments of function {}",
                arguments[0].column->getName(),
                arguments[1].column->getName(),
                getName());
        return std::move(col_result);
    }

private:
    static bool isConstColumn(const IColumn & column) { return column.isConst(); }

    template <bool reverse>
    static Int8 normalComparison(const char * str1, size_t str1_length, const char * str2, size_t str2_length)
    {
        if constexpr (reverse)
            return static_cast<Int8>(memcmpSmallLikeZeroPaddedAllowOverflow15(str2, str2_length, str1, str1_length));
        else
            return static_cast<Int8>(memcmpSmallLikeZeroPaddedAllowOverflow15(str1, str1_length, str2, str2_length));
    }

    template <bool reverse>
    static bool isOverflowComparison(size_t str1_length, size_t str1_offset, size_t str2_length, size_t str2_offset, Int8 & result)
    {
        if (str1_offset >= str1_length && str2_offset >= str2_length)
        {
            result = 0;
            return true;
        }
        else if (str1_offset >= str1_length)
        {
            if constexpr (reverse)
                result = 1;
            else
                result = -1;
            return true;
        }
        else if (str2_offset >= str2_length)
        {
            if constexpr (reverse)
                result = -1;
            else
                result = 1;
            return true;
        }
        return false;
    }

    template <bool reverse>
    static bool isOverflowComparison(
        size_t str1_length, size_t str1_offset, size_t str2_length, size_t str2_offset, PaddedPODArray<Int8> & result_array, size_t rows)
    {
        bool is_overflow = false;
        Int8 res = 0;
        if (str1_offset >= str1_length && str2_offset >= str2_length)
        {
            res = 0;
            is_overflow = true;
        }
        else if (str1_offset >= str1_length)
        {
            if constexpr (reverse)
                res = 1;
            else
                res = -1;
            is_overflow = true;
        }
        else if (str2_offset >= str2_length)
        {
            if constexpr (reverse)
                res = -1;
            else
                res = 1;
            is_overflow = true;
        }
        for (size_t i = 0; i < rows; ++i)
            result_array[i] = res;
        return is_overflow;
    }

    template <bool reverse>
    void executeStringConst(
        const ColumnString & col_str1,
        size_t str1_offset,
        const String & str2,
        size_t str2_offset,
        size_t num_bytes,
        PaddedPODArray<Int8> & result_array) const
    {
        const auto & str1_data = col_str1.getChars();
        const auto & str1_offsets = col_str1.getOffsets();
        size_t str1_data_offset = 0;
        auto str2_length = str2_offset >= str2.size() ? 0 : std::min(num_bytes, str2.size() - str2_offset);
        for (size_t i = 0, size = col_str1.size(); i < size; ++i)
        {
            size_t str1_total_len = str1_offsets[i] - str1_data_offset - 1;
            if (!isOverflowComparison<reverse>(str1_total_len, str1_offset, str2.size(), str2_offset, result_array[i]))
            {
                const auto * str1 = str1_data.data() + str1_data_offset + str1_offset;
                result_array[i] = normalComparison<reverse>(
                    reinterpret_cast<const char *>(str1),
                    std::min(num_bytes, str1_total_len - str1_offset),
                    str2.data() + str2_offset,
                    str2_length);
            }

            str1_data_offset = str1_offsets[i];
        }
    }

    template <bool reverse>
    void executeFixedStringConst(
        const ColumnFixedString & col_str1,
        size_t str1_offset,
        const String & str2,
        size_t str2_offset,
        size_t num_bytes,
        PaddedPODArray<Int8> & result_array) const
    {
        size_t rows = col_str1.size();
        size_t str1_total_len = col_str1.getN();
        if (isOverflowComparison<reverse>(str1_total_len, str1_offset, str2.size(), str2_offset, result_array, rows))
            return;

        const auto & str1_data = col_str1.getChars();
        size_t str1_data_offset = str1_offset;
        auto str1_length = std::min(num_bytes, str1_total_len - str1_offset);
        auto str2_length = std::min(num_bytes, str2.size() - str2_offset);
        for (size_t i = 0; i < rows; ++i)
        {
            const auto * str1 = reinterpret_cast<const char *>(str1_data.data() + str1_data_offset);
            result_array[i] = normalComparison<reverse>(str1, str1_length, str2.data() + str2_offset, str2_length);
            str1_data_offset += str1_total_len;
        }
    }

    template <bool reverse>
    void executeFixedStringString(
        const ColumnFixedString & col_str1,
        size_t str1_offset,
        const ColumnString & col_str2,
        size_t str2_offset,
        size_t num_bytes,
        PaddedPODArray<Int8> & result_array) const
    {
        auto str1_total_len = col_str1.getN();
        const auto & str1_data = col_str1.getChars();
        const auto & str2_data = col_str2.getChars();
        const auto & str2_offsets = col_str2.getOffsets();
        size_t str1_data_offset = str1_offset;
        size_t str2_data_offset = 0;
        for (size_t i = 0, rows = col_str1.size(); i < rows; ++i)
        {
            const auto * str1 = reinterpret_cast<const char *>(str1_data.data() + str1_data_offset);
            auto str1_length = std::min(num_bytes, str1_total_len - str1_offset);
            const auto * str2 = reinterpret_cast<const char *>(str2_data.data() + str2_data_offset + str2_offset);
            size_t str2_total_len = str2_offsets[i] - str2_data_offset - 1;
            if (!isOverflowComparison<reverse>(str1_total_len, str1_offset, str2_total_len, str2_offset, result_array[i]))
            {
                auto str2_length = str2_offset >= str2_total_len ? 0 : std::min(num_bytes, str2_total_len - str2_offset);
                result_array[i] = normalComparison<reverse>(str1, str1_length, str2, str2_length);
            }
            str1_data_offset += str1_total_len;
            str2_data_offset = str2_offsets[i];
        }
    }

    void executeStringString(
        const ColumnString & col_str1,
        size_t str1_offset,
        const ColumnString & col_str2,
        size_t str2_offset,
        size_t num_bytes,
        PaddedPODArray<Int8> & result_array) const
    {
        const auto & str1_data = col_str1.getChars();
        const auto & str1_offsets = col_str1.getOffsets();
        const auto & str2_data = col_str2.getChars();
        const auto & str2_offsets = col_str2.getOffsets();
        ColumnString::Offset str1_data_offset = 0;
        ColumnString::Offset str2_data_offset = 0;
        for (size_t i = 0, rows = col_str1.size(); i < rows; ++i)
        {
            const auto * str1 = reinterpret_cast<const char *>(str1_data.data() + str1_data_offset + str1_offset);
            size_t str1_total_len = str1_offsets[i] - str1_data_offset - 1;
            const auto * str2 = reinterpret_cast<const char *>(str2_data.data() + str2_data_offset + str2_offset);
            size_t str2_total_len = str2_offsets[i] - str2_data_offset - 1;

            if (!isOverflowComparison<false>(str1_total_len, str1_offset, str2_total_len, str2_offset, result_array[i]))
            {
                auto str1_length = std::min(num_bytes, str1_total_len - str1_offset);
                auto str2_length = std::min(num_bytes, str2_total_len - str2_offset);
                result_array[i] = normalComparison<false>(str1, str1_length, str2, str2_length);
            }

            str1_data_offset = str1_offsets[i];
            str2_data_offset = str2_offsets[i];
        }
    }

    void executeFixedStringFixedString(
        const ColumnFixedString & col_str1,
        size_t str1_offset,
        const ColumnFixedString & col_str2,
        size_t str2_offset,
        size_t num_bytes,
        PaddedPODArray<Int8> & result_array) const
    {
        size_t rows = col_str1.size();
        size_t str1_total_len = col_str1.getN();
        size_t str2_total_len = col_str2.getN();
        if (isOverflowComparison<false>(str1_total_len, str1_offset, str2_total_len, str2_offset, result_array, rows))
            return;

        const auto & str1_data = col_str1.getChars();
        const auto & str2_data = col_str2.getChars();
        auto str1_length = std::min(num_bytes, str1_total_len - str1_offset);
        auto str2_length = std::min(num_bytes, str2_total_len - str2_offset);
        size_t str1_data_offset = str1_offset;
        size_t str2_data_offset = str2_offset;
        for (size_t i = 0; i < rows; ++i)
        {
            const auto * str1 = reinterpret_cast<const char *>(str1_data.data() + str1_data_offset);
            const auto * str2 = reinterpret_cast<const char *>(str2_data.data() + str2_data_offset);
            result_array[i] = normalComparison<false>(str1, str1_length, str2, str2_length);
            str1_data_offset += str1_total_len;
            str2_data_offset += str2_total_len;
        }
    }

    void executeConstConst(
        const String & str1,
        size_t str1_offset,
        const String & str2,
        size_t str2_offset,
        size_t num_bytes,
        size_t rows,
        PaddedPODArray<Int8> & result_array) const
    {
        size_t str1_total_len = str1.size();
        size_t str2_total_len = str2.size();
        Int32 res = 0;
        if (str1_offset >= str1_total_len && str2_offset >= str2_total_len) [[unlikely]]
        {
            res = 0;
        }
        else if (str1_offset >= str1_total_len) [[unlikely]]
        {
            res = -1;
        }
        else if (str2_offset >= str2_total_len) [[unlikely]]
        {
            res = 1;
        }
        else
        {
            auto str1_length = std::min(num_bytes, str1_total_len - str1_offset);
            auto str2_length = std::min(num_bytes, str2_total_len - str2_offset);
            res = memcmpSmallLikeZeroPaddedAllowOverflow15(str1.data() + str1_offset, str1_length, str2.data() + str2_offset, str2_length);
        }
        for (size_t i = 0; i < rows; ++i)
            result_array[i] = res;
    }
};

REGISTER_FUNCTION(StringCompare)
{
    factory.registerFunction<FunctionStringCompare>(FunctionDocumentation{
        .description = R"(
                This function compares parts of two strings directly, without the need to copy the parts of the string into new columns.
                )",
        .syntax = R"(
        stringCompare(str1, str2)
        stringCompare(str1, str2, str1_off, str2_off, num_bytes)
        )",
        .arguments
        = {{"string1", "Required. The string to compare."},
           {"string2", "Required. The string to compare."},
           {"string1_offset", "Optional, positive number. The starting position (zero-based index) in `str1` from which the comparison begins."},
           {"string2_offset", "Optional, positive number. The starting position (zero-based index) in `str2` from which the comparison begins."},
           {"num_bytes", "The number of bytes to compare in both strings, starting from their respective offsets."}},
        .returned_value
        = R"(-1 if the substring from str1 is lexicographically smaller than the substring from str2, 0 if the substrings are equal, 1 if the substring from str1 is lexicographically greater than the substring from str2.)",
        .examples{
            {"typical",
             "SELECT stringCompare('123', '123', 0, 0, 3)",
             R"(
                ┌─stringCompar⋯', 0, 0, 3)─┐
             1. │                        0 │
                └──────────────────────────┘
                )"}},
        .category{"String"}});
}
}
