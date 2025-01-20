#include <algorithm>
#include <limits>
#include <type_traits>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/memcmpSmall.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

// usage:
// strncmp(s, offset_s, t, offset_t, n)
//  Compare s from offset_s to offset_s + n with t from offset_t to offset_t + n.
class FunctionStrncmp : public IFunction
{
public:
    static constexpr auto name = "strncmp";
    static FunctionPtr create(const ContextPtr /*context*/) { return std::make_shared<FunctionStrncmp>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 5; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeInt8>(); }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeInt8>(); }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        checkArguments(arguments);
        auto compare_result_column = ColumnInt8::create();
        compare_result_column->getData().resize(arguments[0].column->size());

        auto first_str_col = arguments[0].column;
        first_str_col = recursiveRemoveSparse(first_str_col->convertToFullColumnIfLowCardinality());
        const auto * first_string = checkAndGetColumn<ColumnString>(first_str_col.get());
        const auto * first_fixed_string = checkAndGetColumn<ColumnFixedString>(first_str_col.get());
        const auto * first_const_string = checkAndGetColumn<ColumnConst>(first_str_col.get());

        auto second_str_col = arguments[2].column;
        second_str_col = recursiveRemoveSparse(second_str_col->convertToFullColumnIfLowCardinality());
        const auto * second_string = checkAndGetColumn<ColumnString>(second_str_col.get());
        const auto * second_fixed_string = checkAndGetColumn<ColumnFixedString>(second_str_col.get());
        const auto * second_const_string = checkAndGetColumn<ColumnConst>(second_str_col.get());

        auto first_offset = arguments[1].column->getUInt(0);
        auto second_offset = arguments[3].column->getUInt(0);
        auto bytes = arguments[4].column->getUInt(0);

        auto & result = compare_result_column->getData();

        if (first_string && second_const_string)
        {
            executeStringWithConstString<true>(
                *first_string, first_offset, String(second_const_string->getDataAt(0)), second_offset, bytes, result);
        }
        else if (first_string && second_string)
        {
            executeStringWithString(*first_string, first_offset, *second_string, second_offset, bytes, result);
        }
        else if (first_fixed_string && second_fixed_string)
        {
            executeFixedStringWithFixedString(*first_fixed_string, first_offset, *second_fixed_string, second_offset, bytes, result);
        }
        else if (first_fixed_string && second_string)
        {
            executeFixedStringWithString<true>(*first_fixed_string, first_offset, *second_string, second_offset, bytes, result);
        }
        else if (first_string && second_fixed_string)
        {
            executeFixedStringWithString<false>(*second_fixed_string, second_offset, *first_string, first_offset, bytes, result);
        }
        else if (first_const_string && second_string)
        {
            executeStringWithConstString<false>(
                *second_string, second_offset, String(first_const_string->getDataAt(0)), first_offset, bytes, result);
        }
        else if (first_fixed_string && second_const_string)
        {
            executeFixedStringWithConst<true>(
                *first_fixed_string, first_offset, String(second_const_string->getDataAt(0)), second_offset, bytes, result);
        }
        else if (second_fixed_string && first_const_string)
        {
            executeFixedStringWithConst<false>(
                *second_fixed_string, second_offset, String(first_const_string->getDataAt(0)), first_offset, bytes, result);
        }
        else if (first_const_string && second_const_string)
        {
            executeConstWithConst(
                String(first_const_string->getDataAt(0)),
                first_offset,
                String(second_const_string->getDataAt(0)),
                second_offset,
                bytes,
                arguments[0].column->size(),
                result);
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal columns {} and {} of arguments of function {}",
                first_str_col->getName(),
                second_str_col->getName(),
                getName());
        }
        return std::move(compare_result_column);
    }

private:
    void checkArguments(const ColumnsWithTypeAndName & args) const
    {
        if (args.size() != 5)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires 5 arguments, but got {}.", getName(), args.size());

        if (!WhichDataType(args[0].type).isStringOrFixedString())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The first argument of function {} must be a string, but got {}.",
                getName(),
                args[0].type->getName());

        if (!WhichDataType(args[1].type).isNativeUInt() || !args[1].column->isConst())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The second argument of function {} must be a constant non-negative, but got {}.",
                getName(),
                args[1].column->getName());

        if (!WhichDataType(args[2].type).isStringOrFixedString())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The third argument of function {} must be a string, but got {}.",
                getName(),
                args[2].column->getName());

        if (!WhichDataType(args[3].type).isNativeUInt() || !args[3].column->isConst())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The fourth argument of function {} must be a constant non-negative, but got {}.",
                getName(),
                args[3].column->getName());

        if (!WhichDataType(args[4].type).isNativeUInt() || !args[4].column->isConst())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The fifth argument of function {} must be a constant non-negative, but got {}.",
                getName(),
                args[4].column->getName());
    }

    template <bool positive>
    static Int8 normalComparison(const char * first_str, size_t first_str_len, const char * second_str, size_t second_str_len)
    {
        if constexpr (positive)
            return static_cast<Int8>(memcmpSmallLikeZeroPaddedAllowOverflow15(first_str, first_str_len, second_str, second_str_len));
        else
            return static_cast<Int8>(memcmpSmallLikeZeroPaddedAllowOverflow15(second_str, second_str_len, first_str, first_str_len));
    }

    template <bool positive>
    static bool isOverflowComparison(size_t first_str_len, size_t first_offset, size_t second_str_len, size_t second_offset, Int8 & result)
    {
        if (first_offset >= first_str_len && second_offset >= second_str_len)
        {
            result = 0;
            return true;
        }
        else if (first_offset >= first_str_len)
        {
            if constexpr (positive)
                result = -1;
            else
                result = 1;
            return true;
        }
        else if (second_offset >= second_str_len)
        {
            if constexpr (positive)
                result = 1;
            else
                result = -1;
            return true;
        }
        return false;
    }

    template <bool positive>
    static bool isOverflowComparison(
        size_t first_str_len, size_t first_offset, size_t second_str_len, size_t scond_offset, PaddedPODArray<Int8> & result, size_t rows)
    {
        bool is_overflow = false;
        Int8 res = 0;
        if (first_offset >= first_str_len && scond_offset >= second_str_len)
        {
            res = 0;
            is_overflow = true;
        }
        else if (first_offset >= first_str_len)
        {
            if constexpr (positive)
                res = -1;
            else
                res = 1;
            is_overflow = true;
        }
        else if (scond_offset >= second_str_len)
        {
            if constexpr (positive)
                res = 1;
            else
                res = -1;
            is_overflow = true;
        }
        for (size_t i = 0; i < rows; ++i)
            result[i] = res;
        return is_overflow;
    }

    template <bool positive = true>
    void executeStringWithConstString(
        const ColumnString & first_strs,
        size_t first_offset,
        const String & second_str,
        size_t second_offset,
        size_t bytes,
        PaddedPODArray<Int8> & result) const
    {
        const auto & first_strs_data = first_strs.getChars();
        const auto & first_offsets = first_strs.getOffsets();
        size_t prev_first_offset = 0;
        auto second_str_bytes = second_offset >= second_str.size() ? 0 : std::min(bytes, second_str.size() - second_offset);
        for (size_t i = 0, size = first_strs.size(); i < size; ++i)
        {
            size_t first_str_len = first_offsets[i] - prev_first_offset - 1;
            if (!isOverflowComparison<positive>(first_str_len, first_offset, second_str.size(), second_offset, result[i]))
            {
                const auto * first_str = first_strs_data.data() + prev_first_offset + first_offset;
                result[i] = normalComparison<positive>(
                    reinterpret_cast<const char *>(first_str),
                    std::min(bytes, first_str_len - first_offset),
                    second_str.data() + second_offset,
                    second_str_bytes);
            }

            prev_first_offset = first_offsets[i];
        }
    }

    template <bool positive = true>
    void executeFixedStringWithConst(
        const ColumnFixedString & first_strs,
        size_t first_offset,
        const String & second_str,
        size_t second_offset,
        size_t bytes,
        PaddedPODArray<Int8> & result) const
    {
        size_t rows = first_strs.size();
        size_t first_str_len = first_strs.getN();
        if (isOverflowComparison<positive>(first_str_len, first_offset, second_str.size(), second_offset, result, rows))
            return;

        const auto & first_strs_data = first_strs.getChars();
        auto first_str_bytes = std::min(bytes, first_str_len - first_offset);
        auto second_str_bytes = std::min(bytes, second_str.size() - second_offset);
        const char * second_str_p = second_str.data() + second_offset;
        for (size_t i = 0; i < first_strs.size(); ++i)
        {
            const auto * first_str = reinterpret_cast<const char *>(first_strs_data.data() + i * first_str_len + first_offset);
            result[i] = normalComparison<positive>(first_str, first_str_bytes, second_str_p, second_str_bytes);
        }
    }

    template <bool positive = true>
    void executeFixedStringWithString(
        const ColumnFixedString & fist_strs,
        size_t first_offset,
        const ColumnString & second_strs,
        size_t second_offset,
        size_t bytes,
        PaddedPODArray<Int8> & result) const
    {
        auto first_str_len = fist_strs.getN();
        const auto & first_strs_data = fist_strs.getChars();
        const auto & second_strs_data = second_strs.getChars();
        const auto & second_offsets = second_strs.getOffsets();
        ColumnString::Offset prev_second_offset = 0;
        for (size_t i = 0, rows = fist_strs.size(); i < rows; ++i)
        {
            const auto * first_str = reinterpret_cast<const char *>(first_strs_data.data() + i * first_str_len + first_offset);
            auto first_str_bytes = std::min(bytes, first_str_len - first_offset);
            const auto * second_str = reinterpret_cast<const char *>(second_strs_data.data() + prev_second_offset + second_offset);
            size_t second_str_len = second_offsets[i] - prev_second_offset - 1;
            if (!isOverflowComparison<positive>(first_str_len, first_offset, second_str_len, second_offset, result[i]))
            {
                auto second_str_bytes = second_offset >= second_str_len ? 0 : std::min(bytes, second_str_len - second_offset);
                result[i] = normalComparison<positive>(first_str, first_str_bytes, second_str, second_str_bytes);
            }

            prev_second_offset = second_offsets[i];
        }
    }

    void executeStringWithString(
        const ColumnString & fist_strs,
        size_t first_offset,
        const ColumnString & second_strs,
        size_t second_offset,
        size_t bytes,
        PaddedPODArray<Int8> & result) const
    {
        const auto & first_strs_data = fist_strs.getChars();
        const auto & first_offsets = fist_strs.getOffsets();
        const auto & second_strs_data = second_strs.getChars();
        const auto & second_offsets = second_strs.getOffsets();
        ColumnString::Offset prev_first_offset = 0;
        ColumnString::Offset prev_second_offset = 0;
        for (size_t i = 0, size = fist_strs.size(); i < size; ++i)
        {
            const auto * first_str = reinterpret_cast<const char *>(first_strs_data.data() + prev_first_offset + first_offset);
            size_t first_str_len = first_offsets[i] - prev_first_offset - 1;
            const auto * second_str = reinterpret_cast<const char *>(second_strs_data.data() + prev_second_offset + second_offset);
            size_t second_str_len = second_offsets[i] - prev_second_offset - 1;

            if (!isOverflowComparison<true>(first_str_len, first_offset, second_str_len, second_offset, result[i]))
            {
                auto first_str_bytes = std::min(bytes, first_str_len - first_offset);
                auto second_str_bytes = std::min(bytes, second_str_len - second_offset);
                result[i] = normalComparison<true>(first_str, first_str_bytes, second_str, second_str_bytes);
            }

            prev_first_offset = first_offsets[i];
            prev_second_offset = second_offsets[i];
        }
    }

    void executeFixedStringWithFixedString(
        const ColumnFixedString & first_strs,
        size_t first_offset,
        const ColumnFixedString & second_strs,
        size_t second_offset,
        size_t bytes,
        PaddedPODArray<Int8> & result) const
    {
        size_t rows = first_strs.size();
        size_t first_str_len = first_strs.getN();
        size_t second_str_len = second_strs.getN();
        if (isOverflowComparison<true>(first_str_len, first_offset, second_str_len, second_offset, result, rows))
            return;

        const auto & first_strs_data = first_strs.getChars();
        const auto & second_strs_data = second_strs.getChars();
        auto first_str_bytes = std::min(bytes, first_str_len - first_offset);
        auto second_str_bytes = std::min(bytes, second_str_len - second_offset);
        for (size_t i = 0; i < rows; ++i)
        {
            const auto * first_str = reinterpret_cast<const char *>(first_strs_data.data() + i * first_str_len + first_offset);
            const auto * second_str = reinterpret_cast<const char *>(second_strs_data.data() + i * second_str_len + second_offset);
            result[i] = normalComparison<true>(first_str, first_str_bytes, second_str, second_str_bytes);
        }
    }

    void executeConstWithConst(
        const String & first_str,
        size_t first_offset,
        const String & second_str,
        size_t second_offset,
        size_t bytes,
        size_t rows,
        PaddedPODArray<Int8> & result) const
    {
        size_t first_str_len = first_str.size();
        size_t second_str_len = second_str.size();
        Int32 res = 0;
        if (first_offset >= first_str_len && second_offset >= second_str_len) [[unlikely]]
        {
            res = 0;
        }
        else if (first_offset >= first_str_len) [[unlikely]]
        {
            res = -1;
        }
        else if (second_offset >= second_str_len) [[unlikely]]
        {
            res = 1;
        }
        else
        {
            auto first_str_bytes = std::min(bytes, first_str_len - first_offset);
            auto second_str_bytes = std::min(bytes, second_str_len - second_offset);
            res = memcmpSmallLikeZeroPaddedAllowOverflow15(
                first_str.data() + first_offset, first_str_bytes, second_str.data() + second_offset, second_str_bytes);
        }
        for (size_t i = 0; i < rows; ++i)
            result[i] = res;
    }
};

REGISTER_FUNCTION(Strncmp)
{
    factory.registerFunction<FunctionStrncmp>(FunctionDocumentation{
        .description = R"(
                This function could compare parts of two strings directly, without the need to copy the parts of the string into new columns.
                )",
        .syntax = R"(strncmp(a_str, a_offset, b_str, b_offset, n))",
        .arguments
        = {{"a_str", "The first string to compare."},
           {"a_offset", "The starting position (zero-based index) in `a_str` from which the comparison begins."},
           {"b_str", "The second string to compare."},
           {"b_offset", "The starting position (zero-based index) in `b_str` from which the comparison begins."},
           {"n", "The number of bytes to compare in both strings, starting from their respective offsets."}},
        .returned_value
        = R"(-1 if the substring from a_str is lexicographically smaller than the substring from b_str, 0 if the substrings are equal, 1 if the substring from a_str is lexicographically greater than the substring from b_str.)",
        .examples{
            {"typical",
             "SELECT strncmp('123', 0, '123', 0, 3)",
             R"(
                   ┌─strncmp('123⋯123', 0, 3)─┐
                1. │                        0 │
                   └──────────────────────────┘
                )"}},
        .category{"String"}});
}
}
