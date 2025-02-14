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
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/memcmpSmall.h>

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
            {"str1", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String"},
            {"str2", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String"},
        };
        FunctionArgumentDescriptors optional_args{
            {"str1_offset",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt),
             static_cast<FunctionArgumentDescriptor::ColumnValidator>(&FunctionStringCompare::isConstColumn),
             "UInt"},
            {"str2_offset",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt),
             static_cast<FunctionArgumentDescriptor::ColumnValidator>(&FunctionStringCompare::isConstColumn),
             "UInt"},
            {"n",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt),
             static_cast<FunctionArgumentDescriptor::ColumnValidator>(&FunctionStringCompare::isConstColumn),
             "UInt"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeInt8>(); }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        auto compare_result_column = ColumnInt8::create();

        auto prepare_string_args
            = [](const ColumnPtr & column) -> std::tuple<const ColumnString *, const ColumnFixedString *, const ColumnConst *>
        {
            return {
                checkAndGetColumn<ColumnString>(column.get()),
                checkAndGetColumn<ColumnFixedString>(column.get()),
                checkAndGetColumn<ColumnConst>(column.get())};
        };
        auto [str1_col, fixed_str1_col, const_str1_col] = prepare_string_args(arguments[0].column);
        auto [str2_col, fixed_str2_col, const_str2_col] = prepare_string_args(arguments[1].column);

        auto str1_offset = arguments.size() > 2 ? arguments[2].column->getUInt(0) : 0;
        auto str2_offset = arguments.size() > 2 ? arguments[3].column->getUInt(0) : 0;
        auto bytes = arguments.size() > 2 ? arguments[4].column->getUInt(0) : std::numeric_limits<size_t>::max();
        if (!bytes)
        {
            compare_result_column->insertMany(0, arguments[0].column->size());
            return std::move(compare_result_column);
        }

        compare_result_column->getData().resize(arguments[0].column->size());
        auto & result_array = compare_result_column->getData();

        if (str1_col && const_str2_col)
        {
            executeStringConst<false>(*str1_col, str1_offset, String(const_str2_col->getDataAt(0)), str2_offset, bytes, result_array);
        }
        else if (const_str1_col && str2_col)
        {
            executeStringConst<true>(*str2_col, str2_offset, String(const_str1_col->getDataAt(0)), str1_offset, bytes, result_array);
        }
        else if (str1_col && str2_col)
        {
            executeStringString(*str1_col, str1_offset, *str2_col, str2_offset, bytes, result_array);
        }
        else if (fixed_str1_col && fixed_str2_col)
        {
            executeFixedStringFixedString(*fixed_str1_col, str1_offset, *fixed_str2_col, str2_offset, bytes, result_array);
        }
        else if (fixed_str1_col && str2_col)
        {
            executeFixedStringString<false>(*fixed_str1_col, str1_offset, *str2_col, str2_offset, bytes, result_array);
        }
        else if (str1_col && fixed_str2_col)
        {
            executeFixedStringString<true>(*fixed_str2_col, str2_offset, *str1_col, str1_offset, bytes, result_array);
        }
        else if (fixed_str1_col && const_str2_col)
        {
            executeFixedStringConst<false>(
                *fixed_str1_col, str1_offset, String(const_str2_col->getDataAt(0)), str2_offset, bytes, result_array);
        }
        else if (fixed_str2_col && const_str1_col)
        {
            executeFixedStringConst<true>(
                *fixed_str2_col, str2_offset, String(const_str1_col->getDataAt(0)), str1_offset, bytes, result_array);
        }
        else if (const_str1_col && const_str2_col)
        {
            executeConstConst(
                String(const_str1_col->getDataAt(0)),
                str1_offset,
                String(const_str2_col->getDataAt(0)),
                str2_offset,
                bytes,
                arguments[0].column->size(),
                result_array);
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal columns {} and {} of arguments of function {}",
                arguments[0].column->getName(),
                arguments[1].column->getName(),
                getName());
        }
        return std::move(compare_result_column);
    }

private:
    static bool isConstColumn(const IColumn & column) { return column.isConst(); }

    template <bool reversed>
    static Int8 normalComparison(const char * str1, size_t str1_len, const char * str2, size_t str2_len)
    {
        if constexpr (reversed)
            return static_cast<Int8>(memcmpSmallLikeZeroPaddedAllowOverflow15(str2, str2_len, str1, str1_len));
        else
            return static_cast<Int8>(memcmpSmallLikeZeroPaddedAllowOverflow15(str1, str1_len, str2, str2_len));
    }

    template <bool reversed>
    static bool isOverflowComparison(size_t str1_len, size_t str1_offset, size_t str2_len, size_t str2_offset, Int8 & result)
    {
        if (str1_offset >= str1_len && str2_offset >= str2_len)
        {
            result = 0;
            return true;
        }
        else if (str1_offset >= str1_len)
        {
            if constexpr (reversed)
                result = 1;
            else
                result = -1;
            return true;
        }
        else if (str2_offset >= str2_len)
        {
            if constexpr (reversed)
                result = -1;
            else
                result = 1;
            return true;
        }
        return false;
    }

    template <bool reversed>
    static bool isOverflowComparison(
        size_t str1_len, size_t str1_offset, size_t str2_len, size_t str2_offset, PaddedPODArray<Int8> & result_array, size_t rows)
    {
        bool is_overflow = false;
        Int8 res = 0;
        if (str1_offset >= str1_len && str2_offset >= str2_len)
        {
            res = 0;
            is_overflow = true;
        }
        else if (str1_offset >= str1_len)
        {
            if constexpr (reversed)
                res = 1;
            else
                res = -1;
            is_overflow = true;
        }
        else if (str2_offset >= str2_len)
        {
            if constexpr (reversed)
                res = -1;
            else
                res = 1;
            is_overflow = true;
        }
        for (size_t i = 0; i < rows; ++i)
            result_array[i] = res;
        return is_overflow;
    }

    template <bool reversed>
    void executeStringConst(
        const ColumnString & str1_col,
        size_t str1_offset,
        const String & str2,
        size_t str2_offset,
        size_t bytes,
        PaddedPODArray<Int8> & result_array) const
    {
        const auto & str1_data = str1_col.getChars();
        const auto & str1_offsets = str1_col.getOffsets();
        size_t str1_data_offset = 0;
        auto str2_len = str2_offset >= str2.size() ? 0 : std::min(bytes, str2.size() - str2_offset);
        for (size_t i = 0, size = str1_col.size(); i < size; ++i)
        {
            size_t str1_total_len = str1_offsets[i] - str1_data_offset - 1;
            if (!isOverflowComparison<reversed>(str1_total_len, str1_offset, str2.size(), str2_offset, result_array[i]))
            {
                const auto * str1 = str1_data.data() + str1_data_offset + str1_offset;
                result_array[i] = normalComparison<reversed>(
                    reinterpret_cast<const char *>(str1),
                    std::min(bytes, str1_total_len - str1_offset),
                    str2.data() + str2_offset,
                    str2_len);
            }

            str1_data_offset = str1_offsets[i];
        }
    }

    template <bool reversed>
    void executeFixedStringConst(
        const ColumnFixedString & str1_col,
        size_t str1_offset,
        const String & str2,
        size_t str2_offset,
        size_t bytes,
        PaddedPODArray<Int8> & result_array) const
    {
        size_t rows = str1_col.size();
        size_t str1_total_len = str1_col.getN();
        if (isOverflowComparison<reversed>(str1_total_len, str1_offset, str2.size(), str2_offset, result_array, rows))
            return;

        const auto & str1_data = str1_col.getChars();
        size_t str1_data_offset = str1_offset;
        auto str1_len = std::min(bytes, str1_total_len - str1_offset);
        auto str2_len = std::min(bytes, str2.size() - str2_offset);
        for (size_t i = 0; i < rows; ++i)
        {
            const auto * str1 = reinterpret_cast<const char *>(str1_data.data() + str1_data_offset);
            result_array[i] = normalComparison<reversed>(str1, str1_len, str2.data() + str2_offset, str2_len);
            str1_data_offset += str1_total_len;
        }
    }

    template <bool reversed>
    void executeFixedStringString(
        const ColumnFixedString & str1_col,
        size_t str1_offset,
        const ColumnString & str2_col,
        size_t str2_offset,
        size_t bytes,
        PaddedPODArray<Int8> & result_array) const
    {
        auto str1_total_len = str1_col.getN();
        const auto & str1_data = str1_col.getChars();
        const auto & str2_data = str2_col.getChars();
        const auto & str2_offsets = str2_col.getOffsets();
        size_t str1_data_offset = str1_offset;
        size_t str2_data_offset = 0;
        for (size_t i = 0, rows = str1_col.size(); i < rows; ++i)
        {
            const auto * str1 = reinterpret_cast<const char *>(str1_data.data() + str1_data_offset);
            auto str1_len = std::min(bytes, str1_total_len - str1_offset);
            const auto * str2 = reinterpret_cast<const char *>(str2_data.data() + str2_data_offset + str2_offset);
            size_t str2_total_len = str2_offsets[i] - str2_data_offset - 1;
            if (!isOverflowComparison<reversed>(str1_total_len, str1_offset, str2_total_len, str2_offset, result_array[i]))
            {
                auto str2_len = str2_offset >= str2_total_len ? 0 : std::min(bytes, str2_total_len - str2_offset);
                result_array[i] = normalComparison<reversed>(str1, str1_len, str2, str2_len);
            }
            str1_data_offset += str1_total_len;
            str2_data_offset = str2_offsets[i];
        }
    }

    void executeStringString(
        const ColumnString & str1_col,
        size_t str1_offset,
        const ColumnString & str2_col,
        size_t str2_offset,
        size_t bytes,
        PaddedPODArray<Int8> & result_array) const
    {
        const auto & str1_data = str1_col.getChars();
        const auto & str1_offsets = str1_col.getOffsets();
        const auto & str2_data = str2_col.getChars();
        const auto & str2_offsets = str2_col.getOffsets();
        ColumnString::Offset str1_data_offset = 0;
        ColumnString::Offset str2_data_offset = 0;
        for (size_t i = 0, rows = str1_col.size(); i < rows; ++i)
        {
            const auto * str1 = reinterpret_cast<const char *>(str1_data.data() + str1_data_offset + str1_offset);
            size_t str1_total_len = str1_offsets[i] - str1_data_offset - 1;
            const auto * str2 = reinterpret_cast<const char *>(str2_data.data() + str2_data_offset + str2_offset);
            size_t str2_total_len = str2_offsets[i] - str2_data_offset - 1;

            if (!isOverflowComparison<false>(str1_total_len, str1_offset, str2_total_len, str2_offset, result_array[i]))
            {
                auto str1_len = std::min(bytes, str1_total_len - str1_offset);
                auto str2_len = std::min(bytes, str2_total_len - str2_offset);
                result_array[i] = normalComparison<false>(str1, str1_len, str2, str2_len);
            }

            str1_data_offset = str1_offsets[i];
            str2_data_offset = str2_offsets[i];
        }
    }

    void executeFixedStringFixedString(
        const ColumnFixedString & str1_col,
        size_t str1_offset,
        const ColumnFixedString & str2_col,
        size_t str2_offset,
        size_t bytes,
        PaddedPODArray<Int8> & result_array) const
    {
        size_t rows = str1_col.size();
        size_t str1_total_len = str1_col.getN();
        size_t str2_total_len = str2_col.getN();
        if (isOverflowComparison<false>(str1_total_len, str1_offset, str2_total_len, str2_offset, result_array, rows))
            return;

        const auto & str1_data = str1_col.getChars();
        const auto & str2_data = str2_col.getChars();
        auto str1_len = std::min(bytes, str1_total_len - str1_offset);
        auto str2_len = std::min(bytes, str2_total_len - str2_offset);
        size_t str1_data_offset = str1_offset;
        size_t str2_data_offset = str2_offset;
        for (size_t i = 0; i < rows; ++i)
        {
            const auto * str1 = reinterpret_cast<const char *>(str1_data.data() + str1_data_offset);
            const auto * str2 = reinterpret_cast<const char *>(str2_data.data() + str2_data_offset);
            result_array[i] = normalComparison<false>(str1, str1_len, str2, str2_len);
            str1_data_offset += str1_total_len;
            str2_data_offset += str2_total_len;
        }
    }

    void executeConstConst(
        const String & str1,
        size_t str1_offset,
        const String & str2,
        size_t str2_offset,
        size_t bytes,
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
            auto str1_len = std::min(bytes, str1_total_len - str1_offset);
            auto str2_len = std::min(bytes, str2_total_len - str2_offset);
            res = memcmpSmallLikeZeroPaddedAllowOverflow15(str1.data() + str1_offset, str1_len, str2.data() + str2_offset, str2_len);
        }
        for (size_t i = 0; i < rows; ++i)
            result_array[i] = res;
    }
};

REGISTER_FUNCTION(StringCompare)
{
    factory.registerFunction<FunctionStringCompare>(FunctionDocumentation{
        .description = R"(
                This function could compare parts of two strings directly, without the need to copy the parts of the string into new columns.
                )",
        .syntax = R"(
        stringCompare(str1, str2)
        stringCompare(str1, str2, str1_off, str2_off, n)
        )",
        .arguments
        = {{"str1", "Required. The string to compare."},
           {"str2", "Required. The string to compare."},
           {"str1_off", "Optional, positive number. The starting position (zero-based index) in `str1` from which the comparison begins."},
           {"str2_off", "Optional, positive number. The starting position (zero-based index) in `str2` from which the comparison begins."},
           {"n", "The number of bytes to compare in both strings, starting from their respective offsets."}},
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
