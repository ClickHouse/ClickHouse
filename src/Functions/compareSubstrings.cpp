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
}

// compareSubstrings(str1, str2, offset1, offset2, num_bytes):
// - Compare str1 from offset1 to offset1 + num_bytes with str2 from offset2 to offset2 + num_bytes.
class FunctionCompareSubstrings : public IFunction
{
public:
    static constexpr auto name = "compareSubstrings";
    static FunctionPtr create(const ContextPtr /*context*/) { return std::make_shared<FunctionCompareSubstrings>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"string1", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String"},
            {"string2", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String"},
            {"string1_offset", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt), isColumnConst, "const UInt*"},
            {"string2_offset", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt), isColumnConst, "const UInt*"},
            {"num_bytes", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt), isColumnConst, "const UInt*"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args);

        return std::make_shared<DataTypeInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeInt8>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto col_result = ColumnInt8::create();

        if (input_rows_count == 0)
            return col_result;

        auto try_cast_column
            = [](const ColumnPtr & column) -> std::tuple<const ColumnString *, const ColumnFixedString *, const ColumnConst *>
        {
            return {
                checkAndGetColumn<ColumnString>(column.get()),
                checkAndGetColumn<ColumnFixedString>(column.get()),
                checkAndGetColumn<ColumnConst>(column.get())};
        };
        auto [col_str1, fixed_str1_col, col_str1_const] = try_cast_column(arguments[0].column);
        auto [col_str2, fixed_str2_col, col_str2_const] = try_cast_column(arguments[1].column);

        bool has_five_args = arguments.size() == 5;

        size_t str1_offset = has_five_args ? arguments[2].column->getUInt(0) : 0;
        size_t str2_offset = has_five_args ? arguments[3].column->getUInt(0) : 0;

        size_t num_bytes = has_five_args ? arguments[4].column->getUInt(0) : std::numeric_limits<size_t>::max();
        if (num_bytes == 0)
        {
            col_result->insertMany(0, input_rows_count);
            return col_result;
        }

        col_result->getData().resize(input_rows_count);
        auto & result = col_result->getData();

        if (col_str1 && col_str2)
            executeStringString(*col_str1, *col_str2, str1_offset, str2_offset, num_bytes, input_rows_count, result);
        else if (col_str1 && col_str2_const)
            executeStringConst<false>(*col_str1, col_str2_const->getDataAt(0).toView(), str1_offset, str2_offset, num_bytes, input_rows_count, result);
        else if (col_str1_const && col_str2)
            executeStringConst<true>(*col_str2, col_str1_const->getDataAt(0).toView(), str2_offset, str1_offset, num_bytes, input_rows_count, result);
        else if (fixed_str1_col && fixed_str2_col)
            executeFixedStringFixedString(*fixed_str1_col, *fixed_str2_col, str1_offset, str2_offset, num_bytes, input_rows_count, result);
        else if (col_str1 && fixed_str2_col)
            executeFixedStringString<true>(*fixed_str2_col, *col_str1, str2_offset, str1_offset, num_bytes, input_rows_count, result);
        else if (fixed_str1_col && col_str2)
            executeFixedStringString<false>(*fixed_str1_col, *col_str2, str1_offset, str2_offset, num_bytes, input_rows_count, result);
        else if (fixed_str1_col && col_str2_const)
            executeFixedStringConst<false>(*fixed_str1_col, col_str2_const->getDataAt(0).toView(), str1_offset, str2_offset, num_bytes, input_rows_count, result);
        else if (col_str1_const && fixed_str2_col)
            executeFixedStringConst<true>(*fixed_str2_col, col_str1_const->getDataAt(0).toView(), str2_offset, str1_offset, num_bytes, input_rows_count, result);
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal columns {} and {} of arguments of function {}",
                arguments[0].column->getName(),
                arguments[1].column->getName(),
                getName());
        return col_result;
    }

private:
    template <bool reverse>
    static Int8 normalComparison(const char * str1, size_t str1_length, const char * str2, size_t str2_length)
    {
        if constexpr (reverse)
            return static_cast<Int8>(memcmpSmallLikeZeroPaddedAllowOverflow15(str2, str2_length, str1, str1_length));
        else
            return static_cast<Int8>(memcmpSmallLikeZeroPaddedAllowOverflow15(str1, str1_length, str2, str2_length));
    }

    /// If offset is beyond end of input string(s), compute result and return true, else return false.
    template <bool reverse>
    static bool isOverflowComparison(
        size_t str1_length, size_t str1_offset,
        size_t str2_length, size_t str2_offset,
        Int8 & result)
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

    /// If offset is beyond end of input string(s), compute result and return true, else return false.
    template <bool reverse>
    static bool isOverflowComparison(
        size_t str1_length, size_t str1_offset,
        size_t str2_length, size_t str2_offset,
        size_t input_rows_count,
        PaddedPODArray<Int8> & result)
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
        for (size_t i = 0; i < input_rows_count; ++i)
            result[i] = res;
        return is_overflow;
    }

    void executeStringString(
        const ColumnString & col_str1,
        const ColumnString & col_str2,
        size_t str1_offset,
        size_t str2_offset,
        size_t num_bytes,
        size_t input_rows_count,
        PaddedPODArray<Int8> & result) const
    {
        const auto & str1_data = col_str1.getChars();
        const auto & str1_offsets = col_str1.getOffsets();

        const auto & str2_data = col_str2.getChars();
        const auto & str2_offsets = col_str2.getOffsets();

        ColumnString::Offset str1_data_offset = 0;
        ColumnString::Offset str2_data_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto * str1 = reinterpret_cast<const char *>(str1_data.data() + str1_data_offset + str1_offset);
            size_t str1_length = str1_offsets[i] - str1_data_offset;

            const auto * str2 = reinterpret_cast<const char *>(str2_data.data() + str2_data_offset + str2_offset);
            size_t str2_length = str2_offsets[i] - str2_data_offset;

            if (!isOverflowComparison<false>(str1_length, str1_offset, str2_length, str2_offset, result[i]))
            {
                size_t str1_adjusted_length = std::min(num_bytes, str1_length - str1_offset);
                size_t str2_adjusted_length = std::min(num_bytes, str2_length - str2_offset);
                result[i] = normalComparison<false>(
                    str1, str1_adjusted_length,
                    str2, str2_adjusted_length);
            }

            str1_data_offset = str1_offsets[i];
            str2_data_offset = str2_offsets[i];
        }
    }

    template <bool reverse>
    void executeStringConst(
        const ColumnString & col_str1,
        std::string_view str2,
        size_t str1_offset,
        size_t str2_offset,
        size_t num_bytes,
        size_t input_rows_count,
        PaddedPODArray<Int8> & result) const
    {
        const auto & str1_data = col_str1.getChars();
        const auto & str1_offsets = col_str1.getOffsets();

        ColumnString::Offset str1_data_offset = 0;

        size_t str2_adjusted_length = str2_offset >= str2.size() ? 0 : std::min(num_bytes, str2.size() - str2_offset);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t str1_length = str1_offsets[i] - str1_data_offset;
            if (!isOverflowComparison<reverse>(str1_length, str1_offset, str2.size(), str2_offset, result[i]))
            {
                const auto * str1 = reinterpret_cast<const char *>(str1_data.data() + str1_data_offset + str1_offset);
                size_t str1_adjusted_length = std::min(num_bytes, str1_length - str1_offset);
                result[i] = normalComparison<reverse>(
                    str1, str1_adjusted_length,
                    str2.data() + str2_offset, str2_adjusted_length);
            }

            str1_data_offset = str1_offsets[i];
        }
    }

    void executeFixedStringFixedString(
        const ColumnFixedString & col_str1,
        const ColumnFixedString & col_str2,
        size_t str1_offset,
        size_t str2_offset,
        size_t num_bytes,
        size_t input_rows_count,
        PaddedPODArray<Int8> & result) const
    {
        size_t str1_length = col_str1.getN();
        size_t str2_length = col_str2.getN();

        if (isOverflowComparison<false>(str1_length, str1_offset, str2_length, str2_offset, input_rows_count, result))
            return;

        const auto & str1_data = col_str1.getChars();
        const auto & str2_data = col_str2.getChars();

        auto str1_adjusted_length = std::min(num_bytes, str1_length - str1_offset);
        auto str2_adjusted_length = std::min(num_bytes, str2_length - str2_offset);

        size_t str1_data_offset = str1_offset;
        size_t str2_data_offset = str2_offset;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto * str1 = reinterpret_cast<const char *>(str1_data.data() + str1_data_offset);
            const auto * str2 = reinterpret_cast<const char *>(str2_data.data() + str2_data_offset);

            result[i] = normalComparison<false>(
                str1, str1_adjusted_length,
                str2, str2_adjusted_length);

            str1_data_offset += str1_length;
            str2_data_offset += str2_length;
        }
    }

    template <bool reverse>
    void executeFixedStringString(
        const ColumnFixedString & col_str1,
        const ColumnString & col_str2,
        size_t str1_offset,
        size_t str2_offset,
        size_t num_bytes,
        size_t input_rows_count,
        PaddedPODArray<Int8> & result) const
    {
        auto str1_length = col_str1.getN();

        const auto & str1_data = col_str1.getChars();
        const auto & str2_data = col_str2.getChars();

        const auto & str2_offsets = col_str2.getOffsets();

        size_t str1_data_offset = str1_offset;
        size_t str2_data_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto * str1 = reinterpret_cast<const char *>(str1_data.data() + str1_data_offset);
            auto str1_adjusted_length = std::min(num_bytes, str1_length - str1_offset);

            const auto * str2 = reinterpret_cast<const char *>(str2_data.data() + str2_data_offset + str2_offset);
            size_t str2_length = str2_offsets[i] - str2_data_offset;

            if (!isOverflowComparison<reverse>(str1_length, str1_offset, str2_length, str2_offset, result[i]))
            {
                auto str2_adjusted_length = str2_offset >= str2_length ? 0 : std::min(num_bytes, str2_length - str2_offset);
                result[i] = normalComparison<reverse>(
                    str1, str1_adjusted_length,
                    str2, str2_adjusted_length);
            }

            str1_data_offset += str1_length;
            str2_data_offset = str2_offsets[i];
        }
    }

    template <bool reverse>
    void executeFixedStringConst(
        const ColumnFixedString & col_str1,
        std::string_view str2,
        size_t str1_offset,
        size_t str2_offset,
        size_t num_bytes,
        size_t input_rows_count,
        PaddedPODArray<Int8> & result) const
    {
        size_t str1_length = col_str1.getN();
        if (isOverflowComparison<reverse>(str1_length, str1_offset, str2.size(), str2_offset, input_rows_count, result))
            return;

        const auto & str1_data = col_str1.getChars();
        size_t str1_data_offset = str1_offset;

        auto str1_adjusted_length = std::min(num_bytes, str1_length - str1_offset);
        auto str2_adjusted_length = std::min(num_bytes, str2.size() - str2_offset);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto * str1 = reinterpret_cast<const char *>(str1_data.data() + str1_data_offset);
            result[i] = normalComparison<reverse>(
                str1, str1_adjusted_length,
                str2.data() + str2_offset, str2_adjusted_length);

            str1_data_offset += str1_length;
        }
    }
};

REGISTER_FUNCTION(CompareSubstrings)
{
    FunctionDocumentation::Description description = R"(
Compares two strings lexicographically.
)";
    FunctionDocumentation::Syntax syntax = "compareSubstrings(s1, s2, s1_offset, s2_offset, num_bytes)";
    FunctionDocumentation::Arguments arguments = {
        {"s1", "The first string to compare.", {"String"}},
        {"s2", "The second string to compare.", {"String"}},
        {"s1_offset", "The position (zero-based) in `s1` from which the comparison starts.", {"UInt*"}},
        {"s2_offset", "The position (zero-based index) in `s2` from which the comparison starts.", {"UInt*"}},
        {"num_bytes", "The maximum number of bytes to compare in both strings. If `s1_offset` (or `s2_offset`) + `num_bytes` exceeds the end of an input string, `num_bytes` will be reduced accordingly.", {"UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {R"(
Returns:
- `-1` if `s1`[`s1_offset` : `s1_offset` + `num_bytes`] < `s2`[`s2_offset` : `s2_offset` + `num_bytes`].
- `0` if `s1`[`s1_offset` : `s1_offset` + `num_bytes`] = `s2`[`s2_offset` : `s2_offset` + `num_bytes`].
- `1` if `s1`[`s1_offset` : `s1_offset` + `num_bytes`] > `s2`[`s2_offset` : `s2_offset` + `num_bytes`].
    )",
    {"Int8"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT compareSubstrings('Saxony', 'Anglo-Saxon', 0, 6, 5) AS result",
        R"(
┌─result─┐
│      0 │
└────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCompareSubstrings>(documentation);
}
}
