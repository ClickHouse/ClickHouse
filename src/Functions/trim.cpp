#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/IColumn.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Common/memcpySmall.h>
#include <base/find_symbols.h>

#include <array>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Membership table for the optional second argument of trim* functions.
/// Unlike `SearchSymbols` (a SIMD primitive capped at 16 symbols), it supports
/// a trim character set of any length and looks up each byte in O(1).
using TrimCharsTable = std::array<bool, 256>;

class FunctionTrim final : public IFunction
{
public:
    FunctionTrim(const char * name_, bool trim_left_, bool trim_right_)
        : function_name(name_), trim_left(trim_left_), trim_right(trim_right_) {}

    static FunctionPtr create(ContextPtr, const char * name, bool trim_left, bool trim_right)
    {
        return std::make_shared<FunctionTrim>(name, trim_left, trim_right);
    }

    String getName() const override { return function_name; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"input_string", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}
        };

        FunctionArgumentDescriptors optional_args{
            {"trim_character", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"}
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        std::optional<TrimCharsTable> custom_trim_characters;
        if (arguments.size() == 2 && input_rows_count > 0)
        {
            String trim_characters_string;
            if (const ColumnString * col_trim_characters = checkAndGetColumn<ColumnString>(arguments[1].column.get()))
            {
                trim_characters_string = String(col_trim_characters->getDataAt(0));
            }
            else if (const ColumnConst * col_trim_characters_const = checkAndGetColumnConst<ColumnString>(arguments[1].column.get()))
            {
                trim_characters_string = String(col_trim_characters_const->getDataAt(0));
            }
            else
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected column type of argument 2 of function {}: {}", getName(), arguments[1].column->getName());
            }

            TrimCharsTable table{};
            for (char c : trim_characters_string)
                table[static_cast<UInt8>(c)] = true;
            custom_trim_characters = table;
        }

        ColumnPtr col_input_full = arguments[0].column->convertToFullColumnIfConst();

        auto col_res = ColumnString::create();
        if (const ColumnString * col_input_string = checkAndGetColumn<ColumnString>(col_input_full.get()))
        {
            if (custom_trim_characters)
            {
                dispatch([&](auto do_trim_left, auto do_trim_right)
                {
                    vectorCustom<do_trim_left, do_trim_right>(
                        col_input_string->getChars(), col_input_string->getOffsets(), *custom_trim_characters,
                        col_res->getChars(), col_res->getOffsets(), input_rows_count);
                });
            }
            else
            {
                dispatch([&](auto do_trim_left, auto do_trim_right)
                {
                    vectorSpace<do_trim_left, do_trim_right>(
                        col_input_string->getChars(), col_input_string->getOffsets(),
                        col_res->getChars(), col_res->getOffsets(), input_rows_count);
                });
            }
        }
        else if (const ColumnFixedString * col_input_fixed_string = checkAndGetColumn<ColumnFixedString>(col_input_full.get()))
        {
            dispatch([&](auto do_trim_left, auto do_trim_right)
            {
                vectorFixed<do_trim_left, do_trim_right>(
                    col_input_fixed_string->getChars(), col_input_fixed_string->getN(), custom_trim_characters,
                    col_res->getChars(), col_res->getOffsets(), input_rows_count);
            });
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
        }

        return col_res;
    }

private:
    const char * const function_name;
    const bool trim_left;
    const bool trim_right;

    /// Resolve the runtime trim_left/trim_right flags into compile-time template parameters.
    template <typename Func>
    void dispatch(Func && func) const
    {
        if (trim_left && trim_right)
            func(std::bool_constant<true>{}, std::bool_constant<true>{});
        else if (trim_left)
            func(std::bool_constant<true>{}, std::bool_constant<false>{});
        else
            func(std::bool_constant<false>{}, std::bool_constant<true>{});
    }

    /// Default trim strips ASCII spaces only. Consecutive rows that need no trimming form a
    /// run that is copied in a single memcpy; the run breaks at the first row that needs
    /// trimming, which is trimmed with a SIMD space scan. Output capacity is reserved once
    /// and the size is set at the end (no per-row reallocation).
    template <bool do_trim_left, bool do_trim_right>
    void vectorSpace(
        const ColumnString::Chars & input_data,
        const ColumnString::Offsets & input_offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count) const
    {
        res_offsets.resize_exact(input_rows_count);
        res_data.reserve_exact(input_data.size());

        const UInt8 * const input_begin = input_data.data();
        UInt8 * const res_begin = res_data.data();

        size_t res_offset = 0;
        size_t i = 0;
        while (i < input_rows_count)
        {
            /// i is the start of a run of untrimmed rows; j walks over them with a cheap
            /// boundary-byte check, copying their offsets verbatim (shifted by the bytes
            /// trimmed so far). The whole run is flushed in one memcpy when it ends.
            const size_t batch_input_begin = i == 0 ? 0 : input_offsets[i - 1];
            size_t row_begin = batch_input_begin;
            size_t j = i;
            while (j < input_rows_count)
            {
                const size_t row_end = input_offsets[j];
                const bool trim_this_left = do_trim_left && row_end > row_begin && input_begin[row_begin] == ' ';
                const bool trim_this_right = do_trim_right && row_end > row_begin && input_begin[row_end - 1] == ' ';
                if (trim_this_left || trim_this_right)
                    break;
                res_offsets[j] = res_offset + (row_end - batch_input_begin);
                row_begin = row_end;
                ++j;
            }

            if (j > i)
            {
                const size_t batch_size = row_begin - batch_input_begin;
                memcpy(res_begin + res_offset, input_begin + batch_input_begin, batch_size);
                res_offset += batch_size;
            }

            if (j == input_rows_count)
                break;

            /// row_begin is the start of row j, the row that needs trimming. Skip leading
            /// and trailing spaces with a SIMD scan, then copy what remains.
            const char * char_begin = reinterpret_cast<const char *>(input_begin + row_begin);
            const char * char_end = reinterpret_cast<const char *>(input_begin + input_offsets[j]);
            if (do_trim_left)
                char_begin = find_first_not_symbols<' '>(char_begin, char_end);
            if (do_trim_right)
            {
                const char * found = find_last_not_symbols_or_null<' '>(char_begin, char_end);
                char_end = found ? found + 1 : char_begin;
            }

            const size_t length = char_end - char_begin;
            memcpySmallAllowReadWriteOverflow15(res_begin + res_offset, reinterpret_cast<const UInt8 *>(char_begin), length);
            res_offset += length;
            res_offsets[j] = res_offset;

            i = j + 1;
        }

        res_data.resize_exact(res_offset);
    }

    /// Custom trim character set. Per-row scalar scan with O(1) table lookups. Output
    /// capacity is reserved once and the size is set at the end.
    template <bool do_trim_left, bool do_trim_right>
    void vectorCustom(
        const ColumnString::Chars & input_data,
        const ColumnString::Offsets & input_offsets,
        const TrimCharsTable & table,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count) const
    {
        res_offsets.resize_exact(input_rows_count);
        res_data.reserve_exact(input_data.size());

        const UInt8 * const input_begin = input_data.data();
        UInt8 * const res_begin = res_data.data();
        size_t prev_offset = 0;
        size_t res_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const UInt8 * begin = input_begin + prev_offset;
            const UInt8 * end = input_begin + input_offsets[i];

            if (do_trim_left)
            {
                while (begin < end && table[*begin])
                    ++begin;
            }
            if (do_trim_right)
            {
                while (end > begin && table[end[-1]])
                    --end;
            }

            const size_t length = end - begin;
            memcpySmallAllowReadWriteOverflow15(res_begin + res_offset, begin, length);
            res_offset += length;

            res_offsets[i] = res_offset;
            prev_offset = input_offsets[i];
        }

        res_data.resize_exact(res_offset);
    }

    template <bool do_trim_left, bool do_trim_right>
    void vectorFixed(
        const ColumnString::Chars & input_data,
        size_t n,
        const std::optional<TrimCharsTable> & custom_trim_characters,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count) const
    {
        res_offsets.resize_exact(input_rows_count);
        res_data.reserve_exact(input_data.size());

        const UInt8 * const input_begin = input_data.data();
        UInt8 * const res_begin = res_data.data();
        size_t prev_offset = 0;
        size_t res_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const UInt8 * begin = input_begin + prev_offset;
            const UInt8 * end = begin + n;

            if (custom_trim_characters)
            {
                const TrimCharsTable & table = *custom_trim_characters;
                if (do_trim_left)
                {
                    while (begin < end && table[*begin])
                        ++begin;
                }
                if (do_trim_right)
                {
                    while (end > begin && table[end[-1]])
                        --end;
                }
            }
            else
            {
                if (do_trim_left)
                {
                    while (begin < end && *begin == ' ')
                        ++begin;
                }
                if (do_trim_right)
                {
                    while (end > begin && end[-1] == ' ')
                        --end;
                }
            }

            const size_t length = end - begin;
            memcpySmallAllowReadWriteOverflow15(res_begin + res_offset, begin, length);
            res_offset += length;

            res_offsets[i] = res_offset;
            prev_offset += n;
        }

        res_data.resize_exact(res_offset);
    }
};

}

REGISTER_FUNCTION(Trim)
{
    FunctionDocumentation::Description description_left = R"(
Removes the specified characters from the start of a string.
By default, removes common whitespace (ASCII) characters.
)";
    FunctionDocumentation::Syntax syntax_left = "trimLeft(input[, trim_characters])";
    FunctionDocumentation::Arguments arguments_left = {
        {"input", "String to trim.", {"String"}},
        {"trim_characters", "Optional. Characters to trim. If not specified, common whitespace characters are removed.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_left = {"Returns the string with specified characters trimmed from the left.", {"String"}};
    FunctionDocumentation::Examples examples_left = {
    {
        "Usage example",
        "SELECT trimLeft('ClickHouse', 'Click');",
        R"(
┌─trimLeft('Cl⋯', 'Click')─┐
│ House                    │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation_left = {description_left, syntax_left, arguments_left, {}, returned_value_left, examples_left, introduced_in, category};

    FunctionDocumentation::Description description_right = R"(
Removes the specified characters from the end of a string.
By default, removes common whitespace (ASCII) characters.
)";
    FunctionDocumentation::Syntax syntax_right = "trimRight(s[, trim_characters])";
    FunctionDocumentation::Arguments arguments_right = {
        {"s", "String to trim.", {"String"}},
        {"trim_characters", "Optional characters to trim. If not specified, common whitespace characters are removed.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_right = {"Returns the string with specified characters trimmed from the right.", {"String"}};
    FunctionDocumentation::Examples examples_right = {
    {
        "Usage example",
        "SELECT trimRight('ClickHouse','House');",
        R"(
┌─trimRight('C⋯', 'House')─┐
│ Click                    │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_right = {description_right, syntax_right, arguments_right, {}, returned_value_right, examples_right, introduced_in, category};

    FunctionDocumentation::Description description_both = R"(
Removes the specified characters from the start and end of a string.
By default, removes common whitespace (ASCII) characters.
)";
    FunctionDocumentation::Syntax syntax_both = "trimBoth(s[, trim_characters])";
    FunctionDocumentation::Arguments arguments_both = {
        {"s", "String to trim.", {"String"}},
        {"trim_characters", "Optional. Characters to trim. If not specified, common whitespace characters are removed.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_both = {"Returns the string with specified characters trimmed from both ends.", {"String"}};
    FunctionDocumentation::Examples examples_both = {
    {
        "Usage example",
        "SELECT trimBoth('$$ClickHouse$$', '$')",
        R"(
┌─trimBoth('$$⋯se$$', '$')─┐
│ ClickHouse               │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_both = {description_both, syntax_both, arguments_both, {}, returned_value_both, examples_both, introduced_in, category};

    factory.registerFunction("trimLeft", [](ContextPtr){ return FunctionTrim::create({}, "trimLeft", true, false); }, documentation_left);
    factory.registerFunction("trimRight", [](ContextPtr){ return FunctionTrim::create({}, "trimRight", false, true); }, documentation_right);
    factory.registerFunction("trimBoth", [](ContextPtr){ return FunctionTrim::create({}, "trimBoth", true, true); }, documentation_both);
    factory.registerAlias("ltrim", "trimLeft");
    factory.registerAlias("rtrim", "trimRight");
    factory.registerAlias("trim", "trimBoth");
}
}
