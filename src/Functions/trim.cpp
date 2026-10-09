#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/IColumn.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <base/find_symbols.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionTrim : public IFunction
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
        std::optional<SearchSymbols> custom_trim_characters;
        if (arguments.size() == 2 && input_rows_count > 0)
        {
            if (const ColumnString * col_trim_characters = checkAndGetColumn<ColumnString>(arguments[1].column.get()))
            {
                const String trim_characters_string{col_trim_characters->getDataAt(0)};
                custom_trim_characters = std::make_optional<SearchSymbols>(trim_characters_string);
            }
            else if (const ColumnConst * col_trim_characters_const = checkAndGetColumnConst<ColumnString>(arguments[1].column.get()))
            {
                const String trim_characters_string{col_trim_characters_const->getDataAt(0)};
                custom_trim_characters = std::make_optional<SearchSymbols>(trim_characters_string);
            }
        }

        ColumnPtr col_input_full;
        col_input_full = arguments[0].column->convertToFullColumnIfConst();

        auto col_res = ColumnString::create();
        if (const ColumnString * col_input_string = checkAndGetColumn<ColumnString>(col_input_full.get()))
        {
            vector(
                col_input_string->getChars(), col_input_string->getOffsets(),
                custom_trim_characters,
                col_res->getChars(), col_res->getOffsets(),
                input_rows_count);
        }
        else if (const ColumnFixedString * col_input_fixed_string = checkAndGetColumn<ColumnFixedString>(col_input_full.get()))
        {
            vectorFixed(
                col_input_fixed_string->getChars(), col_input_fixed_string->getN(),
                custom_trim_characters,
                col_res->getChars(),
                col_res->getOffsets(),
                input_rows_count);
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

    template <bool do_trim_left, bool do_trim_right>
    void vectorImpl(
        const ColumnString::Chars & input_data,
        const ColumnString::Offsets & input_offsets,
        const std::optional<SearchSymbols> & custom_trim_characters,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count) const
    {
        res_offsets.resize_exact(input_rows_count);
        res_data.reserve_exact(input_data.size());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        const UInt8 * start;
        size_t length;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            executeImpl<do_trim_left, do_trim_right>(reinterpret_cast<const UInt8 *>(&input_data[prev_offset]), input_offsets[i] - prev_offset, custom_trim_characters, start, length);

            res_data.resize(res_data.size() + length);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], start, length);
            res_offset += length;

            res_offsets[i] = res_offset;
            prev_offset = input_offsets[i];
        }
    }

    void vector(
        const ColumnString::Chars & input_data,
        const ColumnString::Offsets & input_offsets,
        const std::optional<SearchSymbols> & custom_trim_characters,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count) const
    {
        dispatch([&](auto trim_left_v, auto trim_right_v)
        {
            vectorImpl<trim_left_v, trim_right_v>(input_data, input_offsets, custom_trim_characters, res_data, res_offsets, input_rows_count);
        });
    }

    template <bool do_trim_left, bool do_trim_right>
    void vectorFixedImpl(
        const ColumnString::Chars & input_data,
        size_t n,
        const std::optional<SearchSymbols> & custom_trim_characters,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count) const
    {
        res_offsets.resize_exact(input_rows_count);
        res_data.reserve_exact(input_data.size());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        const UInt8 * start;
        size_t length;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            executeImpl<do_trim_left, do_trim_right>(reinterpret_cast<const UInt8 *>(&input_data[prev_offset]), n, custom_trim_characters, start, length);

            res_data.resize(res_data.size() + length);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], start, length);
            res_offset += length;

            res_offsets[i] = res_offset;
            prev_offset += n;
        }
    }

    void vectorFixed(
        const ColumnString::Chars & input_data,
        size_t n,
        const std::optional<SearchSymbols> & custom_trim_characters,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count) const
    {
        dispatch([&](auto trim_left_v, auto trim_right_v)
        {
            vectorFixedImpl<trim_left_v, trim_right_v>(input_data, n, custom_trim_characters, res_data, res_offsets, input_rows_count);
        });
    }

    template <bool do_trim_left, bool do_trim_right>
    static void executeImpl(const UInt8 * data, size_t size, const std::optional<SearchSymbols> & custom_trim_characters, const UInt8 *& res_data, size_t & res_size)
    {
        const char * char_begin = reinterpret_cast<const char *>(data);
        const char * char_end = char_begin + size;

        if constexpr (do_trim_left)
        {
            const char * found = nullptr;
            if (!custom_trim_characters)
                found = find_first_not_symbols<' '>(char_begin, char_end);
            else
            {
                std::string_view input(char_begin, char_end);
                found = find_first_not_symbols(input, *custom_trim_characters);
            }
            size_t num_chars = found - char_begin;
            char_begin += num_chars;
        }
        if constexpr (do_trim_right)
        {
            const char * found = nullptr;
            if (!custom_trim_characters)
                found = find_last_not_symbols_or_null<' '>(char_begin, char_end);
            else
            {
                std::string_view input(char_begin, char_end);
                found = find_last_not_symbols_or_null(input, *custom_trim_characters);
            }
            if (found)
                char_end = found + 1;
            else
                char_end = char_begin;
        }

        res_data = reinterpret_cast<const UInt8 *>(char_begin);
        res_size = char_end - char_begin;
    }

    /// Dispatch runtime trim_left/trim_right to compile-time template parameters
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
