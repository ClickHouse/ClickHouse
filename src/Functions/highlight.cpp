#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Common/FunctionDocumentation.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/HighlightImpl.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 highlight_max_matches_per_row;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

class FunctionHighlight : public IFunction
{
    const UInt64 max_matches_per_row;

public:
    static constexpr auto name = "highlight";

    explicit FunctionHighlight(ContextPtr context)
        : max_matches_per_row(context->getSettingsRef()[Setting::highlight_max_matches_per_row].value)
    {
    }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionHighlight>(context); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const size_t num_args = arguments.size();
        if (num_args != 2 && num_args != 4)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2 or 4 arguments, got {}",
                getName(), num_args);

        /// Argument 0: haystack — String or FixedString
        if (!isStringOrFixedString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected String or FixedString",
                arguments[0].type->getName(), getName());

        /// Argument 1: needles — const Array(String)
        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[1].type.get());
        if (!array_type || !isString(array_type->getNestedType()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected const Array(String)",
                arguments[1].type->getName(), getName());

        /// Arguments 2, 3: open_tag, close_tag — const String
        if (num_args == 4)
        {
            for (size_t i = 2; i <= 3; ++i)
            {
                if (!isString(arguments[i].type))
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument {} of function {}, expected const String",
                        arguments[i].type->getName(), i + 1, getName());
            }
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        /// Extract haystack column — handle both String and FixedString
        ColumnPtr col_haystack_converted;
        const ColumnString * col_haystack = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col_haystack)
        {
            if (const auto * col_fixed = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get()))
            {
                /// Convert FixedString to String by stripping trailing null-byte padding
                auto col_string = ColumnString::create();
                const size_t fixed_n = col_fixed->getN();
                const auto & fixed_chars = col_fixed->getChars();
                auto & str_chars = col_string->getChars();
                auto & str_offsets = col_string->getOffsets();

                str_offsets.resize(input_rows_count);
                str_chars.reserve(fixed_chars.size());

                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    const UInt8 * begin = &fixed_chars[i * fixed_n];
                    size_t len = fixed_n;
                    while (len > 0 && begin[len - 1] == 0)
                        --len;
                    str_chars.insert(begin, begin + len);
                    str_offsets[i] = str_chars.size();
                }

                col_haystack_converted = std::move(col_string);
                col_haystack = assert_cast<const ColumnString *>(col_haystack_converted.get());
            }
            else
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of first argument of function {}",
                    arguments[0].column->getName(), getName());
            }
        }

        /// Extract needles from const Array(String)
        const auto * col_needles_const = checkAndGetColumnConst<ColumnArray>(arguments[1].column.get());
        if (!col_needles_const)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Second argument of function {} must be a constant Array(String)",
                getName());

        const Array needles_array = col_needles_const->getValue<Array>();

        if (needles_array.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at least one needle in the needles array",
                getName());

        if (needles_array.size() > 255)
            throw Exception(
                ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                "Too many needles for function {} (max 255, got {})",
                getName(), needles_array.size());

        /// Build string_view vector pointing into the Array's Field storage
        std::vector<std::string_view> needles;
        needles.reserve(needles_array.size());
        for (const auto & element : needles_array)
            needles.emplace_back(element.safeGet<String>());

        /// Extract open_tag and close_tag
        String open_tag = "<em>";
        String close_tag = "</em>";

        if (arguments.size() == 4)
        {
            const auto * col_open = checkAndGetColumnConst<ColumnString>(arguments[2].column.get());
            const auto * col_close = checkAndGetColumnConst<ColumnString>(arguments[3].column.get());

            if (!col_open || !col_close)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Third and fourth arguments of function {} must be constant Strings",
                    getName());

            open_tag = col_open->getValue<String>();
            close_tag = col_close->getValue<String>();
        }

        /// Execute the highlight algorithm
        auto col_res = ColumnString::create();
        HighlightImpl::execute(
            col_haystack->getChars(),
            col_haystack->getOffsets(),
            needles,
            open_tag,
            close_tag,
            col_res->getChars(),
            col_res->getOffsets(),
            input_rows_count,
            max_matches_per_row);

        return col_res;
    }
};


REGISTER_FUNCTION(Highlight)
{
    FunctionDocumentation::Description description = R"(
Highlights occurrences of search terms in a text string by wrapping them with HTML tags.

The function performs ASCII case-insensitive matching. If multiple search terms overlap or are adjacent in the text, the matched regions are merged into a single highlighted span.
    )";
    FunctionDocumentation::Syntax syntax = "highlight(haystack, needles[, open_tag, close_tag])";
    FunctionDocumentation::Arguments func_arguments = {
        {"haystack", "The text to search in.", {"String", "FixedString"}},
        {"needles", "An array of search terms to highlight.", {"const Array(String)"}},
        {"open_tag", "The opening tag to insert before each match. Default: `<em>`.", {"const String"}},
        {"close_tag", "The closing tag to insert after each match. Default: `</em>`.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the input text with matched terms wrapped in the specified tags.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic highlight",
        "SELECT highlight('The quick brown fox', ['quick', 'fox'])",
        R"(
┌─highlight('The quick brown fox', ['quick', 'fox'])─┐
│ The <em>quick</em> brown <em>fox</em>              │
└────────────────────────────────────────────────────┘
        )"
    },
    {
        "Custom tags",
        "SELECT highlight('Hello World', ['hello'], '<b>', '</b>')",
        R"(
┌─highlight('Hello World', ['hello'], '<b>', '</b>')─┐
│ <b>Hello</b> World                                 │
└────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, func_arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHighlight>(documentation);
}

}
