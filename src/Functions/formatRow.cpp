#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Processors/Port.h>

#include <memory>
#include <ranges>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/** formatRow(<format>, x, y, ...) is a function that allows you to use RowOutputFormat over
  * several columns to generate a string per row, such as CSV, TSV, JSONEachRow, etc.
  * formatRowNoNewline(...) trims the newline character of each row.
  */
template <bool no_newline>
class FunctionFormatRow : public IFunction
{
public:
    static constexpr auto name = no_newline ? "formatRowNoNewline" : "formatRow";

    FunctionFormatRow(String format_name_, Names arguments_column_names_, ContextPtr context_)
        : format_name(std::move(format_name_))
        , arguments_column_names(std::move(arguments_column_names_))
        , context(std::move(context_))
        , format_settings(getFormatSettings(context))
    {
        FormatFactory::instance().checkFormatName(format_name);

        /// We don't need handling exceptions while formatting as a row.
        /// But it can be enabled in query sent via http interface.
        format_settings.json.valid_output_on_exception = false;
        format_settings.xml.valid_output_on_exception = false;
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_str = ColumnString::create();
        ColumnString::Chars & vec = col_str->getChars();
        WriteBufferFromVector<ColumnString::Chars> buffer(vec);
        ColumnString::Offsets & offsets = col_str->getOffsets();
        offsets.resize(input_rows_count);

        Block arg_columns;

        size_t arguments_size = arguments.size();
        for (size_t i = 1; i < arguments_size; ++i)
        {
            auto argument_column = arguments[i];
            argument_column.name = arguments_column_names[i];
            arg_columns.insert(std::move(argument_column));
        }

        materializeBlockInplace(arg_columns);
        auto out = FormatFactory::instance().getOutputFormat(format_name, buffer, arg_columns, context, format_settings);

        /// This function make sense only for row output formats.
        auto * row_output_format = dynamic_cast<IRowOutputFormat *>(out.get());
        if (!row_output_format)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Cannot turn rows into a {} format strings. {} function supports only row output formats",
                            format_name, getName());

        auto columns = arg_columns.getColumns();
        for (size_t i = 0; i != input_rows_count; ++i)
        {
            row_output_format->writePrefixIfNeeded();
            row_output_format->writeRow(columns, i);
            row_output_format->finalize();
            if constexpr (no_newline)
            {
                if (buffer.position() != buffer.buffer().begin() && buffer.position()[-1] == '\n')
                    --buffer.position();
            }

            offsets[i] = buffer.count();
            row_output_format->resetFormatter();
        }

        return col_str;
    }

private:
    String format_name;
    Names arguments_column_names;
    ContextPtr context;
    FormatSettings format_settings;
};

template <bool no_newline>
class FormatRowOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = no_newline ? "formatRowNoNewline" : "formatRow";
    static FunctionOverloadResolverPtr create(ContextPtr context) { return std::make_unique<FormatRowOverloadResolver>(context); }
    explicit FormatRowOverloadResolver(ContextPtr context_) : context(context_) { }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    bool useDefaultImplementationForNulls() const override { return false; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at least two arguments: the format name and its output expression(s)", getName());

        Names arguments_column_names;
        arguments_column_names.reserve(arguments.size());
        for (const auto & argument : arguments)
            arguments_column_names.push_back(argument.name);

        if (const auto * name_col = checkAndGetColumnConst<ColumnString>(arguments.at(0).column.get()))
            return std::make_unique<FunctionToFunctionBaseAdaptor>(
                std::make_shared<FunctionFormatRow<no_newline>>(name_col->getValue<String>(), std::move(arguments_column_names), context),
                DataTypes{std::from_range_t{}, arguments | std::views::transform([](auto & elem) { return elem.type; })},
                return_type);
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument to {} must be a format name", getName());
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeString>(); }

private:
    ContextPtr context;
};

}

REGISTER_FUNCTION(FormatRow)
{
    /// formatRow documentation
    FunctionDocumentation::Description formatRow_description = R"(
Converts arbitrary expressions into a string via given format.

:::note
If the format contains a suffix/prefix, it will be written in each row.
Only row-based formats are supported in this function.
:::
    )";
    FunctionDocumentation::Syntax formatRow_syntax = "formatRow(format, x, y, ...)";
    FunctionDocumentation::Arguments formatRow_arguments =
    {
        {"format", "Text format. For example, CSV, TSV.", {"String"}},
        {"x, y, ...", "Expressions.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue formatRow_returned_value = {"A formatted string. (for text formats it's usually terminated with the new line character).", {"String"}};
    FunctionDocumentation::Examples formatRow_examples =
    {
    {
        "Basic usage",
        R"(
SELECT formatRow('CSV', number, 'good')
FROM numbers(3)
        )",
        R"(
┌─formatRow('CSV', number, 'good')─┐
│ 0,"good"
                         │
│ 1,"good"
                         │
│ 2,"good"
                         │
└──────────────────────────────────┘
        )"
    },
    {
        "With custom format",
        R"(
SELECT formatRow('CustomSeparated', number, 'good')
FROM numbers(3)
SETTINGS format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>'
        )",
        R"(
┌─formatRow('CustomSeparated', number, 'good')─┐
│ <prefix>
0    good
<suffix>                   │
│ <prefix>
1    good
<suffix>                   │
│ <prefix>
2    good
<suffix>                   │
└──────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn formatRow_introduced_in = {20, 7};
    FunctionDocumentation::Category formatRow_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation formatRow_documentation = {formatRow_description, formatRow_syntax, formatRow_arguments, formatRow_returned_value, formatRow_examples, formatRow_introduced_in, formatRow_category};

    /// formatRowNoNewline documentation
    FunctionDocumentation::Description formatRowNoNewline_description = R"(
Same as [`formatRow`](#formatRow), but trims the newline character of each row.

Converts arbitrary expressions into a string via given format, but removes any trailing newline characters from the result.
    )";
    FunctionDocumentation::Syntax formatRowNoNewline_syntax = "formatRowNoNewline(format, x, y, ...)";
    FunctionDocumentation::Arguments formatRowNoNewline_arguments =
    {
        {"format", "Text format. For example, CSV, TSV.", {"String"}},
        {"x, y, ...", "Expressions.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue formatRowNoNewline_returned_value = {"Returns a formatted string with newlines removed.", {"String"}};
    FunctionDocumentation::Examples formatRowNoNewline_examples =
    {
    {
        "Basic usage",
        R"(
SELECT formatRowNoNewline('CSV', number, 'good')
FROM numbers(3)
        )",
        R"(
┌─formatRowNoNewline('CSV', number, 'good')─┐
│ 0,"good"                                  │
│ 1,"good"                                  │
│ 2,"good"                                  │
└───────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn formatRowNoNewline_introduced_in = {20, 7};
    FunctionDocumentation::Category formatRowNoNewline_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation formatRowNoNewline_documentation = {formatRowNoNewline_description, formatRowNoNewline_syntax, formatRowNoNewline_arguments, formatRowNoNewline_returned_value, formatRowNoNewline_examples, formatRowNoNewline_introduced_in, formatRowNoNewline_category};

    factory.registerFunction<FormatRowOverloadResolver<false>>(formatRow_documentation);
    factory.registerFunction<FormatRowOverloadResolver<true>>(formatRowNoNewline_documentation);
}

}
