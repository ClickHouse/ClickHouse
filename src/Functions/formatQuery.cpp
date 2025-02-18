#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsBool print_pretty_type_names;
    extern const SettingsBool implicit_select;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

enum class OutputFormatting : uint8_t
{
    SingleLine,
    MultiLine
};

enum class ErrorHandling : uint8_t
{
    Exception,
    Null
};

class FunctionFormatQuery : public IFunction
{
public:
    FunctionFormatQuery(ContextPtr context, String name_, OutputFormatting output_formatting_, ErrorHandling error_handling_)
        : name(name_), output_formatting(output_formatting_), error_handling(error_handling_)
    {
        const Settings & settings = context->getSettingsRef();
        max_query_size = settings[Setting::max_query_size];
        max_parser_depth = settings[Setting::max_parser_depth];
        max_parser_backtracks = settings[Setting::max_parser_backtracks];
        print_pretty_type_names = settings[Setting::print_pretty_type_names];
        implicit_select = settings[Setting::implicit_select];
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"query", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
        };
        validateFunctionArguments(*this, arguments, args);

        DataTypePtr string_type = std::make_shared<DataTypeString>();
        if (error_handling == ErrorHandling::Null)
            return std::make_shared<DataTypeNullable>(string_type);
        return string_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr col_query = arguments[0].column;

        ColumnUInt8::MutablePtr col_null_map;
        if (error_handling == ErrorHandling::Null)
            col_null_map = ColumnUInt8::create(input_rows_count, 0);

        if (const ColumnString * col_query_string = checkAndGetColumn<ColumnString>(col_query.get()))
        {
            auto col_res = ColumnString::create();
            formatVector(col_query_string->getChars(), col_query_string->getOffsets(), col_res->getChars(), col_res->getOffsets(), col_null_map, input_rows_count);

            if (error_handling == ErrorHandling::Null)
                return ColumnNullable::create(std::move(col_res), std::move(col_null_map));
            return col_res;
        }
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", col_query->getName(), getName());
    }

private:
    void formatVector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        ColumnUInt8::MutablePtr & res_null_map,
        size_t input_rows_count) const
    {
        res_offsets.resize(input_rows_count);
        res_data.resize(data.size());

        size_t prev_offset = 0;
        size_t res_data_size = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const char * begin = reinterpret_cast<const char *>(&data[prev_offset]);
            const char * end = begin + offsets[i] - prev_offset - 1;

            ParserQuery parser(end, false, implicit_select);
            ASTPtr ast;
            WriteBufferFromOwnString buf;

            try
            {
                ast = parseQuery(parser, begin, end, /*query_description*/ {}, max_query_size, max_parser_depth, max_parser_backtracks);
            }
            catch (...)
            {
                if (error_handling == ErrorHandling::Null)
                {
                    const size_t res_data_new_size = res_data_size + 1;
                    if (res_data_new_size > res_data.size())
                        res_data.resize(2 * res_data_new_size);

                    res_data[res_data_size] = '\0';
                    res_data_size += 1;

                    res_offsets[i] = res_data_size;
                    prev_offset = offsets[i];

                    res_null_map->getData()[i] = 1;

                    continue;
                }

                throw;
            }

            IAST::FormatSettings settings(output_formatting == OutputFormatting::SingleLine, /*hilite*/ false);
            settings.show_secrets = true;
            settings.print_pretty_type_names = print_pretty_type_names;
            ast->format(buf, settings);

            auto formatted = buf.stringView();

            const size_t res_data_new_size = res_data_size + formatted.size() + 1;
            if (res_data_new_size > res_data.size())
                res_data.resize(2 * res_data_new_size);

            memcpy(&res_data[res_data_size], formatted.begin(), formatted.size());
            res_data_size += formatted.size();

            res_data[res_data_size] = '\0';
            res_data_size += 1;

            res_offsets[i] = res_data_size;
            prev_offset = offsets[i];
        }

        res_data.resize(res_data_size);
    }

    String name;
    OutputFormatting output_formatting;
    ErrorHandling error_handling;

    size_t max_query_size;
    size_t max_parser_depth;
    size_t max_parser_backtracks;
    bool print_pretty_type_names;
    bool implicit_select;
};

}

REGISTER_FUNCTION(formatQuery)
{
    factory.registerFunction(
        "formatQuery",
        [](ContextPtr context) { return std::make_shared<FunctionFormatQuery>(context, "formatQuery", OutputFormatting::MultiLine, ErrorHandling::Exception); },
        FunctionDocumentation{
            .description = "Returns a formatted, possibly multi-line, version of the given SQL query. Throws in case of a parsing error.\n[example:multiline]",
            .syntax = "formatQuery(query)",
            .arguments = {{"query", "The SQL query to be formatted. [String](../../sql-reference/data-types/string.md)"}},
            .returned_value = "The formatted query. [String](../../sql-reference/data-types/string.md).",
            .examples{
                {"multiline",
                 "SELECT formatQuery('select a,    b FRom tab WHERE a > 3 and  b < 3');",
                 "SELECT\n"
                 "    a,\n"
                 "    b\n"
                 "FROM tab\n"
                 "WHERE (a > 3) AND (b < 3)"}},
            .categories{"Other"}});
}

REGISTER_FUNCTION(formatQueryOrNull)
{
    factory.registerFunction(
        "formatQueryOrNull",
        [](ContextPtr context) { return std::make_shared<FunctionFormatQuery>(context, "formatQueryOrNull", OutputFormatting::MultiLine, ErrorHandling::Null); },
        FunctionDocumentation{
            .description = "Returns a formatted, possibly multi-line, version of the given SQL query. Returns NULL in case of a parsing error.\n[example:multiline]",
            .syntax = "formatQueryOrNull(query)",
            .arguments = {{"query", "The SQL query to be formatted. [String](../../sql-reference/data-types/string.md)"}},
            .returned_value = "The formatted query. [String](../../sql-reference/data-types/string.md).",
            .examples{
                {"multiline",
                 "SELECT formatQuery('select a,    b FRom tab WHERE a > 3 and  b < 3');",
                 "SELECT\n"
                 "    a,\n"
                 "    b\n"
                 "FROM tab\n"
                 "WHERE (a > 3) AND (b < 3)"}},
            .categories{"Other"}});
}

REGISTER_FUNCTION(formatQuerySingleLine)
{
    factory.registerFunction(
        "formatQuerySingleLine",
        [](ContextPtr context) { return std::make_shared<FunctionFormatQuery>(context, "formatQuerySingleLine", OutputFormatting::SingleLine, ErrorHandling::Exception); },
        FunctionDocumentation{
            .description = "Like formatQuery() but the returned formatted string contains no line breaks. Throws in case of a parsing error.\n[example:multiline]",
            .syntax = "formatQuerySingleLine(query)",
            .arguments = {{"query", "The SQL query to be formatted. [String](../../sql-reference/data-types/string.md)"}},
            .returned_value = "The formatted query. [String](../../sql-reference/data-types/string.md).",
            .examples{
                {"multiline",
                 "SELECT formatQuerySingleLine('select a,    b FRom tab WHERE a > 3 and  b < 3');",
                 "SELECT a, b FROM tab WHERE (a > 3) AND (b < 3)"}},
            .categories{"Other"}});
}

REGISTER_FUNCTION(formatQuerySingleLineOrNull)
{
    factory.registerFunction(
        "formatQuerySingleLineOrNull",
        [](ContextPtr context) { return std::make_shared<FunctionFormatQuery>(context, "formatQuerySingleLineOrNull", OutputFormatting::SingleLine, ErrorHandling::Null); },
        FunctionDocumentation{
            .description = "Like formatQuery() but the returned formatted string contains no line breaks. Returns NULL in case of a parsing error.\n[example:multiline]",
            .syntax = "formatQuerySingleLineOrNull(query)",
            .arguments = {{"query", "The SQL query to be formatted. [String](../../sql-reference/data-types/string.md)"}},
            .returned_value = "The formatted query. [String](../../sql-reference/data-types/string.md).",
            .examples{
                {"multiline",
                 "SELECT formatQuerySingleLine('select a,    b FRom tab WHERE a > 3 and  b < 3');",
                 "SELECT a, b FROM tab WHERE (a > 3) AND (b < 3)"}},
            .categories{"Other"}});
}

}
