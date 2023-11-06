#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
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

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

enum class OutputFormatting
{
    SingleLine,
    MultiLine
};

enum class ErrorHandling
{
    Exception,
    Null
};

template <OutputFormatting output_formatting, ErrorHandling error_handling, typename Name>
class FunctionFormatQuery : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr context)
    {
        const auto & settings = context->getSettings();
        return std::make_shared<FunctionFormatQuery>(settings.max_query_size, settings.max_parser_depth);
    }

    FunctionFormatQuery(size_t max_query_size_, size_t max_parser_depth_)
        : max_query_size(max_query_size_)
        , max_parser_depth(max_parser_depth_)
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"query", &isString<IDataType>, nullptr, "String"}
        };
        validateFunctionArgumentTypes(*this, arguments, args);

        DataTypePtr string_type = std::make_shared<DataTypeString>();
        if constexpr (error_handling == ErrorHandling::Null)
            return std::make_shared<DataTypeNullable>(string_type);
        else
            return string_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr col_query = arguments[0].column;

        ColumnUInt8::MutablePtr col_null_map;
        if constexpr (error_handling == ErrorHandling::Null)
            col_null_map = ColumnUInt8::create(input_rows_count, 0);

        if (const ColumnString * col_query_string = checkAndGetColumn<ColumnString>(col_query.get()))
        {
            auto col_res = ColumnString::create();
            formatVector(col_query_string->getChars(), col_query_string->getOffsets(), col_res->getChars(), col_res->getOffsets(), col_null_map);

            if constexpr (error_handling == ErrorHandling::Null)
                return ColumnNullable::create(std::move(col_res), std::move(col_null_map));
            else
                return col_res;
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", col_query->getName(), getName());
    }

private:
    void formatVector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        ColumnUInt8::MutablePtr & res_null_map) const
    {
        const size_t size = offsets.size();
        res_offsets.resize(size);
        res_data.resize(data.size());

        size_t prev_offset = 0;
        size_t res_data_size = 0;

        for (size_t i = 0; i < size; ++i)
        {
            const char * begin = reinterpret_cast<const char *>(&data[prev_offset]);
            const char * end = begin + offsets[i] - prev_offset - 1;

            ParserQuery parser(end);
            ASTPtr ast;
            WriteBufferFromOwnString buf;

            try
            {
                ast = parseQuery(parser, begin, end, /*query_description*/ {}, max_query_size, max_parser_depth);
            }
            catch (...)
            {
                if constexpr (error_handling == ErrorHandling::Null)
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
                else
                {
                    static_assert(error_handling == ErrorHandling::Exception);
                    throw;
                }
            }

            formatAST(*ast, buf, /*hilite*/ false, /*single_line*/ output_formatting == OutputFormatting::SingleLine);
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

    const size_t max_query_size;
    const size_t max_parser_depth;
};

struct NameFormatQuery
{
    static constexpr auto name = "formatQuery";
};

struct NameFormatQueryOrNull
{
    static constexpr auto name = "formatQueryOrNull";
};

struct NameFormatQuerySingleLine
{
    static constexpr auto name = "formatQuerySingleLine";
};

struct NameFormatQuerySingleLineOrNull
{
    static constexpr auto name = "formatQuerySingleLineOrNull";
};

REGISTER_FUNCTION(formatQuery)
{
    factory.registerFunction<FunctionFormatQuery<OutputFormatting::MultiLine, ErrorHandling::Exception, NameFormatQuery>>(FunctionDocumentation{
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
    factory.registerFunction<FunctionFormatQuery<OutputFormatting::MultiLine, ErrorHandling::Null, NameFormatQueryOrNull>>(FunctionDocumentation{
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
    factory.registerFunction<FunctionFormatQuery<OutputFormatting::SingleLine, ErrorHandling::Exception, NameFormatQuerySingleLine>>(FunctionDocumentation{
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
    factory.registerFunction<FunctionFormatQuery<OutputFormatting::SingleLine, ErrorHandling::Null, NameFormatQuerySingleLineOrNull>>(FunctionDocumentation{
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
