#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteBufferFromVector.h>
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

template <OutputFormatting output_formatting, typename Name>
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

        return arguments[0].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr col_query = arguments[0].column;
        if (const ColumnString * col_query_string = checkAndGetColumn<ColumnString>(col_query.get()))
        {
            auto col_res = ColumnString::create();
            formatVector(col_query_string->getChars(), col_query_string->getOffsets(), col_res->getChars(), col_res->getOffsets());
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
        ColumnString::Offsets & res_offsets) const
    {
        const size_t size = offsets.size();
        res_offsets.resize(size);
        res_data.reserve(data.size());

        size_t prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            const char * begin = reinterpret_cast<const char *>(&data[prev_offset]);
            const char * end = begin + offsets[i] - 1;

            ParserQuery parser(end);
            auto ast = parseQuery(parser, begin, end, /*query_description*/ {}, max_query_size, max_parser_depth);
            WriteBufferFromVector buf(res_data, AppendModeTag{});
            formatAST(*ast, buf, /*hilite*/ false, /*single_line*/ output_formatting == OutputFormatting::SingleLine);
            buf.finalize();

            res_offsets[i] = res_data.size() + 1;
            prev_offset = offsets[i];
        }
    }

    const size_t max_query_size;
    const size_t max_parser_depth;
};

struct NameFormatQuery
{
    static constexpr auto name = "formatQuery";
};

struct NameFormatQuerySingleLine
{
    static constexpr auto name = "formatQuerySingleLine";
};

REGISTER_FUNCTION(formatQuery)
{
    factory.registerFunction<FunctionFormatQuery<OutputFormatting::MultiLine, NameFormatQuery>>(FunctionDocumentation{
        .description = "Returns a formatted, possibly multi-line, version of the given SQL query.\n[example:multiline]",
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

REGISTER_FUNCTION(formatQuerySingleLine)
{
    factory.registerFunction<FunctionFormatQuery<OutputFormatting::SingleLine, NameFormatQuerySingleLine>>(FunctionDocumentation{
        .description = "Like formatQuery() but the returned formatted string contains no line breaks.\n[example:multiline]",
        .syntax = "formatQuerySingleLine(query)",
        .arguments = {{"query", "The SQL query to be formatted. [String](../../sql-reference/data-types/string.md)"}},
        .returned_value = "The formatted query. [String](../../sql-reference/data-types/string.md).",
        .examples{
            {"multiline",
             "SELECT formatQuerySingleLine('select a,    b FRom tab WHERE a > 3 and  b < 3');",
             "SELECT a, b FROM tab WHERE (a > 3) AND (b < 3)"}},
        .categories{"Other"}});
}
}
