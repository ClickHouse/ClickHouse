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
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionFormatQuery : public IFunction
{
public:
    static constexpr auto name = "formatQuery";
    static FunctionPtr create(ContextPtr context)
    {
        const auto & settings = context->getSettings();
        return std::make_shared<FunctionFormatQuery>(settings.max_query_size, settings.max_parser_depth);
    }

    FunctionFormatQuery(size_t max_query_size_, size_t max_parser_depth_)
        : max_query_size(max_query_size_), max_parser_depth(max_parser_depth_)
    {
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{{"query", &isString<IDataType>, nullptr, "String"}};
        validateFunctionArgumentTypes(*this, arguments, mandatory_args);
        return arguments[0].type;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr column = arguments[0].column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();
            formatVector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets());
            return col_res;
        }
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }

private:
    void formatQueryImpl(const char * begin, const char * end, ColumnString::Chars & output) const
    {
        ParserQuery parser{end};
        auto ast = parseQuery(parser, begin, end, {}, max_query_size, max_parser_depth);
        WriteBufferFromVector buf(output, AppendModeTag{});
        formatAST(*ast, buf, /* hilite */ false);
        buf.finalize();
    }
    void formatVector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets) const
    {
        const size_t size = offsets.size();
        res_offsets.resize(size);
        res_data.reserve(data.size());

        size_t prev_in_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            const auto * begin = reinterpret_cast<const char *>(&data[prev_in_offset]);
            const char * end = begin + offsets[i] - 1;
            formatQueryImpl(begin, end, res_data);
            res_offsets[i] = res_data.size() + 1;
            prev_in_offset = offsets[i];
        }
    }
    size_t max_query_size;
    size_t max_parser_depth;
};


REGISTER_FUNCTION(formatQuery)
{
    factory.registerFunction<FunctionFormatQuery>(FunctionDocumentation{
        .description = "Returns a formatted version of the given SQL query.\n[example:simple]\n[example:camelcase]",
        .syntax = "formatQuery(query)",
        .arguments = {{"query", "The SQL query to be formatted. [String](../../sql-reference/data-types/string.md)"}},
        .returned_value = "The formatted query. [String](../../sql-reference/data-types/string.md).",
        .examples{
            {"simple", "SELECT formatQuery('select 1;')", "SELECT 1"},
            {"camelcase", "SELECT formatQuery('SeLecT 1')", "SELECT 1"}},
        .categories{"String"}});
}
}
