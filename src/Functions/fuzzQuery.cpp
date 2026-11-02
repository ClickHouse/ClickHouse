#include <Columns/ColumnString.h>
#include <Common/QueryFuzzer.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_fuzz_query_functions;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsBool implicit_select;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

class FunctionFuzzQuery : public IFunction
{
public:
    static constexpr auto name = "fuzzQuery";

    explicit FunctionFuzzQuery(ContextPtr context)
    {
        if (!context->getSettingsRef()[Setting::allow_fuzz_query_functions])
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Function `fuzzQuery` is disabled. Set `allow_fuzz_query_functions` to 1 to enable it");

        const Settings & settings = context->getSettingsRef();
        max_query_size = settings[Setting::max_query_size];
        max_parser_depth = settings[Setting::max_parser_depth];
        max_parser_backtracks = settings[Setting::max_parser_backtracks];
        implicit_select = settings[Setting::implicit_select];
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"query", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
        };
        validateFunctionArguments(*this, arguments, args);
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr col_query = arguments[0].column;

        if (const ColumnString * col_query_string = checkAndGetColumn<ColumnString>(col_query.get()))
        {
            auto col_res = ColumnString::create();
            fuzzVector(
                col_query_string->getChars(), col_query_string->getOffsets(),
                *col_res,
                input_rows_count);
            return col_res;
        }

        if (const ColumnConst * col_query_const = checkAndGetColumnConstStringOrFixedString(col_query.get()))
        {
            auto col_res = ColumnString::create();

            const ColumnString::Chars & data = assert_cast<const ColumnString &>(col_query_const->getDataColumn()).getChars();
            const char * begin = reinterpret_cast<const char *>(data.data());
            const char * end = begin + data.size();
            ParserQuery parser(end, false, implicit_select);
            ASTPtr ast = parseQuery(parser, begin, end, "fuzzQuery", max_query_size, max_parser_depth, max_parser_backtracks);

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                ASTPtr fuzzed_ast = ast->clone();
                {
                    auto [fuzzer, lock] = getGlobalASTFuzzer();
                    fuzzer->fuzzMain(fuzzed_ast);
                }

                WriteBufferFromOwnString buf;
                fuzzed_ast->format(buf, IAST::FormatSettings(/*one_line=*/true));
                col_res->insertData(buf.str().data(), buf.str().size());
            }

            return col_res;
        }

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", col_query->getName(), getName());
    }

private:
    void fuzzVector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString & col_res,
        size_t input_rows_count) const
    {
        size_t prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const char * begin = reinterpret_cast<const char *>(&data[prev_offset]);
            const char * end = reinterpret_cast<const char *>(&data[offsets[i]]);

            ParserQuery parser(end, false, implicit_select);
            ASTPtr ast = parseQuery(parser, begin, end, "fuzzQuery", max_query_size, max_parser_depth, max_parser_backtracks);

            ASTPtr fuzzed_ast;
            {
                auto [fuzzer, lock] = getGlobalASTFuzzer();
                fuzzer->fuzzMain(ast);
            }

            WriteBufferFromOwnString buf;
            ast->format(buf, IAST::FormatSettings(/*one_line=*/true));
            col_res.insertData(buf.str().data(), buf.str().size());
            prev_offset = offsets[i];
        }
    }

    size_t max_query_size;
    size_t max_parser_depth;
    size_t max_parser_backtracks;
    bool implicit_select;
};

}

REGISTER_FUNCTION(fuzzQuery)
{
    factory.registerFunction(
        "fuzzQuery",
        [](ContextPtr context) { return std::make_shared<FunctionFuzzQuery>(context); },
        FunctionDocumentation{
            .description = "Parses the given query string and applies random AST mutations (fuzzing) to it. Returns the fuzzed query as a string. "
                           "Non-deterministic: each call may produce a different result. "
                           "Requires `allow_fuzz_query_functions = 1`.",
            .syntax = "fuzzQuery(query)",
            .arguments = {{"query", "The SQL query to be fuzzed. [String](../../sql-reference/data-types/string.md)"}},
            .returned_value = {"The fuzzed query string", {"String"}},
            .examples{
                {"basic",
                 "SET allow_fuzz_query_functions = 1; SELECT fuzzQuery('SELECT 1');",
                 ""}},
            .introduced_in = {26, 2},
            .category = FunctionDocumentation::Category::Other});
}

}
