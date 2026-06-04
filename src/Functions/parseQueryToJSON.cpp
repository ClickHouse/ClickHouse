#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTToJSON.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_ast_depth;
    extern const SettingsUInt64 max_ast_elements;
    extern const SettingsBool allow_settings_after_format_in_insert;
    extern const SettingsBool implicit_select;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// parseQueryToJSON(sql_string) -> JSON string representing the AST
class FunctionParseQueryToJSON : public IFunction
{
public:
    static constexpr auto name = "parseQueryToJSON";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionParseQueryToJSON>(context);
    }

    explicit FunctionParseQueryToJSON(ContextPtr context)
    {
        const Settings & settings = context->getSettingsRef();
        max_query_size = settings[Setting::max_query_size];
        max_parser_depth = settings[Setting::max_parser_depth];
        max_parser_backtracks = settings[Setting::max_parser_backtracks];
        max_ast_depth = settings[Setting::max_ast_depth];
        max_ast_elements = settings[Setting::max_ast_elements];
        allow_settings_after_format_in_insert = settings[Setting::allow_settings_after_format_in_insert];
        implicit_select = settings[Setting::implicit_select];
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of function {} must be String, got {}",
                getName(), arguments[0]->getName());
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto result = ColumnString::create();

        const auto * col = arguments[0].column.get();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto sql = col->getDataAt(i);

            ParserQuery parser(sql.data() + sql.size(), allow_settings_after_format_in_insert, implicit_select);
            auto ast = parseQuery(parser, sql.data(), sql.data() + sql.size(), "",
                max_query_size, max_parser_depth, max_parser_backtracks);

            /// Apply the same AST limits that core query execution enforces after parsing.
            if (max_ast_depth)
                ast->checkDepth(max_ast_depth);
            if (max_ast_elements)
                ast->checkSize(max_ast_elements);

            result->insert(serializeASTToJSON(*ast));
        }

        return result;
    }

private:
    size_t max_query_size;
    size_t max_parser_depth;
    size_t max_parser_backtracks;
    size_t max_ast_depth;
    size_t max_ast_elements;
    bool allow_settings_after_format_in_insert;
    bool implicit_select;
};

}


REGISTER_FUNCTION(ParseQueryToJSON)
{
    FunctionDocumentation::Description description = R"(
Parses a SQL query string into its AST (Abstract Syntax Tree) and returns a JSON representation of that tree.
The resulting JSON can be passed to `formatQueryFromJSON` to reconstruct the SQL query, or sent directly
to the server using the `clickhouse_json` value of the `dialect` setting (gated by `allow_experimental_json_ast_dialect`).

This is useful for tools that want to inspect or transform queries programmatically without going through
the SQL grammar.

Parsing limits (`max_query_size`, `max_parser_depth`, `max_parser_backtracks`) are taken from the current
session settings.
    )";
    FunctionDocumentation::Syntax syntax = "parseQueryToJSON(sql)";
    FunctionDocumentation::Arguments func_arguments = {
        {"sql", "A SQL query string to parse.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"A JSON string representing the AST.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Simple SELECT",
        R"(SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1'));)",
        R"(
┌─formatQueryFromJSON(parseQueryToJSON('SELECT 1'))─┐
│ SELECT 1                                          │
└───────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, func_arguments, {}, returned_value, examples, {26, 4}, category};

    factory.registerFunction<FunctionParseQueryToJSON>(documentation);
}

}
