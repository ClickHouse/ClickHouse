#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTFromJSON.h>
#include <Parsers/IAST.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// formatQueryFromJSON(json_string) -> SQL query string
class FunctionFormatQueryFromJSON : public IFunction
{
public:
    static constexpr auto name = "formatQueryFromJSON";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFormatQueryFromJSON>(); }

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
            auto json = String(col->getDataAt(i));
            auto ast = deserializeASTFromJSON(json);

            WriteBufferFromOwnString buf;
            IAST::FormatSettings format_settings(/*one_line=*/true);
            ast->format(buf, format_settings);
            result->insert(buf.str());
        }

        return result;
    }
};

} /// namespace


REGISTER_FUNCTION(FormatQueryFromJSON)
{
    FunctionDocumentation::Description description = R"(
Takes a JSON representation of a SQL AST (as produced by `parseQueryToJSON`) and formats it back into a SQL query string.
Together, `parseQueryToJSON` and `formatQueryFromJSON` provide a round-trip conversion between SQL and its JSON AST representation.
    )";
    FunctionDocumentation::Syntax syntax = "formatQueryFromJSON(json)";
    FunctionDocumentation::Arguments func_arguments = {
        {"json", "A JSON string representing a SQL AST.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"A SQL query string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Round-trip",
        R"(SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a, b FROM t WHERE x > 1'));)",
        R"(
┌─formatQueryFromJSON(parseQueryToJSON('SELECT a, b FROM t WHERE x > 1'))─┐
│ SELECT a, b FROM t WHERE x > 1                                         │
└─────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, func_arguments, {}, returned_value, examples, {}, category};

    factory.registerFunction<FunctionFormatQueryFromJSON>(documentation);
}

}
