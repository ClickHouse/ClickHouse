#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Parsers/ASTToJSON.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

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
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionParseQueryToJSON>(); }

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

            ParserQuery parser(sql.data() + sql.size());
            auto ast = parseQuery(parser, sql.data(), sql.data() + sql.size(), "", 0, 0, 0);

            result->insert(serializeASTToJSON(*ast));
        }

        return result;
    }
};

}


REGISTER_FUNCTION(ParseQueryToJSON)
{
    FunctionDocumentation::Description description = R"(
Parses a SQL query string into its AST (Abstract Syntax Tree) and returns a JSON representation of that tree.
The resulting JSON can be passed to `formatQueryFromJSON` to reconstruct the SQL query.
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
    FunctionDocumentation documentation = {description, syntax, func_arguments, {}, returned_value, examples, {}, category};

    factory.registerFunction<FunctionParseQueryToJSON>(documentation);
}

}
