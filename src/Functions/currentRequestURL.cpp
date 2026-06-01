#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>


namespace DB
{
namespace
{

/// Returns the HTTP request URL (path and query string) that invoked the query,
/// or an empty string if the query was not invoked through HTTP.
class FunctionCurrentRequestURL final : public IFunction
{
    const String request_url;

public:
    static constexpr auto name = "currentRequestURL";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionCurrentRequestURL>(context->getHTTPRequestURL());
    }

    explicit FunctionCurrentRequestURL(const String & request_url_) : request_url{request_url_}
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool allowsOmittingParentheses() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, request_url);
    }
};

}

REGISTER_FUNCTION(CurrentRequestURL)
{
    FunctionDocumentation::Description description = R"(
Returns the HTTP request URL (path and query string) that invoked the query.
Returns an empty string if the query was not invoked through HTTP.

Useful, in combination with SQL-defined HTTP handlers (`CREATE HANDLER`), to extract parameters
embedded in the request path.
    )";
    FunctionDocumentation::Syntax syntax = "currentRequestURL()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the current HTTP request URL.", {"String"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT currentRequestURL()", ""}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCurrentRequestURL>(documentation);
}

}
