#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>


namespace DB
{
namespace
{

/// Returns the name of the SQL-defined HTTP handler (CREATE HANDLER) that invoked the query,
/// or an empty string if the query was not invoked through such a handler.
class FunctionCurrentHandler final : public IFunction
{
    const String handler_name;

public:
    static constexpr auto name = "currentHandler";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionCurrentHandler>(context->getHTTPHandlerName());
    }

    explicit FunctionCurrentHandler(const String & handler_name_) : handler_name{handler_name_}
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
        return DataTypeString().createColumnConst(input_rows_count, handler_name);
    }
};

}

REGISTER_FUNCTION(CurrentHandler)
{
    FunctionDocumentation::Description description = R"(
Returns the name of the SQL-defined HTTP handler (created with `CREATE HANDLER`) that invoked the query.
Returns an empty string if the query was not invoked through such a handler.

Useful to customize query behavior depending on the invoked handler.
    )";
    FunctionDocumentation::Syntax syntax = "currentHandler()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the current handler name.", {"String"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT currentHandler()", ""}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCurrentHandler>(documentation);
}

}
