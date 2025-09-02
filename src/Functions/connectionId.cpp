#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>


namespace DB
{

/// Get the connection Id. It's used for MySQL handler only.
class FunctionConnectionId : public IFunction, WithContext
{
public:
    static constexpr auto name = "connectionId";

    explicit FunctionConnectionId(ContextPtr context_) : WithContext(context_) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionConnectionId>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return std::make_shared<DataTypeUInt64>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(input_rows_count, getContext()->getClientInfo().connection_id);
    }
};

REGISTER_FUNCTION(ConnectionId)
{
    FunctionDocumentation::Description description_connectionId = R"(
Retrieves the connection ID of the client that submitted the current query and returns it as a `UInt64` integer.
This function is most useful in debugging scenarios or for internal purposes within the MySQL handler.
It was created for compatibility with MySQL's `CONNECTION_ID` function.
It is not typically used in production queries.
)";
    FunctionDocumentation::Syntax syntax_connectionId = "connectionId()";
    FunctionDocumentation::Arguments arguments_connectionId = {};
    FunctionDocumentation::ReturnedValue returned_value_connectionId = {"Returns the connection ID of the current client.", {"UInt64"}};
    FunctionDocumentation::Examples examples_connectionId = {
    {
        "Usage example",
        R"(
SELECT connectionId();
        )",
        R"(
┌─connectionId()─┐
│              0 │
└────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_connectionId = {21, 3};
    FunctionDocumentation::Category category_connectionId = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_connectionId = {description_connectionId, syntax_connectionId, arguments_connectionId, returned_value_connectionId, examples_connectionId, introduced_in_connectionId, category_connectionId};

    factory.registerFunction<FunctionConnectionId>(documentation_connectionId, FunctionFactory::Case::Insensitive);
    factory.registerAlias("connection_id", "connectionID", FunctionFactory::Case::Insensitive);
}

}
