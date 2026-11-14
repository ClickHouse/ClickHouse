#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>


namespace DB
{

/// Get the connection Id. It's used for MySQL handler only.
class FunctionConnectionId final : public IFunction
{
public:
    static constexpr auto name = "connectionId";

    /// The connection id is a property of the query, so capture it here instead of keeping a reference to
    /// the context. A function built while analyzing a subquery can be executed after the context that
    /// built it is gone - e.g. a scalar subquery whose expression actions are reused by the outer query -
    /// and looking the context up at execution time throws `Context has expired`.
    explicit FunctionConnectionId(const ContextPtr & context_) : connection_id(context_->getClientInfo().connection_id) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionConnectionId>(context_); }

    bool isDeterministic() const override { return false; }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return std::make_shared<DataTypeUInt64>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(input_rows_count, connection_id);
    }

private:
    const UInt64 connection_id;
};

REGISTER_FUNCTION(ConnectionId)
{
    FunctionDocumentation::Description description = R"(
Returns the connection ID of the client that submitted the current query.
This function is most useful in debugging scenarios.
It was created for compatibility with MySQL's `CONNECTION_ID` function.
It is not typically used in production queries.
)";
    FunctionDocumentation::Syntax syntax = "connectionId()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the connection ID of the current client.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
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
    FunctionDocumentation::IntroducedIn introduced_in = {21, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionConnectionId>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("connection_id", "connectionID", FunctionFactory::Case::Insensitive);
}

}
