#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>

namespace DB
{
class FunctionInitialQueryID : public IFunction
{
    const String initial_query_id;

public:
    static constexpr auto name = "initialQueryID";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionInitialQueryID>(context->getClientInfo().initial_query_id);
    }

    explicit FunctionInitialQueryID(const String & initial_query_id_) : initial_query_id(initial_query_id_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, initial_query_id);
    }
};

REGISTER_FUNCTION(InitialQueryID)
{
    FunctionDocumentation::Description description = R"(
Returns the ID of the initial current query.
Other parameters of a query can be extracted from field `initial_query_id` in [`system.query_log`](../../operations/system-tables/query_log.md).

In contrast to [`queryID`](/sql-reference/functions/other-functions#queryID) function, `initialQueryID` returns the same results on different shards.
)";
    FunctionDocumentation::Syntax syntax = "initialQueryID()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the ID of the initial current query.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
CREATE TABLE tmp (str String) ENGINE = Log;
INSERT INTO tmp (*) VALUES ('a');
SELECT count(DISTINCT t) FROM (SELECT initialQueryID() AS t FROM remote('127.0.0.{1..3}', currentDatabase(), 'tmp') GROUP BY queryID());
        )",
        R"(
┌─count(DISTINCT t)─┐
│                 1 │
└───────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionInitialQueryID>(documentation);
    factory.registerAlias("initial_query_id", FunctionInitialQueryID::name, FunctionFactory::Case::Insensitive);
}
}
