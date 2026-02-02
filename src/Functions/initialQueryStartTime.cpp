#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Core/Field.h>

namespace DB
{
class FunctionInitialQueryStartTime : public IFunction
{
    const time_t initial_query_start_time;
public:
    static constexpr auto name = "initialQueryStartTime";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionInitialQueryStartTime>(context->getClientInfo().initial_query_start_time);
    }

    explicit FunctionInitialQueryStartTime(const time_t & initial_query_start_time_) : initial_query_start_time(initial_query_start_time_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeDateTime>();
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeDateTime().createColumnConst(
                input_rows_count,
                static_cast<UInt64>(initial_query_start_time));
    }
};

REGISTER_FUNCTION(InitialQueryStartTime)
{
    FunctionDocumentation::Description description = R"(
Returns the start time of the initial current query.
`initialQueryStartTime` returns the same results on different shards.
)";
    FunctionDocumentation::Syntax syntax = "initialQueryStartTime()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the start time of the initial current query.", {"DateTime"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
CREATE TABLE tmp (str String) ENGINE = Log;
INSERT INTO tmp (*) VALUES ('a');
SELECT count(DISTINCT t) FROM (SELECT initialQueryStartTime() AS t FROM remote('127.0.0.{1..3}', currentDatabase(), 'tmp') GROUP BY queryID());
        )",
        R"(
┌─count(DISTINCT t)─┐
│                 1 │
└───────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionInitialQueryStartTime>(documentation);
    factory.registerAlias("initial_query_start_time", FunctionInitialQueryStartTime::name, FunctionFactory::Case::Insensitive);
}
}
