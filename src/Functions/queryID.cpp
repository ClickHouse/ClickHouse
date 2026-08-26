#include <Columns/IColumn.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>

namespace DB
{
class FunctionQueryID : public IFunction
{
    const String query_id;

public:
    static constexpr auto name = "queryID";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionQueryID>(context->getClientInfo().current_query_id);
    }

    explicit FunctionQueryID(const String & query_id_) : query_id(query_id_) {}

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
        return DataTypeString().createColumnConst(input_rows_count, query_id)->convertToFullColumnIfConst();
    }
};

REGISTER_FUNCTION(QueryID)
{
    FunctionDocumentation::Description description = R"(
Returns the ID of the current query.
Other parameters of a query can be extracted from field `query_id` in the [`system.query_log`](../../operations/system-tables/query_log.md) table.

In contrast to [`initialQueryID`](#initialQueryID) function, `queryID` can return different results on different shards.
)";
    FunctionDocumentation::Syntax syntax = "queryID()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the ID of the current query.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
CREATE TABLE tmp (str String) ENGINE = Log;
INSERT INTO tmp (*) VALUES ('a');
SELECT count(DISTINCT t) FROM (SELECT queryID() AS t FROM remote('127.0.0.{1..3}', currentDatabase(), 'tmp') GROUP BY queryID());
        )",
        R"(
┌─count(DISTINCT t)─┐
│                 3 │
└───────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionQueryID>(documentation);
    factory.registerAlias("query_id", FunctionQueryID::name, FunctionFactory::Case::Insensitive);
}
}
