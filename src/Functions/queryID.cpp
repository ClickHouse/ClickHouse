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
    FunctionDocumentation::Description description_queryID = R"(
Returns the ID of the current query.
Other parameters of a query can be extracted from field `query_id` in the [`system.query_log`](../../operations/system-tables/query_log.md) table.

In contrast to [`initialQueryID`](#initialqueryid) function, `queryID` can return different results on different shards.
)";
    FunctionDocumentation::Syntax syntax_queryID = "queryID()";
    FunctionDocumentation::Arguments arguments_queryID = {};
    FunctionDocumentation::ReturnedValue returned_value_queryID = {"Returns the ID of the current query.", {"String"}};
    FunctionDocumentation::Examples examples_queryID = {
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
    FunctionDocumentation::IntroducedIn introduced_in_queryID = {21, 9};
    FunctionDocumentation::Category category_queryID = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_queryID = {description_queryID, syntax_queryID, arguments_queryID, returned_value_queryID, examples_queryID, introduced_in_queryID, category_queryID};

    factory.registerFunction<FunctionQueryID>(documentation_queryID);
    factory.registerAlias("query_id", FunctionQueryID::name, FunctionFactory::Case::Insensitive);
}
}
