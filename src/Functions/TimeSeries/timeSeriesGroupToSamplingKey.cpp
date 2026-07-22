#include <Functions/FunctionFactory.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesGroupToSamplingKey(group) returns a stable UInt64 hash derived from the tags
/// of a specified group. The value is intended as a deterministic sort key for sampling operations
/// like `limitk` and `limit_ratio`, where Prometheus requires a "deterministic pseudo-random" order.
class FunctionTimeSeriesGroupToSamplingKey : public IFunction
{
public:
    static constexpr auto name = "timeSeriesGroupToSamplingKey";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimeSeriesGroupToSamplingKey>(context); }
    explicit FunctionTimeSeriesGroupToSamplingKey(ContextPtr context) : tags_collector(context->getQueryContext()->getTimeSeriesTagsCollector()) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    /// Function timeSeriesGroupToSamplingKey returns information stored in the query context, it's deterministic in the scope of the current query.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeUInt64>();
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with one argument: {}(group)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForGroup(name, arguments, 0);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 0);

        auto sampling_keys = tags_collector->getSamplingKeyByGroup(groups);
        chassert(sampling_keys.size() == input_rows_count);

        auto result_column = ColumnUInt64::create();
        result_column->reserve(sampling_keys.size());
        for (UInt64 key : sampling_keys)
            result_column->insertValue(key);
        return result_column;
    }

private:
    std::shared_ptr<const ContextTimeSeriesTagsCollector> tags_collector;
};


REGISTER_FUNCTION(TimeSeriesGroupToSamplingKey)
{
    FunctionDocumentation::Description description = R"(
Returns a stable `UInt64` sampling key derived from the tags of a specified group.

The value is deterministic: identical input tags always produce the same key.
It's intended as a sort key for sampling operators like `limitk` and `limit_ratio`.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesGroupToSamplingKey(group)";
    FunctionDocumentation::Arguments arguments = {{"group", "A group of tags.", {"UInt64"}}};
    FunctionDocumentation::ReturnedValue returned_value = {
        "A stable `UInt64` hash derived from the tags associated with the group.",
        {"UInt64"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS group,
       timeSeriesGroupToSamplingKey(group) AS sampling_key
        )",
        R"(
┌─group─┬─────────sampling_key─┐
│     1 │ 12876543210987654321 │
└───────┴──────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesGroupToSamplingKey>(documentation);
}

}
