#include <Functions/FunctionFactory.h>

#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesTagsToGroup([('tag_name_1', 'tag_value_1'), ...], 'tag_name_2', 'tag_value_2', ...)
/// returns a group assigned to the specified set of tags.
class FunctionTimeSeriesTagsToGroup : public IFunction
{
public:
    static constexpr auto name = "timeSeriesTagsToGroup";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimeSeriesTagsToGroup>(context); }
    explicit FunctionTimeSeriesTagsToGroup(ContextPtr context) : tags_collector(context->getQueryContext()->getTimeSeriesTagsCollector()) {}

    String getName() const override { return name; }

    /// There should be 1 or more arguments.
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Function timeSeriesRemoveTag uses information stored in the query context, it's deterministic in the scope of the current query.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    /// This function allows NULLs as a way to specify that some tags don't have values.
    bool useDefaultImplementationForNulls() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeUInt64>();
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.empty())
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with at least 1 arguments: {}([('tag_name_1', 'tag_value_1), ...], 'tag_name_2', 'tag_value_2', ...)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypesForTagNamesAndValues(name, arguments, 0);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto tags_vector = TimeSeriesTagsFunctionHelpers::extractTagNamesAndValuesFromArguments(name, arguments, 0);

        auto groups = tags_collector->getGroupForTags(tags_vector);
        chassert(groups.size() == input_rows_count);

        return TimeSeriesTagsFunctionHelpers::makeColumnForGroup(groups);
    }

private:
    std::shared_ptr<ContextTimeSeriesTagsCollector> tags_collector;
};


REGISTER_FUNCTION(TimeSeriesTagsToGroup)
{
    FunctionDocumentation::Description description = R"(
Returns a group of tags associated with specified tags.
If the same group of tags is found multiple times during the query execution, the function returns the same group.
For an empty set of tags the function always returns 0.
See also function [timeSeriesGroupToTags()](/sql-reference/functions/time-series-functions#timeSeriesGroupToTags).
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesTagsToGroup(tags_array, tag_name_1, tag_value_1, tag_name2, tag_value2, ...)";
    FunctionDocumentation::Arguments arguments = {
        {"tags_array", "Array of pairs (tag_name, tag_value).", {"Array(Tuple(String, String))", "NULL"}},
        {"tag_name_i", "The name of a tag.", {"String", "FixedString"}},
        {"tag_value_i", "The value of a tag.", {"String", "FixedString", "Nullable(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a group of tags associated with the specified tags.", {"UInt64"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS group1,
       timeSeriesTagsToGroup([], '__name__', 'http_failures') AS group2,
       timeSeriesTagsToGroup([]) AS empty_group,
       timeSeriesTagsToGroup([], '__name__', 'http_failures') AS same_group2,
       throwIf(same_group2 != group2),
       timeSeriesGroupToTags(group2)
        )",
        R"(
┌─group1─┬─group2─┬─empty_group─┬─same_group2─┬─throwIf(notEquals(same_group2, group2))─┬─timeSeriesGroupToTags(group2)──┐
│      1 │      2 │           0 │           2 │                                       0 │ [('__name__','http_failures')] │
└────────┴────────┴─────────────┴─────────────┴─────────────────────────────────────────┴────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesTagsToGroup>(documentation);
}

}
