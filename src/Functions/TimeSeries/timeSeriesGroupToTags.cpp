#include <Functions/FunctionFactory.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesGroupToTags(group) returns Array(Tuple(String, String))
/// containing the names and values of tags associated with a specified group.
class FunctionTimeSeriesGroupToTags : public IFunction
{
public:
    static constexpr auto name = "timeSeriesGroupToTags";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimeSeriesGroupToTags>(context); }
    explicit FunctionTimeSeriesGroupToTags(ContextPtr context) : tags_collector(context->getQueryContext()->getTimeSeriesTagsCollector()) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    /// Function timeSeriesGroupToTags returns information stored in the query context, it's deterministic in the scope of the current query.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}));
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

        auto tags = tags_collector->getTagsByGroup(groups);
        chassert(tags.size() == input_rows_count);

        return TimeSeriesTagsFunctionHelpers::makeColumnForTagNamesAndValues(tags);
    }

private:
    std::shared_ptr<const ContextTimeSeriesTagsCollector> tags_collector;
};


REGISTER_FUNCTION(TimeSeriesGroupToTags)
{
    FunctionDocumentation::Description description = R"(
Returns the names and values of the tags associated with a specified group.
See also function [timeSeriesTagsToGroup()](/sql-reference/functions/time-series-functions#timeSeriesTagsToGroup).
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesGroupToTags(group)";
    FunctionDocumentation::Arguments arguments = {{"group", "A group of tags.", {"UInt64"}}};
    FunctionDocumentation::ReturnedValue returned_value = {
        R"(
Returns an array of pairs `(tag_name, tag_value)`.
The returned array is always sorted by `tag_name` and never contains the same `tag_name` more than once.
        )",
        {"Array(Tuple(String, String))"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS group,
       timeSeriesGroupToTags(group) AS sorted_tags,
       timeSeriesTagsToGroup(sorted_tags) AS same_group,
       throwIf(same_group != group)
        )",
        R"(
┌─group─┬─sorted_tags────────────────────────────────────────────────────────┬─same_group─┬─throwIf(notE⋯up, group))─┐
│     1 │ [('__name__','http_requests_count'),('env','dev'),('region','eu')] │          1 │                        0 │
└───────┴────────────────────────────────────────────────────────────────────┴────────────┴──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesGroupToTags>(documentation);
    factory.registerAlias("timeSeriesTagsGroupToTags", "timeSeriesGroupToTags");
}

}
