#include <Functions/FunctionFactory.h>

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

/// Function timeSeriesRemoveTag(group, 'tag_name') removes a specified tag from a tags group,
/// and returns the new tags group.
class FunctionTimeSeriesRemoveTag : public IFunction
{
public:
    static constexpr auto name = "timeSeriesRemoveTag";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimeSeriesRemoveTag>(context); }
    explicit FunctionTimeSeriesRemoveTag(ContextPtr context) : tags_collector(context->getQueryContext()->getTimeSeriesTagsCollector()) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    /// Function timeSeriesRemoveTag uses information stored in the query context, it's deterministic in the scope of the current query.
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
        if (arguments.size() != 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with two arguments: {}(group, tag_to_remove)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForGroup(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagName(name, arguments, 1);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto old_groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 0);
        auto tag_to_remove = TimeSeriesTagsFunctionHelpers::extractConstTagNameFromArgument(name, arguments, 1);

        auto new_groups = tags_collector->removeTag(old_groups, tag_to_remove);
        chassert(new_groups.size() == input_rows_count);

        return TimeSeriesTagsFunctionHelpers::makeColumnForGroup(new_groups);
    }

private:
    std::shared_ptr<ContextTimeSeriesTagsCollector> tags_collector;
};


REGISTER_FUNCTION(TimeSeriesRemoveTag)
{
    FunctionDocumentation::Description description = R"(
Removes a specified tag from a group of tags.
If there is no such tag in the group then the group is returned unchanged.
See also function [timeSeriesRemoveTags()](/sql-reference/functions/time-series-functions#timeSeriesRemoveTags),
[timeSeriesRemoveAllTagsExcept()](/sql-reference/functions/time-series-functions#timeSeriesRemoveAllTagsExcept).
)";
    FunctionDocumentation::Syntax syntax = "timeSeriesRemoveTag(group, tag_to_remove)";
    FunctionDocumentation::Arguments arguments = {
        {"group", "A group of tags.", {"UInt64"}},
        {"tag_to_remove", "The name of a tag to remove from the group.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value = {
        "A new group of tags without the specified tag.", {"UInt64"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS group_of_3,
       timeSeriesRemoveTag(group_of_3, '__name__') AS group_of_2,
       timeSeriesGroupToTags(group_of_2),
       timeSeriesRemoveTag(group_of_2, 'env') AS group_of_1,
       timeSeriesGroupToTags(group_of_1),
       timeSeriesRemoveTag(group_of_1, 'region') AS empty_group,
       timeSeriesGroupToTags(empty_group)
        )",
        R"(
┌─group_of_3─┬─group_of_2─┬─timeSeriesGroupToTags(group_of_2)─┬─group_of_1─┬─timeSeriesGroupToTags(group_of_1)─┬─empty_group─┬─timeSeriesGroupToTags(empty_group)─┐
│          1 │          2 │ [('env','dev'),('region','eu')]   │          3 │ [('region','eu')]                 │           0 │ []                                 │
└────────────┴────────────┴───────────────────────────────────┴────────────┴───────────────────────────────────┴─────────────┴────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesRemoveTag>(documentation);
}

}
