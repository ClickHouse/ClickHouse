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

/// Function timeSeriesRemoveTags(group, ['tag_name_1', 'tag_name_2', ...]) removes specified tags from a tags group,
/// and returns the new tags group.
class FunctionTimeSeriesRemoveTags : public IFunction
{
public:
    static constexpr auto name = "timeSeriesRemoveTags";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimeSeriesRemoveTags>(context); }
    explicit FunctionTimeSeriesRemoveTags(ContextPtr context) : tags_collector(context->getQueryContext()->getTimeSeriesTagsCollector()) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    /// Function timeSeriesRemoveTags uses information stored in the query context, it's deterministic in the scope of the current query.
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
                            "Function {} must be called with two arguments: {}(group, tags_to_remove)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForGroup(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagNames(name, arguments, 1);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto old_groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 0);
        auto tags_to_remove = TimeSeriesTagsFunctionHelpers::extractConstTagNamesFromArgument(name, arguments, 1);

        auto new_groups = tags_collector->removeTags(old_groups, tags_to_remove);
        chassert(new_groups.size() == input_rows_count);

        return TimeSeriesTagsFunctionHelpers::makeColumnForGroup(new_groups);
    }

private:
    std::shared_ptr<ContextTimeSeriesTagsCollector> tags_collector;
};


REGISTER_FUNCTION(TimeSeriesRemoveTags)
{
    FunctionDocumentation::Description description = R"(
Removes specified tags from a group of tags.
If some of the specified tags are not in the group of tags the function ignores them.
See also function [timeSeriesRemoveTag()](/sql-reference/functions/time-series-functions#timeSeriesRemoveTag),
[timeSeriesRemoveAllTagsExcept()](/sql-reference/functions/time-series-functions#timeSeriesRemoveAllTagsExcept).
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesRemoveTags(group, tags_to_remove)";
    FunctionDocumentation::Arguments arguments = {
        {"group", "A group of tags.", {"UInt64"}},
        {"tags_to_remove", "The names of tags to remove from the group.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "A new group of tags without the specified tags.", {"UInt64"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS group_of_3,
       timeSeriesRemoveTags(group_of_3, ['env', 'region']) AS group_of_1,
       timeSeriesGroupToTags(group_of_1),
       timeSeriesRemoveTags(group_of_1, ['__name__', 'nonexistent']) AS empty_group,
       timeSeriesGroupToTags(empty_group)
        )",
        R"(
┌─group_of_3─┬─group_of_1─┬─timeSeriesGroupToTags(group_of_1)────┬─empty_group─┬─timeSeriesGroupToTags(empty_group)─┐
│          1 │          2 │ [('__name__','http_requests_count')] │           0 │ []                                 │
└────────────┴────────────┴──────────────────────────────────────┴─────────────┴────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesRemoveTags>(documentation);
}

}
