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

/// Function timeSeriesRemoveAllTagsExcept(group, ['tag_name_1', 'tag_name_2', ...])
/// removes all tags from a tags group except specified ones, and returns the new tags group.
class FunctionTimeSeriesRemoveAllTagsExcept : public IFunction
{
public:
    static constexpr auto name = "timeSeriesRemoveAllTagsExcept";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimeSeriesRemoveAllTagsExcept>(context); }
    explicit FunctionTimeSeriesRemoveAllTagsExcept(ContextPtr context) : tags_collector(context->getQueryContext()->getTimeSeriesTagsCollector()) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    /// Function timeSeriesRemoveAllTagsExcept uses information stored in the query context, it's deterministic in the scope of the current query.
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
                            "Function {} must be called with two arguments: {}(group, tags_to_keep)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForGroup(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagNames(name, arguments, 1);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto old_groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 0);
        auto tags_to_keep = TimeSeriesTagsFunctionHelpers::extractConstTagNamesFromArgument(name, arguments, 1);

        auto new_groups = tags_collector->removeAllTagsExcept(old_groups, tags_to_keep);
        chassert(new_groups.size() == input_rows_count);

        return TimeSeriesTagsFunctionHelpers::makeColumnForGroup(new_groups);
    }

private:
    std::shared_ptr<ContextTimeSeriesTagsCollector> tags_collector;
};


REGISTER_FUNCTION(TimeSeriesRemoveAllTagsExcept)
{
    FunctionDocumentation::Description description = R"(
Removes all tags except specified ones from a group of tags.
See also function [timeSeriesRemoveTag()](/sql-reference/functions/time-series-functions#timeSeriesRemoveTag),
[timeSeriesRemoveTags()](/sql-reference/functions/time-series-functions#timeSeriesRemoveTags).
)";
    FunctionDocumentation::Syntax syntax = "timeSeriesRemoveAllTagsExcept(group, tags_to_keep)";
    FunctionDocumentation::Arguments arguments = {
        {"group", "A group of tags.", {"UInt64"}},
        {"tags_to_keep", "The names of tags to keep in the group.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "A new group of tags with only the specified tags kept.", {"UInt64"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS group,
       timeSeriesRemoveAllTagsExcept(group, ['env']) AS result_group,
       timeSeriesGroupToTags(result_group)
        )",
        R"(
┌─group─┬─result_group─┬─timeSeriesGroupToTags(result_group)─┐
│     1 │            2 │ [('env','dev')]                     │
└───────┴──────────────┴─────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesRemoveAllTagsExcept>(documentation);
}

}
