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

/// Function FunctionTimeSeriesRemoveTagsFromGroup(<group>, ['<tag_name1>', '<tag_name2>', ...]) removes specified tags from a tags group,
/// and returns the new tags group.
class FunctionTimeSeriesRemoveTagsFromGroup : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesRemoveTagsFromGroup";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesRemoveTagsFromGroup>(context_); }
    explicit FunctionTimeSeriesRemoveTagsFromGroup(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    /// Function timeSeriesRemoveTagsFromGroup() uses the information stored in the query context by function timeSeriesStoreTags(),
    /// so it's deterministic in the scope of the current query.
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
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with two arguments", name);

        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForGroup(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagNames(name, arguments, 1);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto old_groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 0);
        auto tags_to_remove = TimeSeriesTagsFunctionHelpers::extractConstTagNamesFromArgument(name, arguments, 1);
        
        auto & tags_collector = getContext()->getQueryContext()->getTimeSeriesTagsCollector();
        auto new_groups = tags_collector.removeTagsFromGroup(old_groups, tags_to_remove);

        chassert(new_groups.size() == input_rows_count);
        return TimeSeriesTagsFunctionHelpers::makeColumnForGroup(new_groups);
    }
};


REGISTER_FUNCTION(TimeSeriesRemoveTagsFromGroup)
{
    FunctionDocumentation::Description description = R"(Removes specified tags from a tags group. If there are no such tag in the group then the group is returned unchanged.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesRemoveTagsFromGroup(group, tags_to_remove)";
    FunctionDocumentation::Arguments arguments = {{"group", "A group associated with a set of tags.", {"UInt64"}},
                                                  {"tags_to_remove", "The names of tags to remove from the group.", {"Array(String)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"A tags group without the specified tags.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesStoreTags(8374283493092, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS id, timeSeriesIdToTagsGroup(id) AS group, timeSeriesRemoveTagsFromGroup(group, ['env', 'region']) AS group1, timeSeriesTagsGroupToTags(group1)", "8374283493092    0    1    [('__name__','http_requests_count')]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesRemoveTagsFromGroup>(documentation);
}

}
