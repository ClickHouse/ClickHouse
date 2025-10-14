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

/// Function FunctionTimeSeriesRemoveTagFromGroup(<group>, '<tag_name>') removes a specified tag from a tags group,
/// and returns the new tags group.
class FunctionTimeSeriesRemoveTagFromGroup : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesRemoveTagFromGroup";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesRemoveTagFromGroup>(context_); }
    explicit FunctionTimeSeriesRemoveTagFromGroup(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    /// Function timeSeriesRemoveTagFromGroup() uses the information stored in the query context by function timeSeriesStoreTags(),
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
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagName(name, arguments, 1);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        chassert(arguments.size() == 2);

        auto old_groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 0);
        auto tag_to_remove = TimeSeriesTagsFunctionHelpers::extractConstTagNameFromArgument(name, arguments, 1);
        
        auto & tags_collector = getContext()->getQueryContext()->getTimeSeriesTagsCollector();
        auto new_groups = tags_collector.removeTagFromGroup(old_groups, tag_to_remove);

        chassert(new_groups.size() == input_rows_count);
        return TimeSeriesTagsFunctionHelpers::makeColumnForGroup(new_groups);
    }
};


REGISTER_FUNCTION(TimeSeriesRemoveTagFromGroup)
{
    FunctionDocumentation::Description description = R"(Removes a specified tag from a tags group. If there is no such tag in the group then the group is returned unchanged.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesRemoveTagFromGroup(group, tag_to_remove)";
    FunctionDocumentation::Arguments arguments = {{"group", "A group associated with a set of tags.", {"UInt64"}},
                                                  {"tag_to_remove", "The name of a tag to remove from the group.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"A tags group without the specified tag.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesStoreTags(8374283493092, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS id, timeSeriesIdToTagsGroup(id) AS group, timeSeriesRemoveTagFromGroup(group, '__name__') AS group1, timeSeriesTagsGroupToTags(group1)", "8374283493092    0    1    [('env', 'dev'), ('region', 'eu')]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesRemoveTagFromGroup>(documentation);
}

}
