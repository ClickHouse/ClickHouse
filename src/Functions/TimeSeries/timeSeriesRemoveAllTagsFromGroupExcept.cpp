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

/// Function timeSeriesRemoveAllTagsFromGroupExcept(group, ['tag_name_1', 'tag_name_2', ...])
/// removes all tags from a tags group except specified ones, and returns the new tags group.
class FunctionTimeSeriesRemoveAllTagsFromGroupExcept : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesRemoveAllTagsFromGroupExcept";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesRemoveAllTagsFromGroupExcept>(context_); }
    explicit FunctionTimeSeriesRemoveAllTagsFromGroupExcept(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    /// Function timeSeriesRemoveAllTagsFromGroupExcept uses information stored in the query context, it's deterministic in the scope of the current query.
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
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstStrings(name, arguments, 1);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto old_groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 0);
        auto tags_to_keep = TimeSeriesTagsFunctionHelpers::extractConstStringsFromArgument(name, arguments, 1);
        
        auto & tags_collector = getContext()->getQueryContext()->getTimeSeriesTagsCollector();
        auto new_groups = tags_collector.removeAllTagsFromGroupExcept(old_groups, tags_to_keep);

        chassert(new_groups.size() == input_rows_count);
        return TimeSeriesTagsFunctionHelpers::makeColumnForGroup(new_groups);
    }
};


REGISTER_FUNCTION(TimeSeriesRemoveAllTagsFromGroupExcept)
{
    FunctionDocumentation::Description description = R"(Removes all tags from a tags group except specified ones.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesRemoveAllTagsFromGroupExcept(group, tags_to_keep)";
    FunctionDocumentation::Arguments arguments = {{"group", "A group associated with a set of tags.", {"UInt64"}},
                                                  {"tags_to_keep", "The names of tags to keep in the group.", {"Array(String)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"A tags group with only the specified tags kept.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesStoreTags(8374283493092, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS id, timeSeriesIdToTagsGroup(id) AS group, timeSeriesRemoveAllTagsFromGroupExcept(group, ['env']) AS group1, timeSeriesTagsGroupToTags(group1", "8374283493092    0    1    [('env','dev')]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesRemoveAllTagsFromGroupExcept>(documentation);
}

}
