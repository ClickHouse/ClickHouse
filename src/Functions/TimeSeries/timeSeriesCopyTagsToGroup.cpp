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

/// Function FunctionTimeSeriesCopyTagsToGroup(<dest_group>, <src_group>, ['<tag_name1>', '<tag_name2>', ...])
/// copies specified tags from the `src` group to the `dest` group, and returns the new tags group.
class FunctionTimeSeriesCopyTagsToGroup : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesCopyTagsToGroup";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesCopyTagsToGroup>(context_); }
    explicit FunctionTimeSeriesCopyTagsToGroup(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    /// Function timeSeriesCopyTagsToGroup() uses the information stored in the query context by function timeSeriesStoreTags(),
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
        if (arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with two arguments", name);

        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForGroup(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForGroup(name, arguments, 1);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagNames(name, arguments, 2);
    }

    using Group = ContextTimeSeriesTagsCollector::Group;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto dest_groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 0, /* return_single_element_if_const_column = */ true);
        auto src_groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 1, /* return_single_element_if_const_column = */ true);
        chassert((dest_groups.size() == input_rows_count) || (src_groups.size() == input_rows_count));

        auto tags_to_copy = TimeSeriesTagsFunctionHelpers::extractConstTagNamesFromArgument(name, arguments, 2);
        
        auto & tags_collector = getContext()->getQueryContext()->getTimeSeriesTagsCollector();
        std::vector<Group> new_groups;

        if (dest_groups.size() == 1)
            new_groups = tags_collector.copyTagsToGroup(dest_groups[0], src_groups, tags_to_copy);
        else if (src_groups.size() == 1)
            new_groups = tags_collector.copyTagsToGroup(dest_groups, src_groups[0], tags_to_copy);
        else
            new_groups = tags_collector.copyTagsToGroup(dest_groups, src_groups, tags_to_copy);

        chassert(new_groups.size() == input_rows_count);
        return TimeSeriesTagsFunctionHelpers::makeColumnForGroup(new_groups);
    }
};


REGISTER_FUNCTION(TimeSeriesCopyTagsToGroup)
{
    FunctionDocumentation::Description description = R"(Copies specified tags from the 'src' group to the 'dest' group.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesCopyTagsToGroup(dest_group, src_group, tags_to_copy)";
    FunctionDocumentation::Arguments arguments = {{"group", "A group associated with a set of tags.", {"UInt64"}},
                                                  {"tags_to_copy", "The names of tags to copy.", {"Array(String)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"A tags group with copied tags.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesStoreTags(8374283493092, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS id1, timeSeriesStoreTags(524344498142, [('code', '404'), ('message', 'Page not found')], '__name__', 'http_codes') AS id2, timeSeriesIdToTagsGroup(id1) AS group1, timeSeriesIdToTagsGroup(id2) AS group2, timeSeriesCopyTagsToGroup(group1, group2, ['__name__', 'code', 'env']) AS res, timeSeriesTagsGroupToTags(res)", "8374283493092    524344498142    0    1    2    [('__name__','http_codes'),('code','404'),('region','eu')]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesCopyTagsToGroup>(documentation);
}

}
