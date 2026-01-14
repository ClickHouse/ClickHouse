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

/// Function timeSeriesCopyTags(dest_group, src_group, 'tag_name')
/// copies the specified tag from the `src` group to the `dest` group, and returns the new tags group.
class FunctionTimeSeriesCopyTag : public IFunction
{
public:
    static constexpr auto name = "timeSeriesCopyTag";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimeSeriesCopyTag>(context); }
    explicit FunctionTimeSeriesCopyTag(ContextPtr context) : tags_collector(context->getQueryContext()->getTimeSeriesTagsCollector()) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    /// Function timeSeriesCopyTags uses information stored in the query context, it's deterministic in the scope of the current query.
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
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with three arguments: {}(dest_group, src_group, tag_to_copy)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForGroup(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForGroup(name, arguments, 1);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagName(name, arguments, 2);
    }

    using Group = ContextTimeSeriesTagsCollector::Group;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto dest_groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 0, /* return_single_element_if_const_column = */ true);
        auto src_groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 1, /* return_single_element_if_const_column = */ true);
        chassert((dest_groups.size() == input_rows_count) || (src_groups.size() == input_rows_count));

        auto tag_to_copy = TimeSeriesTagsFunctionHelpers::extractConstTagNameFromArgument(name, arguments, 2);

        std::vector<Group> new_groups;

        if (dest_groups.size() == 1)
            new_groups = tags_collector->copyTag(dest_groups[0], src_groups, tag_to_copy);
        else if (src_groups.size() == 1)
            new_groups = tags_collector->copyTag(dest_groups, src_groups[0], tag_to_copy);
        else
            new_groups = tags_collector->copyTag(dest_groups, src_groups, tag_to_copy);

        chassert(new_groups.size() == input_rows_count);
        return TimeSeriesTagsFunctionHelpers::makeColumnForGroup(new_groups);
    }

private:
    std::shared_ptr<ContextTimeSeriesTagsCollector> tags_collector;
};


REGISTER_FUNCTION(TimeSeriesCopyTag)
{
    FunctionDocumentation::Description description = R"(
Copies a specified tag from one group of tags (`src_group`) to another (`dest_group`).
The function replaces any previous values of the copied tag in `dest_group`.
If the copied tag is not present in `src_group`, then the function will remove it from `dest_group` as well.
The function mimics the copying logic of the prometheus
[group left/group right](https://prometheus.io/docs/prometheus/latest/querying/operators/#group-modifiers) modifiers.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesCopyTag(dest_group, src_group, tag_to_copy)";
    FunctionDocumentation::Arguments arguments = {
        {"dest_group", "The destination group of tags.", {"UInt64"}},
        {"src_group", "The source group of tags.", {"UInt64"}},
        {"tag_to_copy", "The name of a tag to copy.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a group of tags containing the tags from `dest_group` along with the copied tags from `src_group`.", {"UInt64"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS dest_group,
       timeSeriesTagsToGroup([('code', '404'), ('message', 'Page not found')], '__name__', 'http_codes') AS src_group,
       timeSeriesCopyTag(dest_group, src_group, '__name__') AS result_group,
       timeSeriesGroupToTags(result_group)
        )",
        R"(
┌─dest_group─┬─src_group─┬─result_group─┬─timeSeriesGroupToTags(result_group)────────────────────────┐
│          1 │         2 │            3 │ [('__name__','http_codes'),('code','404'),('region','eu')] │
└────────────┴───────────┴──────────────┴────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesCopyTag>(documentation);
}

}
