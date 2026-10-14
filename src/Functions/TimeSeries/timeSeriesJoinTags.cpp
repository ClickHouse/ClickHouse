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

/// Function timeSeriesJoinTags(<group>, 'dest_tag', 'separator', ['src_tag_1', 'src_tag_2', ...])
/// joins all the values of all the `src_tags` using `separator` and returns a new group with the tag `dest_tag` set to the joined value.
class FunctionTimeSeriesJoinTags : public IFunction
{
public:
    static constexpr auto name = "timeSeriesJoinTags";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimeSeriesJoinTags>(context); }
    explicit FunctionTimeSeriesJoinTags(ContextPtr context) : tags_collector(context->getQueryContext()->getTimeSeriesTagsCollector()) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 4; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    /// Function timeSeriesJoinTags uses information stored in the query context, it's deterministic in the scope of the current query.
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
        if (arguments.size() != 4)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with four arguments: {}(group, dest_tag, separator, src_tags)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForGroup(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagName(name, arguments, 1);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstString(name, arguments, 2);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagNames(name, arguments, 3);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto old_groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 0);
        auto dest_tag = TimeSeriesTagsFunctionHelpers::extractConstTagNameFromArgument(name, arguments, 1);
        auto separator = TimeSeriesTagsFunctionHelpers::extractConstStringFromArgument(name, arguments, 2);
        auto src_tags = TimeSeriesTagsFunctionHelpers::extractConstTagNamesFromArgument(name, arguments, 3);

        auto new_groups = tags_collector->joinTags(old_groups, dest_tag, separator, src_tags);
        chassert(new_groups.size() == input_rows_count);

        return TimeSeriesTagsFunctionHelpers::makeColumnForGroup(new_groups);
    }

private:
    std::shared_ptr<ContextTimeSeriesTagsCollector> tags_collector;
};


REGISTER_FUNCTION(TimeSeriesJoinTags)
{
    FunctionDocumentation::Description description = R"(
Joins the values of specified tags extracted from a group of tags.
The function inserts a separator between joined values and returns a new group of tags
with the tag `dest_tag` set to the joined value.
This function mimics the logic of the prometheus function
[label_join()](https://prometheus.io/docs/prometheus/latest/querying/functions/#label_join).
)";
    FunctionDocumentation::Syntax syntax = "timeSeriesJoinTags(group, dest_tag, separator, src_tags)";
    FunctionDocumentation::Arguments arguments = {
        {"group", "A group of tags.", {"UInt64"}},
        {"dest_tag", "The name of a tag with the joined result which will be added to the `group`.", {"String"}},
        {"separator", "A separator to insert between joined values.", {"String"}},
        {"src_tags", "The names of source tags with values which will be joined.", {"Array(String)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a new group of tags with the `dest_tag` tag set to the joined result.", {"UInt64"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesTagsToGroup([('__name__', 'up'), ('job', 'api-server'), ('src1', 'a'), ('src2', 'b'), ('src3', 'c')]) AS group,
       timeSeriesJoinTags(group, 'foo', ',', ['src1', 'src2', 'src3']) AS result_group,
       timeSeriesGroupToTags(result_group)
        )",
        R"(
┌─group─┬─result_group─┬─timeSeriesGroupToTags(result_group)─────────────────────────────────────────────────────────────┐
│     1 │            2 │ [('__name__','up'),('foo','a,b,c'),('job','api-server'),('src1','a'),('src2','b'),('src3','c')] │
└───────┴──────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesJoinTags>(documentation);
}

}
