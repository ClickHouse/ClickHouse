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

/// Function timeSeriesLabelJoin(<group>, 'dest_tag', 'separator', ['src_tag_1', 'src_tag_2', ...])
/// joins all the values of all the `src_tags` using `separator` and returns a new group with the tag `dest_tag` set to the joined value.
class FunctionTimeSeriesLabelJoin : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesLabelJoin";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesLabelJoin>(context_); }
    explicit FunctionTimeSeriesLabelJoin(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    /// Function timeSeriesLabelJoin uses information stored in the query context, it's deterministic in the scope of the current query.
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
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstString(name, arguments, 1);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstString(name, arguments, 2);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstStrings(name, arguments, 3);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto old_groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 0);
        auto dest_tag = TimeSeriesTagsFunctionHelpers::extractConstStringFromArgument(name, arguments, 1);
        auto separator = TimeSeriesTagsFunctionHelpers::extractConstStringFromArgument(name, arguments, 2);
        auto src_tags = TimeSeriesTagsFunctionHelpers::extractConstStringsFromArgument(name, arguments, 3);

        auto & tags_collector = getContext()->getQueryContext()->getTimeSeriesTagsCollector();
        auto new_groups = tags_collector.labelJoin(old_groups, dest_tag, separator, src_tags);

        chassert(new_groups.size() == input_rows_count);
        return TimeSeriesTagsFunctionHelpers::makeColumnForGroup(new_groups);
    }
};


REGISTER_FUNCTION(TimeSeriesLabelJoin)
{
    FunctionDocumentation::Description description = R"(Joins all the values of all the `src_tags` using `separator` and returns a new group with the tag `dest_tag` set to the joined value.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesLabelJoin(group, dest_tag, separator, src_tags)";
    FunctionDocumentation::Arguments arguments = {{"group", "A group associated with a set of tags.", {"UInt64"}},
                                                  {"dest_tag", "The name of a destination tag to get the result group.", {"String"}},
                                                  {"separator", "A separator to insert between joined values.", {"String"}},
                                                  {"src_tags", "The names of source tags to get values for joining.", {"Array(String)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"A tags group with the tag `dest_tag` set to the joined value.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesGet(8374283493092, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS id, timeSeriesIdToTagsGroup(id) AS group, timeSeriesLabelJoin(group, '__name__') AS group1, timeSeriesTagsGroupToTags(group1)", "8374283493092    0    1    [('env', 'dev'), ('region', 'eu')]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesLabelJoin>(documentation);
}

}
