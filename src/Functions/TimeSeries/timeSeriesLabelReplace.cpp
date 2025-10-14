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

/// Function timeSeriesLabelReplace(group, 'dest_tag', 'replacement', 'src_tag', 'regex')
/// matches the regular expression `regex` against the value of the tag `src_tag`.
/// If it matches, the value of the tag `dest_tag` in the returned group will be the expansion of `replacement`,
/// together with the original tags in the input.
class FunctionTimeSeriesLabelReplace : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesLabelReplace";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesLabelReplace>(context_); }
    explicit FunctionTimeSeriesLabelReplace(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    /// Function timeSeriesLabelReplace uses information stored in the query context, it's deterministic in the scope of the current query.
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
        if (arguments.size() != 5)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with five arguments: {}(group, dest_tag, replacement, src_tag, regex)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForGroup(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstString(name, arguments, 1);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstString(name, arguments, 2);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstString(name, arguments, 3);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstString(name, arguments, 4);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto old_groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 0);
        auto dest_tag = TimeSeriesTagsFunctionHelpers::extractConstStringFromArgument(name, arguments, 1);
        auto replacement = TimeSeriesTagsFunctionHelpers::extractConstStringFromArgument(name, arguments, 2);
        auto src_tag = TimeSeriesTagsFunctionHelpers::extractConstStringFromArgument(name, arguments, 3);
        auto regex = TimeSeriesTagsFunctionHelpers::extractConstStringFromArgument(name, arguments, 4);

        auto & tags_collector = getContext()->getQueryContext()->getTimeSeriesTagsCollector();
        auto new_groups = tags_collector.labelReplace(old_groups, dest_tag, replacement, src_tag, regex);

        chassert(new_groups.size() == input_rows_count);
        return TimeSeriesTagsFunctionHelpers::makeColumnForGroup(new_groups);
    }
};


REGISTER_FUNCTION(TimeSeriesLabelReplace)
{
    FunctionDocumentation::Description description = R"(Matches the regular expression `regex` against the value of the tag `src_tag`. If it matches, the value of the tag `dest_tag` in the returned group will be the expansion of `replacement`, together with the original tags in the input.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesLabelReplace(group, dest_tag, replacement, src_tag, regex)";
    FunctionDocumentation::Arguments arguments = {{"group", "A group associated with a set of tags.", {"UInt64"}},
                                                  {"dest_tag", "The name of a destination tag to get the result group.", {"String"}},
                                                  {"replacement", "A replacement pattern, can contain $1, $2 or $name to refer capturing groups in the regular expression 'regex'.", {"String"}},
                                                  {"src_tag", "The name of a tag which value is used to match the regular expression 'regex'.", {"String"}},
                                                  {"regex", "A regular expression.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"A tags group without the specified tag.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesStoreTags(8374283493092, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS id, timeSeriesIdToTagsGroup(id) AS group, timeSeriesLabelReplace(group, '__name__') AS group1, timeSeriesTagsGroupToTags(group1)", "8374283493092    0    1    [('env', 'dev'), ('region', 'eu')]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesLabelReplace>(documentation);
}

}
