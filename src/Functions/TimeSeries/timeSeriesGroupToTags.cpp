#include <Functions/FunctionFactory.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesGroupToTags(<group>) returns Array(Tuple(String, String)) containing the names and values of tags associated with
/// a specified group.
class FunctionTimeSeriesGroupToTags : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesGroupToTags";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesGroupToTags>(context_); }
    explicit FunctionTimeSeriesGroupToTags(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    /// Function timeSeriesGroupToTags(<group>) returns the information stored in the query context by function timeSeriesStoreTags(),
    /// so it's deterministic in the scope of the current query.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}));
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with one argument", name);
        
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForGroup(name, arguments, 0);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 0);
        
        auto & tags_collector = getContext()->getQueryContext()->getTimeSeriesTagsCollector();
        auto tags = tags_collector.getTagsByGroup(groups);

        chassert(tags.size() == input_rows_count);
        return TimeSeriesTagsFunctionHelpers::makeColumnForTagNamesAndValues(tags);
    }
};


REGISTER_FUNCTION(TimeSeriesGroupToTags)
{
    FunctionDocumentation::Description description = R"(Finds tags associated with a group index. Group indices are numbers 0, 1, 2, 3 associated with each unique set of tags in the context of the currently executed query.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesGroupToTags(group)";
    FunctionDocumentation::Arguments arguments = {{"group", "Group index associated with a time series.", {"UInt64"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Array of pairs (tag_name, tag_value).", {"Array(Tuple(String, String))"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesStoreTags(8374283493092, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS id, timeSeriesIdToTagsGroup(id) AS group, timeSeriesGroupToTags(group)", "8374283493092    0    [('__name__', ''http_requests_count''), ('env', 'dev'), ('region', 'eu')]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesGroupToTags>(documentation);
    factory.registerAlias("timeSeriesTagsGroupToTags", "timeSeriesGroupToTags");
}

}
