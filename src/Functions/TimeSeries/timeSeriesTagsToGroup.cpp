#include <Functions/FunctionFactory.h>

#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesTagsToGroup([('tag1_name', 'tag1_value'), ...], 'tag2_name', 'tag2_value', ...)
/// returns a group assigned to the specified set of tags.
class FunctionTimeSeriesTagsToGroup : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesTagsToGroup";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesTagsToGroup>(context_); }
    explicit FunctionTimeSeriesTagsToGroup(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    /// There should be 1 or more arguments.
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Function timeSeriesTagsToGroup uses the information stored in the query context,
    /// it's deterministic in the scope of the current query.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    /// This function allows NULLs as a way to specify that some tags don't have values.
    bool useDefaultImplementationForNulls() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeUInt64>();
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() < 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with at least 1 arguments", name);

        TimeSeriesTagsFunctionHelpers::checkArgumentTypesForTagNamesAndValues(name, arguments, 0);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto tags_vector = TimeSeriesTagsFunctionHelpers::extractTagNamesAndValuesFromArguments(name, arguments, 0);

        auto & tags_collector = getContext()->getQueryContext()->getTimeSeriesTagsCollector();
        auto groups = tags_collector.getGroupForTags(tags_vector);

        chassert(groups.size() == input_rows_count);
        return TimeSeriesTagsFunctionHelpers::makeColumnForGroup(groups);
    }
};


REGISTER_FUNCTION(TimeSeriesTagsToGroup)
{
    FunctionDocumentation::Description description = R"(Returns the group assigned to a specified set of tags.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesTagsToGroup(tags_array, tag_name_1, tag_value_1, tag_name2, tag_value2, ...)";
    FunctionDocumentation::Arguments arguments = {{"tags_array", "Array of pairs (tag_name, tag_value).", {"Array(Tuple(String, String))", "NULL"}},
                                                  {"tag_name_i", "The name of a tag.", {"String", "FixedString"}},
                                                  {"tag_value_i", "The value of a tag.", {"String", "FixedString", "Nullable(String)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the group assigned to the specified set of tags.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count'), timeSeriesTagsToGroup([], '__name__', 'http_failures'), timeSeriesTagsToGroup([]), timeSeriesTagsToGroup([], '__name__', 'http_failures')", "1    2    0    2"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesTagsToGroup>(documentation);
}

}
