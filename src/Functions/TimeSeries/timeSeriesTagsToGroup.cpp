#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>
#include <base/insertAtEnd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TIME_SERIES_TAGS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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

    /// Function timeSeriesTagsGroupToTags uses the information stored in the query context,
    /// and it's deterministic in the scope of the current query.
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

        TimeSeriesTagsHelpers::checkArgumentTypesForTagNamesAndValues(name, arguments, 0);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        chassert(arguments.size() >= 1);
        auto tags_vector = TimeSeriesTagsHelpers::extractTagNamesAndValuesFromArguments(name, arguments, 0);

        auto & tags_collector = getContext()->getQueryContext()->getTimeSeriesTagsCollector();
        auto groups = tags_collector.getGroupForTags(tags_vector);

        return TimeSeriesTagsHelpers::makeColumnForGroup(groups);
    }
};


REGISTER_FUNCTION(TimeSeriesTagsToGroup)
{
    FunctionDocumentation::Description description = R"(Returns the group assigned to a specified set of tags.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesTagsToGroup(tags_array, separate_tag_name_1, separate_tag_value_1, ...)";
    FunctionDocumentation::Arguments arguments = {{"tags_array", "Array of pairs (tag_name, tag_value).", {"Array(Tuple(String, String))", "NULL"}},
                                                  {"separate_tag_name_i", "The name of a tag.", {"String", "FixedString"}},
                                                  {"separate_tag_value_i", "The value of a tag.", {"String", "FixedString", "Nullable(String)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the group assigned to a specified set of tags.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count'), timeSeriesTagsToGroup([], '__name__', 'http_failures'), timeSeriesTagsToGroup([]), timeSeriesTagsToGroup([], '__name__', 'http_failures')", "8374283493092"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesTagsToGroup>(documentation);
}

}
