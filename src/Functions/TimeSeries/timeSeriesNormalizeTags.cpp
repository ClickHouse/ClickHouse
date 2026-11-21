#include <Functions/FunctionFactory.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/IFunction.h>
#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Function timeSeriesNormalizeTags([('tag_name_1', 'tag_value_1'), ...], 'tag_name_2', 'tag_value_2', ...)
/// combines an array of (tag_name, tag_value) pairs with separate tag name/value arguments into a single
/// Array(Tuple(String, String)): sorted by tag name, with duplicate tags and tags with empty or NULL values removed.
/// It handles its arguments exactly like timeSeriesTagsToGroup() and timeSeriesStoreTags() do, but it is stateless:
/// it returns the tags themselves and doesn't use the per-query tags collector.
class FunctionTimeSeriesNormalizeTags final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesNormalizeTags";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesNormalizeTags>(); }

    [[nodiscard]] String getName() const override { return name; }

    /// The first argument is the tags array, optionally followed by separate (name, value) pairs.
    [[nodiscard]] bool isVariadic() const override { return true; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 0; }

    [[nodiscard]] bool isDeterministic() const override { return true; }

    /// Allows NULLs as a way to specify that some tags don't have values.
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty())
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with at least one argument: "
                            "{}([('tag_name_1', 'tag_value_1'), ...], 'tag_name_2', 'tag_value_2', ...)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypesForTagNamesAndValues(name, arguments, 0);
        auto string_type = std::make_shared<DataTypeString>();
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{string_type, string_type}));
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const override
    {
        auto tags_vector = TimeSeriesTagsFunctionHelpers::extractTagNamesAndValuesFromArguments(name, arguments, 0);
        return TimeSeriesTagsFunctionHelpers::makeColumnForTagNamesAndValues(tags_vector);
    }
};

}


REGISTER_FUNCTION(TimeSeriesNormalizeTags)
{
    FunctionDocumentation::Description description = R"(
Combines an array of `(tag_name, tag_value)` pairs with separate tag name/value arguments into a single array of pairs.
The result is sorted by tag name, and duplicate tags and tags with empty or `NULL` values are removed.
The function treats its arguments exactly like [timeSeriesTagsToGroup()](/reference/functions/regular-functions/time-series-functions#timeSeriesTagsToGroup) does,
but it returns the tags themselves instead of a group, so it doesn't depend on the state of the query.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesNormalizeTags(tags_array, tag_name_1, tag_value_1, tag_name_2, tag_value_2, ...)";
    FunctionDocumentation::Arguments arguments = {
        {"tags_array", "Array of pairs (tag_name, tag_value).", {"Array(Tuple(String, String))", "Map(String, String)", "NULL"}},
        {"tag_name_i", "The name of a tag.", {"String", "FixedString"}},
        {"tag_value_i", "The value of a tag.", {"String", "FixedString", "Nullable(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        R"(
Returns an array of pairs `(tag_name, tag_value)` sorted by tag name, without duplicate tags or empty tag values.
        )",
        {"Array(Tuple(String, String))"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesNormalizeTags([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count', 'job', '') AS tags
        )",
        R"(
┌─tags───────────────────────────────────────────────────────────────┐
│ [('__name__','http_requests_count'),('env','dev'),('region','eu')] │
└────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesNormalizeTags>(documentation);
}

}
