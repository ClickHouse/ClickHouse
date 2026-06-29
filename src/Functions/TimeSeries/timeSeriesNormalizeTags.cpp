#include <Functions/FunctionFactory.h>

#include <Columns/ColumnMap.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
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
/// Map(String, String): sorted by tag name, with duplicate tag names and empty tag values removed.
class FunctionTimeSeriesNormalizeTags final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesNormalizeTags";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesNormalizeTags>(); }

    String getName() const override { return name; }

    /// The first argument is the tags array, optionally followed by separate (name, value) pairs.
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isDeterministic() const override { return true; }

    /// Allows NULLs as a way to specify that some tags don't have values.
    bool useDefaultImplementationForNulls() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.empty())
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with at least one argument: "
                            "{}([('tag_name_1', 'tag_value_1'), ...], 'tag_name_2', 'tag_value_2', ...)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypesForTagNamesAndValues(name, arguments, 0);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const override
    {
        auto tags_vector = TimeSeriesTagsFunctionHelpers::extractTagNamesAndValuesFromArguments(name, arguments, 0);
        return ColumnMap::create(TimeSeriesTagsFunctionHelpers::makeColumnForTagNamesAndValues(tags_vector));
    }
};

}


REGISTER_FUNCTION(TimeSeriesNormalizeTags)
{
    FunctionDocumentation::Description description = R"(
Combines an array of `(tag_name, tag_value)` pairs with separate tag name/value arguments into a single map.
The result is sorted by tag name, and duplicate tag names and tags with empty values are removed.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesNormalizeTags(tags_array, tag_name_1, tag_value_1, tag_name_2, tag_value_2, ...)";
    FunctionDocumentation::Arguments arguments = {
        {"tags_array", "Array of pairs (tag_name, tag_value).", {"Array(Tuple(String, String))", "Map(String, String)", "NULL"}},
        {"tag_name_i", "The name of a tag.", {"String", "FixedString"}},
        {"tag_value_i", "The value of a tag.", {"String", "FixedString", "Nullable(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        R"(
Returns a map of tag names to tag values, sorted by tag name, without duplicate tag names or empty tag values.
        )",
        {"Map(String, String)"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesNormalizeTags([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS tags
        )",
        R"(
┌─tags───────────────────────────────────────────────────────────┐
│ {'__name__':'http_requests_count','env':'dev','region':'eu'}    │
└─────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesNormalizeTags>(documentation);
}

}
