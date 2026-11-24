#include <Functions/FunctionFactory.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
#include <Functions/TimeSeries/TimeSeriesTagsTransforms.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesRemoveTag(tags, 'tag_name') removes a specified tag from a set of tags,
/// and returns the new set of tags.
class FunctionTimeSeriesRemoveTag final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesRemoveTag";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesRemoveTag>(); }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] size_t getNumberOfArguments() const override { return 2; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    /// A NULL set of tags is treated as an empty set of tags.
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(Array(Tuple(String, String)), const String) -> Array(Tuple(String, String))";
    }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}));
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with two arguments: {}(tags, tag_to_remove)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForTags(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagName(name, arguments, 1);
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto tags_vector = TimeSeriesTagsFunctionHelpers::extractTagsFromArgument(name, arguments, 0);
        auto tag_to_remove = TimeSeriesTagsFunctionHelpers::extractConstTagNameFromArgument(name, arguments, 1);

        TimeSeriesTagsTransforms::removeTag(tags_vector, tag_to_remove);
        chassert(tags_vector.size() == input_rows_count);

        return TimeSeriesTagsFunctionHelpers::makeColumnForTagNamesAndValues(tags_vector);
    }
};


REGISTER_FUNCTION(TimeSeriesRemoveTag)
{
    FunctionDocumentation::Description description = R"(
Removes a specified tag from a set of tags.
If there is no such tag in the set of tags then the set of tags is returned unchanged.
See also function [timeSeriesRemoveTags()](/reference/functions/regular-functions/time-series-functions#timeSeriesRemoveTags),
[timeSeriesRemoveAllTagsExcept()](/reference/functions/regular-functions/time-series-functions#timeSeriesRemoveAllTagsExcept).
)";
    FunctionDocumentation::Syntax syntax = "timeSeriesRemoveTag(tags, tag_to_remove)";
    FunctionDocumentation::Arguments arguments = {
        {"tags", "A set of tags.", {"Array(Tuple(String, String))"}},
        {"tag_to_remove", "The name of a tag to remove from the set of tags.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value = {
        "A new set of tags without the specified tag.", {"Array(Tuple(String, String))"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesRemoveTag([('__name__', 'http_requests_count'), ('env', 'dev'), ('region', 'eu')], '__name__') AS tags_of_2,
       timeSeriesRemoveTag(tags_of_2, 'env') AS tags_of_1,
       timeSeriesRemoveTag(tags_of_1, 'region') AS no_tags
        )",
        R"(
┌─tags_of_2───────────────────────┬─tags_of_1─────────┬─no_tags─┐
│ [('env','dev'),('region','eu')] │ [('region','eu')] │ []      │
└─────────────────────────────────┴───────────────────┴─────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesRemoveTag>(documentation);
}

}
