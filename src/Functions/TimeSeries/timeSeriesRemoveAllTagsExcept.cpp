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

/// Function timeSeriesRemoveAllTagsExcept(tags, ['tag_name_1', 'tag_name_2', ...])
/// removes all tags from a set of tags except specified ones, and returns the new set of tags.
class FunctionTimeSeriesRemoveAllTagsExcept final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesRemoveAllTagsExcept";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesRemoveAllTagsExcept>(); }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] size_t getNumberOfArguments() const override { return 2; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    /// A NULL set of tags is treated as an empty set of tags.
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(Array(Tuple(String, String)), const Array(String | IsNothing)) -> Array(Tuple(String, String))";
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
                            "Function {} must be called with two arguments: {}(tags, tags_to_keep)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForTags(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagNames(name, arguments, 1);
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto tags_vector = TimeSeriesTagsFunctionHelpers::extractTagsFromArgument(name, arguments, 0);
        auto tags_to_keep = TimeSeriesTagsFunctionHelpers::extractConstTagNamesFromArgument(name, arguments, 1);

        TimeSeriesTagsTransforms::removeAllTagsExcept(tags_vector, tags_to_keep);
        chassert(tags_vector.size() == input_rows_count);

        return TimeSeriesTagsFunctionHelpers::makeColumnForTagNamesAndValues(tags_vector);
    }
};


REGISTER_FUNCTION(TimeSeriesRemoveAllTagsExcept)
{
    FunctionDocumentation::Description description = R"(
Removes all tags except specified ones from a set of tags.
See also function [timeSeriesRemoveTag()](/reference/functions/regular-functions/time-series-functions#timeSeriesRemoveTag),
[timeSeriesRemoveTags()](/reference/functions/regular-functions/time-series-functions#timeSeriesRemoveTags).
)";
    FunctionDocumentation::Syntax syntax = "timeSeriesRemoveAllTagsExcept(tags, tags_to_keep)";
    FunctionDocumentation::Arguments arguments = {
        {"tags", "A set of tags.", {"Array(Tuple(String, String))"}},
        {"tags_to_keep", "The names of tags to keep in the set of tags.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "A new set of tags with only the specified tags kept.", {"Array(Tuple(String, String))"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesRemoveAllTagsExcept([('__name__', 'http_requests_count'), ('env', 'dev'), ('region', 'eu')], ['env']) AS result_tags
        )",
        R"(
┌─result_tags─────┐
│ [('env','dev')] │
└─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesRemoveAllTagsExcept>(documentation);
}

}
