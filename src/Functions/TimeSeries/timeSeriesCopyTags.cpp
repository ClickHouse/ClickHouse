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

/// Function timeSeriesCopyTags(dest_tags, src_tags, ['tag_name_1', 'tag_name_2', ...])
/// copies specified tags from `src_tags` to `dest_tags`, and returns the new set of tags.
class FunctionTimeSeriesCopyTags final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesCopyTags";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesCopyTags>(); }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] size_t getNumberOfArguments() const override { return 3; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    /// A NULL set of tags is treated as an empty set of tags.
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(Array(Tuple(String, String)), Array(Tuple(String, String)), const Array(String | IsNothing)) -> Array(Tuple(String, String))";
    }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}));
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 3)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with three arguments: {}(dest_tags, src_tags, tags_to_copy)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForTags(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForTags(name, arguments, 1);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagNames(name, arguments, 2);
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto dest_tags_vector = TimeSeriesTagsFunctionHelpers::extractTagsFromArgument(name, arguments, 0);
        auto src_tags_vector = TimeSeriesTagsFunctionHelpers::extractTagsFromArgument(name, arguments, 1);
        auto tags_to_copy = TimeSeriesTagsFunctionHelpers::extractConstTagNamesFromArgument(name, arguments, 2);

        TimeSeriesTagsTransforms::copyTags(dest_tags_vector, src_tags_vector, tags_to_copy);
        chassert(dest_tags_vector.size() == input_rows_count);

        return TimeSeriesTagsFunctionHelpers::makeColumnForTagNamesAndValues(dest_tags_vector);
    }
};


REGISTER_FUNCTION(TimeSeriesCopyTags)
{
    FunctionDocumentation::Description description = R"(
Copies specified tags from one set of tags (`src_tags`) to another (`dest_tags`).
The function replaces any previous values of the copied tags in `dest_tags`.
If some of the copied tags don't present in `src_tags` then the function will remove them in `dest_tags` as well.
The function mimics the copying logic of the prometheus
[group left/group right](https://prometheus.io/docs/prometheus/latest/querying/operators/#group-modifiers) modifiers.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesCopyTags(dest_tags, src_tags, tags_to_copy)";
    FunctionDocumentation::Arguments arguments = {
        {"dest_tags", "The destination set of tags.", {"Array(Tuple(String, String))"}},
        {"src_tags", "The source set of tags.", {"Array(Tuple(String, String))"}},
        {"tags_to_copy", "The names of tags to copy.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a set of tags containing the tags from `dest_tags` along with the copied tags from `src_tags`.", {"Array(Tuple(String, String))"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
WITH [('__name__', 'http_requests_count'), ('env', 'dev'), ('region', 'eu')] AS dest_tags,
     [('__name__', 'http_codes'), ('code', '404'), ('message', 'Page not found')] AS src_tags
SELECT timeSeriesCopyTags(dest_tags, src_tags, ['__name__', 'code', 'env']) AS result_tags
        )",
        R"(
┌─result_tags────────────────────────────────────────────────┐
│ [('__name__','http_codes'),('code','404'),('region','eu')] │
└────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesCopyTags>(documentation);
}

}
