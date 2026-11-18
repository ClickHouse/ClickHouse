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

/// Function timeSeriesReplaceTag(tags, 'dest_tag', 'replacement', 'src_tag', 'regex')
/// matches the regular expression `regex` against the value of the tag `src_tag`.
/// If it matches, the value of the tag `dest_tag` in the returned set of tags will be the expansion of `replacement`,
/// together with the original tags in the input.
class FunctionTimeSeriesReplaceTag final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesReplaceTag";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesReplaceTag>(); }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] size_t getNumberOfArguments() const override { return 5; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3, 4}; }

    /// A NULL set of tags is treated as an empty set of tags.
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(Array(Tuple(String, String)), const String, const String, const String, const String) -> Array(Tuple(String, String))";
    }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}));
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 5)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with five arguments: {}(tags, dest_tag, replacement, src_tag, regex)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForTags(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagName(name, arguments, 1);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstString(name, arguments, 2);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagName(name, arguments, 3);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstString(name, arguments, 4);
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto tags_vector = TimeSeriesTagsFunctionHelpers::extractTagsFromArgument(name, arguments, 0);
        auto dest_tag = TimeSeriesTagsFunctionHelpers::extractConstTagNameFromArgument(name, arguments, 1);
        auto replacement = TimeSeriesTagsFunctionHelpers::extractConstStringFromArgument(name, arguments, 2);

        /// We need to allow empty src_tag here.
        /// Empty src_tag is allowed in PromQL label_replace() — Prometheus validates only the
        /// destination label name, but not the source label name, and reads a missing source label as "".
        /// Also there is a known trick - label_replace(v, new_tag_name, new_tag_value, "", "")
        /// adds a new tag with constant value.
        auto src_tag = TimeSeriesTagsFunctionHelpers::extractConstStringFromArgument(name, arguments, 3);

        auto regex = TimeSeriesTagsFunctionHelpers::extractConstStringFromArgument(name, arguments, 4);

        TimeSeriesTagsTransforms::replaceTag(tags_vector, dest_tag, replacement, src_tag, regex);
        chassert(tags_vector.size() == input_rows_count);

        return TimeSeriesTagsFunctionHelpers::makeColumnForTagNamesAndValues(tags_vector);
    }
};


REGISTER_FUNCTION(TimeSeriesReplaceTag)
{
    FunctionDocumentation::Description description = R"(
Matches the regular expression `regex` against the value of the tag `src_tag`.
If it matches, the value of the tag `dest_tag` in the returned set of tags will be the expansion of `replacement`,
together with the original tags in the input.
This function mimics the logic of the prometheus function
[label_replace()](https://prometheus.io/docs/prometheus/latest/querying/functions/#label_replace).
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesReplaceTag(tags, dest_tag, replacement, src_tag, regex)";
    FunctionDocumentation::Arguments arguments = {
        {"tags", "A set of tags.", {"Array(Tuple(String, String))"}},
        {"dest_tag", "The name of a destination tag to set in the result set of tags.", {"String"}},
        {"replacement", "A replacement pattern, can contain $1, $2 or $name to refer capturing groups in the regular expression 'regex'.", {"String"}},
        {"src_tag", "The name of a tag which value is used to match the regular expression 'regex'.", {"String"}},
        {"regex", "A regular expression.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "A new set of tags with maybe `dest_tag` added.", {"Array(Tuple(String, String))"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesReplaceTag([('__name__', 'up'), ('job', 'api-server'), ('service', 'a:c')], 'foo', '$1', 'service', '(.*):.*') AS result_tags
        )",
        R"(
┌─result_tags────────────────────────────────────────────────────────────┐
│ [('__name__','up'),('foo','a'),('job','api-server'),('service','a:c')] │
└────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesReplaceTag>(documentation);
}

}
