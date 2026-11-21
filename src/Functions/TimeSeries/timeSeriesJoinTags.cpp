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

/// Function timeSeriesJoinTags(tags, 'dest_tag', 'separator', ['src_tag_1', 'src_tag_2', ...])
/// joins all the values of all the `src_tags` using `separator` and returns a new set of tags with the tag `dest_tag` set to the joined value.
class FunctionTimeSeriesJoinTags final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesJoinTags";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesJoinTags>(); }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] size_t getNumberOfArguments() const override { return 4; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }

    /// A NULL set of tags is treated as an empty set of tags.
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(Array(Tuple(String, String)), const String, const String, const Array(String | IsNothing)) -> Array(Tuple(String, String))";
    }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}));
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 4)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with four arguments: {}(tags, dest_tag, separator, src_tags)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForTags(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagName(name, arguments, 1);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstString(name, arguments, 2);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagNames(name, arguments, 3);
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto tags_vector = TimeSeriesTagsFunctionHelpers::extractTagsFromArgument(name, arguments, 0);
        auto dest_tag = TimeSeriesTagsFunctionHelpers::extractConstTagNameFromArgument(name, arguments, 1);
        auto separator = TimeSeriesTagsFunctionHelpers::extractConstStringFromArgument(name, arguments, 2);
        auto src_tags = TimeSeriesTagsFunctionHelpers::extractConstTagNamesFromArgument(name, arguments, 3);

        TimeSeriesTagsTransforms::joinTags(tags_vector, dest_tag, separator, src_tags);
        chassert(tags_vector.size() == input_rows_count);

        return TimeSeriesTagsFunctionHelpers::makeColumnForTagNamesAndValues(tags_vector);
    }
};


REGISTER_FUNCTION(TimeSeriesJoinTags)
{
    FunctionDocumentation::Description description = R"(
Joins the values of specified tags extracted from a set of tags.
The function inserts a separator between joined values and returns a new set of tags
with the tag `dest_tag` set to the joined value.
This function mimics the logic of the prometheus function
[label_join()](https://prometheus.io/docs/prometheus/latest/querying/functions/#label_join).
)";
    FunctionDocumentation::Syntax syntax = "timeSeriesJoinTags(tags, dest_tag, separator, src_tags)";
    FunctionDocumentation::Arguments arguments = {
        {"tags", "A set of tags.", {"Array(Tuple(String, String))"}},
        {"dest_tag", "The name of a tag with the joined result which will be added to `tags`.", {"String"}},
        {"separator", "A separator to insert between joined values.", {"String"}},
        {"src_tags", "The names of source tags with values which will be joined.", {"Array(String)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a new set of tags with the `dest_tag` tag set to the joined result.", {"Array(Tuple(String, String))"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesJoinTags([('__name__', 'up'), ('job', 'api-server'), ('src1', 'a'), ('src2', 'b'), ('src3', 'c')], 'foo', ',', ['src1', 'src2', 'src3']) AS result_tags
        )",
        R"(
┌─result_tags─────────────────────────────────────────────────────────────────────────────────────┐
│ [('__name__','up'),('foo','a,b,c'),('job','api-server'),('src1','a'),('src2','b'),('src3','c')] │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesJoinTags>(documentation);
}

}
