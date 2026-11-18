#include <Functions/FunctionFactory.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesExtractTag(tags, tag_to_extract) returns a Nullable(String) containing either the value of a specified tag
/// or NULL if there is no such tag in the specified set of tags.
class FunctionTimeSeriesExtractTag final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesExtractTag";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesExtractTag>(); }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] size_t getNumberOfArguments() const override { return 2; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    /// A NULL set of tags is treated as an empty set of tags.
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(Array(Tuple(String, String)), const String) -> Nullable(String)";
    }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with two arguments: {}(tags, tag_to_extract)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForTags(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagName(name, arguments, 1);
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto tags_vector = TimeSeriesTagsFunctionHelpers::extractTagsFromArgument(name, arguments, 0);
        auto tag_to_extract = TimeSeriesTagsFunctionHelpers::extractConstTagNameFromArgument(name, arguments, 1);
        chassert(tags_vector.size() == input_rows_count);

        auto tag_values = ColumnString::create();
        auto null_map = ColumnUInt8::create();
        tag_values->reserve(tags_vector.size());
        auto & null_map_data = null_map->getData();
        null_map_data.resize(tags_vector.size());

        for (size_t i = 0; i != tags_vector.size(); ++i)
        {
            bool found = false;
            for (const auto & [tag_name, tag_value] : *tags_vector[i])
            {
                if (tag_name == tag_to_extract)
                {
                    tag_values->insertData(tag_value.data(), tag_value.size());
                    found = true;
                    break;
                }
            }
            if (!found)
                tag_values->insertDefault();
            null_map_data[i] = !found;
        }

        return ColumnNullable::create(std::move(tag_values), std::move(null_map));
    }
};


REGISTER_FUNCTION(TimeSeriesExtractTag)
{
    FunctionDocumentation::Description description = R"(
Extracts the value of a specified tag from a set of tags. Returns NULL if not found.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesExtractTag(tags, tag_to_extract)";
    FunctionDocumentation::Arguments arguments = {
        {"tags", "A set of tags.", {"Array(Tuple(String, String))"}},
        {"tag_to_extract", "The name of a tag to extract from the set of tags.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the value of a specified tag.", {"Nullable(String)"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
WITH [('__name__', 'http_requests_count'), ('env', 'dev'), ('region', 'eu')] AS tags
SELECT timeSeriesExtractTag(tags, '__name__'),
       timeSeriesExtractTag(tags, 'env'),
       timeSeriesExtractTag(tags, 'instance')
        )",
        R"(
┌─timeSeriesExtractTag(tags, '__name__')─┬─timeSeriesExtractTag(tags, 'env')─┬─timeSeriesExtractTag(tags, 'instance')─┐
│ http_requests_count                    │ dev                               │ ᴺᵁᴸᴸ                                   │
└────────────────────────────────────────┴───────────────────────────────────┴────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesExtractTag>(documentation);
}

}
