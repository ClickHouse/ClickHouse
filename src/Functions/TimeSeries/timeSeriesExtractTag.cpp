#include <Functions/FunctionFactory.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesExtractTag(group, tag_to_extract) returns a Nullable(String) containing either the value of a specified tag
/// or NULL if there is no such tag in the specified group.
class FunctionTimeSeriesExtractTag : public IFunction
{
public:
    static constexpr auto name = "timeSeriesExtractTag";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimeSeriesExtractTag>(context); }
    explicit FunctionTimeSeriesExtractTag(ContextPtr context) : tags_collector(context->getQueryContext()->getTimeSeriesTagsCollector()) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    /// Function timeSeriesExtractTag returns information stored in the query context, it's deterministic in the scope of the current query.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with two arguments: {}(group, tag_to_extract)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForGroup(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstTagName(name, arguments, 1);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto groups = TimeSeriesTagsFunctionHelpers::extractGroupFromArgument(name, arguments, 0);
        auto tag_to_extract = TimeSeriesTagsFunctionHelpers::extractConstTagNameFromArgument(name, arguments, 1);

        auto tag_values = ColumnString::create();
        tags_collector->extractTag(groups, tag_to_extract, *tag_values);
        chassert(tag_values->size() == input_rows_count);

        auto null_map = ColumnUInt8::create();
        null_map->reserve(input_rows_count);
        for (size_t i = 0; i != input_rows_count; ++i)
            null_map->insertValue(tag_values->getDataAt(i).empty());

        return ColumnNullable::create(std::move(tag_values), std::move(null_map));
    }

private:
    std::shared_ptr<const ContextTimeSeriesTagsCollector> tags_collector;
};


REGISTER_FUNCTION(TimeSeriesExtractTag)
{
    FunctionDocumentation::Description description = R"(
Extracts the value of a specified tag from the group. Returns NULL if not found.
See also function [timeSeriesGroupToTags()](/sql-reference/functions/time-series-functions#timeSeriesGroupToTags).
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesExtractTag(group)";
    FunctionDocumentation::Arguments arguments = {
        {"group", "A group of tags.", {"UInt64"}},
        {"tag_to_extract", "The name of a tag to extract from the group", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the value of a specified tag.", {"Nullable(String)"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS group,
       timeSeriesExtractTag(group, '__name__'),
       timeSeriesExtractTag(group, 'env'),
       timeSeriesExtractTag(group, 'instance')
        )",
        R"(
┌─group─┬─timeSeriesExtractTag(group, '__name__')─┬─timeSeriesExtractTag(group, 'env')─┬─timeSeriesExtractTag(group, 'instance')─┐
│     1 │ http_requests_count                     │ dev                                │ ᴺᵁᴸᴸ                                    │
└───────┴─────────────────────────────────────────┴────────────────────────────────────┴─────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesExtractTag>(documentation);
}

}
