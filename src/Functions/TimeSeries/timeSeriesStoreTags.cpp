#include <Functions/FunctionFactory.h>

#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesStoreTags(id, [('tag_name_1', 'tag_value_1'), ...], 'tag_name_2', 'tag_value_2', ...) returns `id`
/// and stores the mapping between the identifier of a time series and its tags in the query context so that
/// they can later be extracted by function timeSeriesIdToTags().
class FunctionTimeSeriesStoreTags : public IFunction
{
public:
    static constexpr auto name = "timeSeriesStoreTags";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimeSeriesStoreTags>(context); }
    explicit FunctionTimeSeriesStoreTags(ContextPtr context) : tags_collector(context->getQueryContext()->getTimeSeriesTagsCollector()) {}

    String getName() const override { return name; }

    /// There should be 2 or more arguments.
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Function timeSeriesStoreTags(id, ...) always returns `id`, so it's deterministic.
    bool isDeterministic() const override { return true; }

    /// This function allows NULLs as a way to specify that some tags don't have values.
    bool useDefaultImplementationForNulls() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return arguments[0].type;
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() < 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with at least 2 arguments: {}(id, [('tag_name_1', 'tag_value_1), ...], 'tag_name_2', 'tag_value_2', ...)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForID(name, arguments, 0, /* allow_nullable = */ true);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypesForTagNamesAndValues(name, arguments, 1);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & id_type = TimeSeriesTagsFunctionHelpers::checkArgumentTypeForID(name, arguments, 0, /* allow_nullable = */ true);
        if (id_type == typeid(UInt64))
            return executeForIDType<UInt64, false>(arguments, result_type, input_rows_count);
        if (id_type == typeid(std::optional<UInt64>))
            return executeForIDType<UInt64, true>(arguments, result_type, input_rows_count);
        if (id_type == typeid(UInt128))
            return executeForIDType<UInt128, false>(arguments, result_type, input_rows_count);
        if (id_type == typeid(std::optional<UInt128>))
            return executeForIDType<UInt128, true>(arguments, result_type, input_rows_count);
        UNREACHABLE();
    }

    template <typename IDType, bool id_is_nullable>
    ColumnPtr executeForIDType(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const
    {
        auto tags_vector = TimeSeriesTagsFunctionHelpers::extractTagNamesAndValuesFromArguments(name, arguments, 1);

        if constexpr (id_is_nullable)
        {
            auto ids = TimeSeriesTagsFunctionHelpers::extractIDFromArgument<std::optional<IDType>>(name, arguments, 0);
            std::vector<IDType> valid_ids;
            valid_ids.reserve(ids.size());
            for (size_t i = 0; i != ids.size(); ++i)
            {
                if (ids[i])
                {
                    size_t offset = valid_ids.size();
                    valid_ids.emplace_back(*ids[i]);
                    tags_vector[offset] = tags_vector[i];
                }
            }
            tags_vector.resize(valid_ids.size());
            tags_collector->storeTags(valid_ids, tags_vector);
        }
        else
        {
            auto ids = TimeSeriesTagsFunctionHelpers::extractIDFromArgument<IDType>(name, arguments, 0);
            tags_collector->storeTags(ids, tags_vector);
        }

        return arguments[0].column;
    }

private:
    std::shared_ptr<ContextTimeSeriesTagsCollector> tags_collector;
};


REGISTER_FUNCTION(TimeSeriesStoreTags)
{
    FunctionDocumentation::Description description = R"(
Stores in the query context a mapping between a specified identifier of a time series and a set of tags.
Functions [timeSeriesIdToTags()](/sql-reference/functions/time-series-functions#timeSeriesIdToTags)
and [timeSeriesIdToGroup()](/sql-reference/functions/time-series-functions#timeSeriesIdToGroup)
can be used to access this mapping later during the query execution.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesStoreTags(id, tags_array, separate_tag_name_1, separate_tag_value_1, ...)";
    FunctionDocumentation::Arguments arguments = {
        {"id", "Identifier of a time series.", {"UInt64", "UInt128", "UUID", "FixedString(16)"}},
        {"tags_array", "Array of pairs (tag_name, tag_value).", {"Array(Tuple(String, String))", "NULL"}},
        {"separate_tag_name_i", "The name of a tag.", {"String", "FixedString"}},
        {"separate_tag_value_i", "The value of a tag.", {"String", "FixedString", "Nullable(String)"}}\
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the identifier of a time series (i.e. just the first argument)."
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT 8374283493092 AS id,
       timeSeriesStoreTags(id, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS same_id,
       throwIf(same_id != id),
       timeSeriesIdToTags(same_id),
       timeSeriesGroupToTags(timeSeriesIdToGroup(same_id))
        )",
        R"(
┌────────────id─┬───────same_id─┬─throwIf(notEquals(same_id, id))─┬─timeSeriesIdToTags(same_id)────────────────────────────────────────┬─timeSeriesGroupToTags(timeSeriesIdToGroup(same_id))────────────────┐
│ 8374283493092 │ 8374283493092 │                               0 │ [('__name__','http_requests_count'),('env','dev'),('region','eu')] │ [('__name__','http_requests_count'),('env','dev'),('region','eu')] │
└───────────────┴───────────────┴─────────────────────────────────┴────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesStoreTags>(documentation);
}

}
