#include <Functions/FunctionFactory.h>

#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesIdToGroup(id) converts the specified identifier of a time series to its group index.
/// Group indices are numbers 0, 1, 2, 3 associated with each unique set of tags in the context of the currently executed query.
class FunctionTimeSeriesIdToGroup : public IFunction
{
public:
    static constexpr auto name = "timeSeriesIdToGroup";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimeSeriesIdToGroup>(context); }
    explicit FunctionTimeSeriesIdToGroup(ContextPtr context) : tags_collector(context->getQueryContext()->getTimeSeriesTagsCollector()) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    /// Function timeSeriesIdToGroup returns information stored in the query context, it's deterministic in the scope of the current query.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeUInt64>();
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with one argument: {}(id)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForID(name, arguments, 0);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & id_type = TimeSeriesTagsFunctionHelpers::checkArgumentTypeForID(name, arguments, 0);
        if (id_type == typeid(UInt64))
            return executeForIDType<UInt64>(arguments, result_type, input_rows_count);
        if (id_type == typeid(UInt128))
            return executeForIDType<UInt128>(arguments, result_type, input_rows_count);
        UNREACHABLE();
    }

    template <typename IDType>
    ColumnPtr executeForIDType(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const
    {
        auto ids = TimeSeriesTagsFunctionHelpers::extractIDFromArgument<IDType>(name, arguments, 0);

        auto groups = tags_collector->getGroupByID(ids);
        chassert(groups.size() == input_rows_count);

        return TimeSeriesTagsFunctionHelpers::makeColumnForGroup(groups);
    }

private:
    std::shared_ptr<const ContextTimeSeriesTagsCollector> tags_collector;
};


REGISTER_FUNCTION(TimeSeriesIdToGroup)
{
    FunctionDocumentation::Description description = R"(
Returns the names and values of the tags associated with a specified identifier of a time series.
See also function [timeSeriesStoreTags()](/sql-reference/functions/time-series-functions#timeSeriesStoreTags).
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesIdToGroup(id)";
    FunctionDocumentation::Arguments arguments = {
        {"id", "Identifier of a time series.", {"UInt64", "UInt128", "UUID", "FixedString(16)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a group of tags associated with the identifier `id` of a time series.", {"UInt64"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT 8374283493092 AS id,
       timeSeriesStoreTags(id, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS same_id,
       throwIf(same_id != id),
       timeSeriesIdToGroup(same_id) AS group,
       timeSeriesGroupToTags(group)
        )",
        R"(
┌────────────id─┬───────same_id─┬─throwIf(notE⋯me_id, id))─┬─group─┬─timeSeriesGroupToTags(group)───────────────────────────────────────┐
│ 8374283493092 │ 8374283493092 │                        0 │     1 │ [('__name__','http_requests_count'),('env','dev'),('region','eu')] │
└───────────────┴───────────────┴──────────────────────────┴───────┴────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesIdToGroup>(documentation);
    factory.registerAlias("timeSeriesIdToTagsGroup", "timeSeriesIdToGroup");
}

}
