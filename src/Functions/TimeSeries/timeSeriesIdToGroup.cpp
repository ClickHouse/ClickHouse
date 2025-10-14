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

/// Function timeSeriesIdToGroup(<id>) converts the specified identifier of a time series to its group index.
/// Group indices are numbers 0, 1, 2, 3 associated with each unique set of tags in the context of the currently executed query.
class FunctionTimeSeriesIdToGroup : public IFunction, private WithContext
{
public:
    static constexpr auto name = "timeSeriesIdToGroup";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesIdToGroup>(context_); }
    explicit FunctionTimeSeriesIdToGroup(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    /// Function timeSeriesIdToGroup(<id>) returns the information stored in the query context by function timeSeriesStoreTags(),
    /// so it's deterministic in the scope of the current query.
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
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with two arguments", name);

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
        
        auto & tags_collector = getContext()->getQueryContext()->getTimeSeriesTagsCollector();
        auto groups = tags_collector.getGroupByID(ids);

        chassert(groups.size() == input_rows_count);
        return TimeSeriesTagsFunctionHelpers::makeColumnForGroup(groups);
    }
};


REGISTER_FUNCTION(TimeSeriesIdToGroup)
{
    FunctionDocumentation::Description description = R"(Converts the specified identifier of a time series to its group index. Group indices are numbers 0, 1, 2, 3 associated with each unique set of tags in the context of the currently executed query.)";
    FunctionDocumentation::Syntax syntax = "timeSeriesIdToGroup(id)";
    FunctionDocumentation::Arguments arguments = {{"id", "Identifier of a time series.", {"UInt64", "UInt128", "UUID", "FixedString(16)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a group index associated with this set of tags.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesStoreTags(8374283493092, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS id, timeSeriesIdToGroup(id)", "8374283493092    0"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesIdToGroup>(documentation);
    factory.registerAlias("timeSeriesIdToTagsGroup", "timeSeriesIdToGroup");
}

}
