#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Core/Field.h>

namespace DB
{
class FunctionInitialQueryStartTime : public IFunction
{
    const time_t initial_query_start_time;
public:
    static constexpr auto name = "initialQueryStartTime";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionInitialQueryStartTime>(context->getClientInfo().initial_query_start_time);
    }

    explicit FunctionInitialQueryStartTime(const time_t & initial_query_start_time_) : initial_query_start_time(initial_query_start_time_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeDateTime>();
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeDateTime().createColumnConst(
                input_rows_count,
                static_cast<UInt64>(initial_query_start_time));
    }
};

REGISTER_FUNCTION(InitialQueryStartTime)
{
    factory.registerFunction<FunctionInitialQueryStartTime>(FunctionDocumentation{
        .description = "Returns the start time of the initial current query.",
        .syntax = {"initialQueryStartTime()"},
        .returned_value = "Start time of the initial query.",
        .examples = {{"simple", "SELECT initialQueryStartTime()", "2025-01-28 11:38:04"}},
        .category{"Other"},
    });
    factory.registerAlias("initial_query_start_time", FunctionInitialQueryStartTime::name, FunctionFactory::Case::Insensitive);
}
}
