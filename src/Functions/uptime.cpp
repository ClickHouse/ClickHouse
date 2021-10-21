#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>


namespace DB
{

/** Returns server uptime in seconds.
  */
class FunctionUptime : public IFunction
{
public:
    static constexpr auto name = "uptime";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionUptime>(context->isDistributed(), context->getUptimeSeconds());
    }

    explicit FunctionUptime(bool is_distributed_, time_t uptime_) : is_distributed(is_distributed_), uptime(uptime_)
    {
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt32>();
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool isSuitableForConstantFolding() const override { return !is_distributed; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeUInt32().createColumnConst(input_rows_count, static_cast<UInt64>(uptime));
    }

private:
    bool is_distributed;
    time_t uptime;
};


void registerFunctionUptime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUptime>();
}

}
