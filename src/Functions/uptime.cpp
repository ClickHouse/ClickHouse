#include <Functions/FunctionServerConstantBase.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

namespace
{
    constexpr char name[] = "uptime";

    /// Returns server uptime in seconds.
    class FunctionUptime : public FunctionServerConstantBase<UInt32, DataTypeUInt32, name>
    {
    public:
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionUptime>(context); }

        explicit FunctionUptime(ContextPtr context) : FunctionServerConstantBase(context, context->getUptimeSeconds()) {}
    };
}

void registerFunctionUptime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUptime>();
}

}
