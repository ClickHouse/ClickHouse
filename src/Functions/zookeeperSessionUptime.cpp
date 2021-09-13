#include <Functions/FunctionServerConstantBase.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{
namespace
{
    constexpr char name[] = "zookeeperSessionUptime";

    class FunctionZooKeeperSessionUptime : public FunctionServerConstantBase<UInt32, DataTypeUInt32, name>
    {
    public:
        FunctionZooKeeperSessionUptime(ContextPtr context) : FunctionServerConstantBase(context, context->getZooKeeperSessionUptime()) {}

        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionZooKeeperSessionUptime>(context); }
    };
}

void registerFunctionZooKeeperSessionUptime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionZooKeeperSessionUptime>();
}

}
