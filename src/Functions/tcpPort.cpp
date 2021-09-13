#include <Functions/FunctionServerConstantBase.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

namespace
{
    constexpr char name[] = "tcpPort";

    class FunctionTcpPort : public FunctionServerConstantBase<UInt16, DataTypeUInt16, name>
    {
    public:
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTcpPort>(context); }

        explicit FunctionTcpPort(ContextPtr context) : FunctionServerConstantBase(context, context->getTCPPort()) {}
    };
}

void registerFunctionTcpPort(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTcpPort>();
}

}
