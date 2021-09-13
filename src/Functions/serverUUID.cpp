#include <Functions/FunctionServerConstantBase.h>
#include <DataTypes/DataTypeUUID.h>
#include <Core/ServerUUID.h>

namespace DB
{

namespace
{
    constexpr char name[] = "serverUUID";

    class FunctionServerUUID : public FunctionServerConstantBase<UUID, DataTypeUUID, name>
    {
    public:
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionServerUUID>(context); }

        explicit FunctionServerUUID(ContextPtr context) : FunctionServerConstantBase(context, ServerUUID::get()) {}
    };
}

void registerFunctionServerUUID(FunctionFactory & factory)
{
    factory.registerFunction<FunctionServerUUID>();
}

}

