#include <Functions/FunctionServerConstantBase.h>
#include <DataTypes/DataTypeString.h>
#include <Common/DNSResolver.h>

namespace DB
{
namespace
{
    constexpr char name[] = "hostName";

    /// Get the host name. Is is constant on single server, but is not constant in distributed queries.
    class FunctionHostName : public FunctionServerConstantBase<String, DataTypeString, name>
    {
    public:
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionHostName>(context); }

        explicit FunctionHostName(ContextPtr context) : FunctionServerConstantBase(context, DNSResolver::instance().getHostName()) {}
    };
}

void registerFunctionHostName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHostName>();
    factory.registerAlias("hostname", "hostName");
}

}
