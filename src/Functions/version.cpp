#include <Functions/FunctionServerConstantBase.h>
#include <DataTypes/DataTypeString.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif

namespace DB
{

namespace
{
    constexpr char name[] = "version";

    /// version() - returns the current version as a string.
    class FunctionVersion : public FunctionServerConstantBase<String, DataTypeString, name>
    {
    public:
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionVersion>(context); }

        explicit FunctionVersion(ContextPtr context) : FunctionServerConstantBase(context, VERSION_STRING) {}
    };
}

void registerFunctionVersion(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVersion>(FunctionFactory::CaseInsensitive);
}

}
