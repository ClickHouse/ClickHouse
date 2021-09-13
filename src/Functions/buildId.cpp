#if defined(__ELF__) && !defined(__FreeBSD__)

#include <Functions/FunctionServerConstantBase.h>
#include <DataTypes/DataTypeString.h>
#include <Common/SymbolIndex.h>

namespace DB
{
namespace
{
    constexpr char name[] = "buildId";

    /// buildId() - returns the compiler build id of the running binary.
    class FunctionBuildId : public FunctionServerConstantBase<String, DataTypeString, name>
    {
    public:
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionBuildId>(context); }

        explicit FunctionBuildId(ContextPtr context) : FunctionServerConstantBase(context, SymbolIndex::instance()->getBuildIDHex()) {}
    };
}

void registerFunctionBuildId(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBuildId>();
}

}

#else

namespace DB
{
class FunctionFactory;
void registerFunctionBuildId(FunctionFactory &) {}
}

#endif
