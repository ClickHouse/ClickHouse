#include "hasAllAny.h"
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>


namespace DB
{

class FunctionArrayHasAny : public FunctionArrayHasAllAny
{
public:
    static constexpr auto name = "hasAny";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayHasAny>(); }
    FunctionArrayHasAny() : FunctionArrayHasAllAny(GatherUtils::ArraySearchType::Any, name) {}
};

void registerFunctionHasAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayHasAny>();
}

}
