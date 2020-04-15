#include "hasAllAny.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

class FunctionArrayHasAny : public FunctionArrayHasAllAny
{
public:
    static constexpr auto name = "hasAny";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayHasAny>(); }
    FunctionArrayHasAny() : FunctionArrayHasAllAny(false, name) {}
};

void registerFunctionHasAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayHasAny>();
}

}
