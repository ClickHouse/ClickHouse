#include "hasAllAny.h"
#include <Functions/FunctionFactory.h>
#include "registerFunctionsArray.h"


namespace DB
{

class FunctionArrayHasAny : public FunctionArrayHasAllAny
{
public:
    static constexpr auto name = "hasAny";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayHasAny>(context); }
    FunctionArrayHasAny(const Context & context_) : FunctionArrayHasAllAny(context_, false, name) {}
};

void registerFunctionHasAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayHasAny>();
}

}
