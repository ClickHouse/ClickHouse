#include "hasAllAny.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

class FunctionArrayHasAll : public FunctionArrayHasAllAny
{
public:
    static constexpr auto name = "hasAll";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayHasAll>(context); }
    FunctionArrayHasAll(const Context & context_) : FunctionArrayHasAllAny(context_, true, name) {}
};

void registerFunctionHasAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayHasAll>();
}

}
