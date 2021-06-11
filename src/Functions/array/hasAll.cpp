#include "hasAllAny.h"
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>


namespace DB
{

class FunctionArrayHasAll : public FunctionArrayHasAllAny
{
public:
    static constexpr auto name = "hasAll";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayHasAll>(); }
    FunctionArrayHasAll() : FunctionArrayHasAllAny(GatherUtils::ArraySearchType::All, name) {}
};

void registerFunctionHasAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayHasAll>();
}

}
