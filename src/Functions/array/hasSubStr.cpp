#include "hasAllAny.h"
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>


namespace DB
{

class FunctionArrayHasSubStr : public FunctionArrayHasAllAny
{
public:
    static constexpr auto name = "hasSubStr";
    static FunctionPtr create(const Context & ) { return std::make_shared<FunctionArrayHasSubStr>(); }
    FunctionArrayHasSubStr() : FunctionArrayHasAllAny(GatherUtils::ArraySearchType::SubStr, name) {}
};

void registerFunctionHasSubStr(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayHasSubStr>();
}

}
