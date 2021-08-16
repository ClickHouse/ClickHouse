#include "hasAllAny.h"
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>


namespace DB
{

class FunctionArrayHasSubstr : public FunctionArrayHasAllAny
{
public:
    static constexpr auto name = "hasSubstr";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayHasSubstr>(); }
    FunctionArrayHasSubstr() : FunctionArrayHasAllAny(GatherUtils::ArraySearchType::Substr, name) {}
};

void registerFunctionHasSubstr(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayHasSubstr>();
}

}
