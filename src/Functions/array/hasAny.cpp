#include "hasAllAny.h"
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>


namespace DB
{

class FunctionArrayHasAny : public FunctionArrayHasAllAny
{
public:
    static constexpr auto name = "hasAny";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayHasAny>(); }
    FunctionArrayHasAny() : FunctionArrayHasAllAny(GatherUtils::ArraySearchType::Any, name) {}
};

REGISTER_FUNCTION(HasAny)
{
    factory.registerFunction<FunctionArrayHasAny>();
}

}
