#include "hasAllAny.h"
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>


namespace DB
{

class FunctionArrayHasAll : public FunctionArrayHasAllAny
{
public:
    static constexpr auto name = "hasAll";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayHasAll>(); }
    FunctionArrayHasAll() : FunctionArrayHasAllAny(GatherUtils::ArraySearchType::All, name) {}
};

REGISTER_FUNCTION(HasAll)
{
    factory.registerFunction<FunctionArrayHasAll>();
}

}
