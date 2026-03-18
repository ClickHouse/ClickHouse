#include "arrayPop.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

class FunctionArrayPopFront : public FunctionArrayPop
{
public:
    static constexpr auto name = "arrayPopFront";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayPopFront>(); }
    FunctionArrayPopFront() : FunctionArrayPop(true, name) {}
};

REGISTER_FUNCTION(ArrayPopFront)
{
    factory.registerFunction<FunctionArrayPopFront>();
}

}
