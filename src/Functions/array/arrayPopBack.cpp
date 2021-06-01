#include "arrayPop.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

class FunctionArrayPopBack : public FunctionArrayPop
{
public:
    static constexpr auto name = "arrayPopBack";
    static FunctionPtr create(ContextConstPtr) { return std::make_shared<FunctionArrayPopBack>(); }
    FunctionArrayPopBack() : FunctionArrayPop(false, name) {}
};

void registerFunctionArrayPopBack(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPopBack>();
}

}
