#include "arrayPush.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

class FunctionArrayPushBack : public FunctionArrayPush
{
public:
    static constexpr auto name = "arrayPushBack";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayPushBack>(); }
    FunctionArrayPushBack() : FunctionArrayPush(false, name) {}
};

void registerFunctionArrayPushBack(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPushBack>();
}

}
