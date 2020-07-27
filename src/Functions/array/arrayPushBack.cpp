#include "arrayPush.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

class FunctionArrayPushBack : public FunctionArrayPush
{
public:
    static constexpr auto name = "arrayPushBack";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayPushBack>(context); }
    FunctionArrayPushBack(const Context & context_) : FunctionArrayPush(context_, false, name) {}
};

void registerFunctionArrayPushBack(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPushBack>();
}

}
