#include "arrayPush.h"
#include <Functions/FunctionFactory.h>


namespace DB
{


class FunctionArrayPushFront : public FunctionArrayPush
{
public:
    static constexpr auto name = "arrayPushFront";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayPushFront>(context); }
    FunctionArrayPushFront(const Context & context_) : FunctionArrayPush(context_, true, name) {}
};


void registerFunctionArrayPushFront(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPushFront>();
}

}
