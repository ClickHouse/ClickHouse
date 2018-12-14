#include <Functions/arrayPop.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

class FunctionArrayPopFront : public FunctionArrayPop
{
public:
    static constexpr auto name = "arrayPopFront";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayPopFront>(); }
    FunctionArrayPopFront() : FunctionArrayPop(true, name) {}
};

void registerFunctionArrayPopFront(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPopFront>();
}

}
