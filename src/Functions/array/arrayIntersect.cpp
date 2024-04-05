#include <Functions/FunctionFactory.h>
#include <Functions/array/arrayLogicalFunctionBase.h>


namespace DB
{
class FunctionArrayIntersect : public FunctionArrayLogicalBase<true>
{
public:
    static constexpr auto name = "arrayIntersect";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayIntersect>(); }

    FunctionArrayIntersect() : FunctionArrayLogicalBase<true>(name) { }
};


REGISTER_FUNCTION(ArrayIntersect)
{
    factory.registerFunction<FunctionArrayIntersect>();
}

}
