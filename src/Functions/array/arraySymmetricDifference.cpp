#include <Functions/FunctionFactory.h>
#include <Functions/array/arrayLogicalFunctionBase.h>


namespace DB
{

class FunctionArraySymmetricDifference : public FunctionArrayLogicalBase<false>
{
public:
    static constexpr auto name = "arraySymmetricDifference";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArraySymmetricDifference>(); }

    FunctionArraySymmetricDifference() : FunctionArrayLogicalBase<false>(name) { }
};


REGISTER_FUNCTION(ArraySymmetricDifference)
{
    factory.registerFunction<FunctionArraySymmetricDifference>();
}

}
