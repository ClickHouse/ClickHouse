#include <Functions/FunctionFactory.h>
#include <Common/assert_cast.h>
#include <base/TypeLists.h>
#include <Functions/array/arrayLogicalFunction.h>


namespace DB
{

class FunctionArraySymmetricDifference : public FunctionArrayLogical
{
public:
    static constexpr auto name = "arraySymmetricDifference";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArraySymmetricDifference>(); }
    FunctionArraySymmetricDifference() : FunctionArrayLogical(false, name) {}
};




REGISTER_FUNCTION(ArraySymmetricDifference)
{
    factory.registerFunction<FunctionArraySymmetricDifference>();
}

}
