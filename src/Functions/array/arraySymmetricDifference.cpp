#include <Functions/FunctionFactory.h>
#include <Functions/array/arrayLogicalFunction.h>
#include <base/TypeLists.h>
#include <Common/assert_cast.h>


namespace DB
{

class FunctionArraySymmetricDifference : public FunctionArrayLogical
{
public:
    static constexpr auto name = "arraySymmetricDifference";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArraySymmetricDifference>(); }

    FunctionArraySymmetricDifference() : FunctionArrayLogical(false, name) { }
};


REGISTER_FUNCTION(ArraySymmetricDifference)
{
    factory.registerFunction<FunctionArraySymmetricDifference>();
}

}
