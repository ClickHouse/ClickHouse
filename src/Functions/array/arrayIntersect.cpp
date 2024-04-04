#include <Functions/FunctionFactory.h>
#include <Functions/array/arrayLogicalFunction.h>
#include <base/TypeLists.h>
#include <Common/assert_cast.h>


namespace DB
{

class FunctionArrayIntersect : public FunctionArrayLogical
{
public:
    static constexpr auto name = "arrayIntersect";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayIntersect>(); }

    FunctionArrayIntersect() : FunctionArrayLogical(true, name) {}
};


REGISTER_FUNCTION(ArrayIntersect)
{
        factory.registerFunction<FunctionArrayIntersect>();
}

}
