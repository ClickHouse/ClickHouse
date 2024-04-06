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
    factory.registerFunction<FunctionArrayIntersect>(FunctionDocumentation{.description=R"(
Function arrayIntersect(arr1, arr2, ..., arrN) returns an array of unique elements that are present in all input arrays.
)", .examples{{"sum", "SELECT arrayIntersect([1, 2, 3], [NULL, 2, 1]);", "[1, 2]"}}, .categories{"Array"}});
}

}
