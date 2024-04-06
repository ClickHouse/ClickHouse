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
    factory.registerFunction<FunctionArraySymmetricDifference>(FunctionDocumentation{.description=R"(
Function arraySymmetricDifference(arr1, arr2, ..., arrN) returns an array of unique elements that are not present in all input arrays.
)", .examples{{"sum", "SELECT arraySymmetricDifference([1, 2, 3], [NULL, 2, 1]);", "[3, NULL]"}}, .categories{"Array"}});
}

}
