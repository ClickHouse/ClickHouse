#include <Functions/arrayEnumerateExtended.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

/** arrayEnumerateUniq(arr)
  *  - outputs an array parallel (having same size) to this, where for each element specified
  *  how many times this element was encountered before (including this element) among elements with the same value.
  *  For example: arrayEnumerateUniq([10, 20, 10, 30]) = [1, 1, 2, 1]
  * arrayEnumerateUniq(arr1, arr2...)
  *  - for tuples from elements in the corresponding positions in several arrays.
  */
class FunctionArrayEnumerateUniq : public FunctionArrayEnumerateExtended<FunctionArrayEnumerateUniq>
{
    using Base = FunctionArrayEnumerateExtended<FunctionArrayEnumerateUniq>;
public:
    static constexpr auto name = "arrayEnumerateUniq";
    using Base::create;
};

void registerFunctionArrayEnumerateUniq(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayEnumerateUniq>();
}

}
