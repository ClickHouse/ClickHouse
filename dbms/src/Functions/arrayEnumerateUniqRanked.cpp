#include "arrayEnumerateRanked.h"
#include "Functions/FunctionFactory.h"


namespace DB
{

/** arrayEnumerateUniqRanked(start_level, arr1, depth1, arr2, depth2)
  *  - outputs an array parallel (having same size) to this, where for each element specified
  *  how many times this element was encountered before (including this element) among elements with the same value.
  */
class FunctionArrayEnumerateUniqRanked : public FunctionArrayEnumerateRankedExtended<FunctionArrayEnumerateUniqRanked>
{
    using Base = FunctionArrayEnumerateRankedExtended<FunctionArrayEnumerateUniqRanked>;
public:
    static constexpr auto name = "arrayEnumerateUniqRanked";
    using Base::create;
};

void registerFunctionArrayEnumerateUniqRanked(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayEnumerateUniqRanked>();
}

}
