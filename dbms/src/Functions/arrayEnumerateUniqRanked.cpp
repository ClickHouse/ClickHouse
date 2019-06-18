#include "Functions/FunctionFactory.h"
#include "arrayEnumerateRanked.h"


namespace DB
{

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
