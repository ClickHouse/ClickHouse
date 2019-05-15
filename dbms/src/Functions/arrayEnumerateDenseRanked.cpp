#include "FunctionFactory.h"
#include "arrayEnumerateRanked.h"


namespace DB
{

class FunctionArrayEnumerateDenseRanked : public FunctionArrayEnumerateRankedExtended<FunctionArrayEnumerateDenseRanked>
{
    using Base = FunctionArrayEnumerateRankedExtended<FunctionArrayEnumerateDenseRanked>;

public:
    static constexpr auto name = "arrayEnumerateDenseRanked";
    using Base::create;
};

void registerFunctionArrayEnumerateDenseRanked(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayEnumerateDenseRanked>();
}

}
