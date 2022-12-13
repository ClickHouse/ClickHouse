#include <Functions/FunctionFactory.h>
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

REGISTER_FUNCTION(ArrayEnumerateDenseRanked)
{
    factory.registerFunction<FunctionArrayEnumerateDenseRanked>();
}

}
