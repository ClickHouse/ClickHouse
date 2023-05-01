#include "arrayEnumerateExtended.h"
#include <Functions/FunctionFactory.h>


namespace DB
{


class FunctionArrayEnumerateDense : public FunctionArrayEnumerateExtended<FunctionArrayEnumerateDense>
{
    using Base = FunctionArrayEnumerateExtended<FunctionArrayEnumerateDense>;
public:
    static constexpr auto name = "arrayEnumerateDense";
    using Base::create;
};

REGISTER_FUNCTION(ArrayEnumerateDense)
{
    factory.registerFunction<FunctionArrayEnumerateDense>();
}

}
