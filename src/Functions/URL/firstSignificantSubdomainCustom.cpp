#include <Functions/FunctionFactory.h>
#include "ExtractFirstSignificantSubdomain.h"
#include "FirstSignificantSubdomainCustomImpl.h"


namespace DB
{

struct NameFirstSignificantSubdomainCustom { static constexpr auto name = "firstSignificantSubdomainCustom"; };
using FunctionFirstSignificantSubdomainCustom = FunctionCutToFirstSignificantSubdomainCustomImpl<ExtractFirstSignificantSubdomain<true, false>, NameFirstSignificantSubdomainCustom>;

struct NameFirstSignificantSubdomainCustomRFC { static constexpr auto name = "firstSignificantSubdomainCustomRFC"; };
using FunctionFirstSignificantSubdomainCustomRFC = FunctionCutToFirstSignificantSubdomainCustomImpl<ExtractFirstSignificantSubdomain<true, true>, NameFirstSignificantSubdomainCustomRFC>;

REGISTER_FUNCTION(FirstSignificantSubdomainCustom)
{
    factory.registerFunction<FunctionFirstSignificantSubdomainCustom>();
    factory.registerFunction<FunctionFirstSignificantSubdomainCustomRFC>();
}

}
