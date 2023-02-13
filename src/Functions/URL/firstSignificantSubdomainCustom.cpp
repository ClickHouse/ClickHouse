#include <Functions/FunctionFactory.h>
#include "ExtractFirstSignificantSubdomain.h"
#include "FirstSignificantSubdomainCustomImpl.h"


namespace DB
{

struct NameFirstSignificantSubdomainCustom { static constexpr auto name = "firstSignificantSubdomainCustom"; };

using FunctionFirstSignificantSubdomainCustom = FunctionCutToFirstSignificantSubdomainCustomImpl<ExtractFirstSignificantSubdomain<true>, NameFirstSignificantSubdomainCustom>;

REGISTER_FUNCTION(FirstSignificantSubdomainCustom)
{
    factory.registerFunction<FunctionFirstSignificantSubdomainCustom>();
}

}
