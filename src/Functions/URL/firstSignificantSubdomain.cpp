#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "ExtractFirstSignificantSubdomain.h"


namespace DB
{

struct NameFirstSignificantSubdomain { static constexpr auto name = "firstSignificantSubdomain"; };
using FunctionFirstSignificantSubdomain = FunctionStringToString<ExtractSubstringImpl<ExtractFirstSignificantSubdomain<true, false>>, NameFirstSignificantSubdomain>;

struct NameFirstSignificantSubdomainRFC { static constexpr auto name = "firstSignificantSubdomainRFC"; };
using FunctionFirstSignificantSubdomainRFC = FunctionStringToString<ExtractSubstringImpl<ExtractFirstSignificantSubdomain<true, true>>, NameFirstSignificantSubdomainRFC>;

REGISTER_FUNCTION(FirstSignificantSubdomain)
{
    factory.registerFunction<FunctionFirstSignificantSubdomain>();
    factory.registerFunction<FunctionFirstSignificantSubdomainRFC>();
}

}
