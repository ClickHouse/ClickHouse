#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "ExtractFirstSignificantSubdomain.h"


namespace DB
{

struct NameFirstSignificantSubdomain { static constexpr auto name = "firstSignificantSubdomain"; };

using FunctionFirstSignificantSubdomain = FunctionStringToString<ExtractSubstringImpl<ExtractFirstSignificantSubdomain<true>>, NameFirstSignificantSubdomain>;

REGISTER_FUNCTION(FirstSignificantSubdomain)
{
    factory.registerFunction<FunctionFirstSignificantSubdomain>();
}

}
