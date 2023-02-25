#include "domain.h"

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>

namespace DB
{

struct NameDomain { static constexpr auto name = "domain"; };
using FunctionDomain = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<false>>, NameDomain>;


REGISTER_FUNCTION(Domain)
{
    factory.registerFunction<FunctionDomain>();
}

}
