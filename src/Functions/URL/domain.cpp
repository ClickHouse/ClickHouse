#include "domain.h"

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>

namespace DB
{

struct NameDomain { static constexpr auto name = "domain"; };
using FunctionDomain = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<false, false>>, NameDomain>;

struct NameDomainRFC { static constexpr auto name = "domainRFC"; };
using FunctionDomainRFC = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<false, true>>, NameDomainRFC>;

REGISTER_FUNCTION(Domain)
{
    factory.registerFunction<FunctionDomain>();
    factory.registerFunction<FunctionDomainRFC>();
}

}
