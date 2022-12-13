#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "domain.h"

namespace DB
{

struct NameDomainWithoutWWW { static constexpr auto name = "domainWithoutWWW"; };
using FunctionDomainWithoutWWW = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<true, false>>, NameDomainWithoutWWW>;

struct NameDomainWithoutWWWRFC { static constexpr auto name = "domainWithoutWWWRFC"; };
using FunctionDomainWithoutWWWRFC = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<true, true>>, NameDomainWithoutWWWRFC>;


REGISTER_FUNCTION(DomainWithoutWWW)
{
    factory.registerFunction<FunctionDomainWithoutWWW>(
    {
        R"(
Extracts the hostname from a URL, removing the leading "www." if present.

The URL can be specified with or without a scheme.
If the argument can't be parsed as URL, the function returns an empty string.
        )",
        Documentation::Examples{{"domainWithoutWWW", "SELECT domainWithoutWWW('https://www.clickhouse.com')"}},
        Documentation::Categories{"URL"}
    });
    factory.registerFunction<FunctionDomainWithoutWWWRFC>(
    {
        R"(Similar to `domainWithoutWWW` but follows stricter rules to be compatible with RFC 3986 and less performant.)",
        Documentation::Examples{},
        Documentation::Categories{"URL"}
    });
}

}
