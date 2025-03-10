#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/domain.h>

namespace DB
{

struct NameDomainWithoutWWW { static constexpr auto name = "domainWithoutWWW"; };
using FunctionDomainWithoutWWW = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<true, false>>, NameDomainWithoutWWW>;

struct NameDomainWithoutWWWRFC { static constexpr auto name = "domainWithoutWWWRFC"; };
using FunctionDomainWithoutWWWRFC = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<true, true>>, NameDomainWithoutWWWRFC>;


REGISTER_FUNCTION(DomainWithoutWWW)
{
    factory.registerFunction<FunctionDomainWithoutWWW>(
    FunctionDocumentation{
        .description=R"(
Extracts the hostname from a URL, removing the leading "www." if present.

The URL can be specified with or without a scheme.
If the argument can't be parsed as URL, the function returns an empty string.
        )",
        .examples{{"domainWithoutWWW", "SELECT domainWithoutWWW('https://www.clickhouse.com')", ""}},
        .categories{"URL"}
    });
    factory.registerFunction<FunctionDomainWithoutWWWRFC>(
    FunctionDocumentation{
        .description=R"(Similar to `domainWithoutWWW` but follows stricter rules to be compatible with RFC 3986 and less performant.)",
        .examples{},
        .categories{"URL"}
    });
}

}
