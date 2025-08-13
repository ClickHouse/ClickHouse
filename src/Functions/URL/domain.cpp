#include <Functions/URL/domain.h>
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
    factory.registerFunction<FunctionDomain>(FunctionDocumentation
        {
        .description=R"(
Extracts the hostname from a URL.

The URL can be specified with or without a scheme.
If the argument can't be parsed as URL, the function returns an empty string.
        )",
        .examples{{"domain", "SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')", ""}},
        .categories{"URL"}
        });

    factory.registerFunction<FunctionDomainRFC>(FunctionDocumentation
        {
        .description=R"(Similar to `domain` but follows stricter rules to be compatible with RFC 3986 and less performant.)",
        .examples{},
        .categories{"URL"}
        });
}

}
