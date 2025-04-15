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
        .syntax="domainWithoutWWW(url)",
        .arguments={
            {"url", "URL. [String](../data-types/string.md)."}
        },
        .returned_value="Domain name if the input string can be parsed as a URL (without leading `www.`), otherwise an empty string. [String](../data-types/string.md).",
        .examples{{"domainWithoutWWW", "SELECT domainWithoutWWW('https://www.clickhouse.com')", ""}},
        .category=FunctionDocumentation::Category::URL
    });
    factory.registerFunction<FunctionDomainWithoutWWWRFC>(
    FunctionDocumentation{
        .description=R"(Similar to `domainWithoutWWW` but follows stricter rules to be compatible with RFC 3986 and less performant.)",
        .syntax="domainWithoutWWWRFC(url)",
        .arguments={
            {"url", "URL. [String](../data-types/string.md)."},
        },
        .returned_value="Domain name if the input string can be parsed as a URL (without leading `www.`), otherwise an empty string. [String](../data-types/string.md).",
        .examples{{
            "Usage example",
            R"(
SELECT
    domainWithoutWWW('http://user:password@www.example.com:8080/path?query=value#fragment'),
    domainWithoutWWWRFC('http://user:password@www.example.com:8080/path?query=value#fragment');
            )",
            R"(
┌─domainWithoutWWW('http://user:password@www.example.com:8080/path?query=value#fragment')─┬─domainWithoutWWWRFC('http://user:password@www.example.com:8080/path?query=value#fragment')─┐
│                                                                                         │ example.com                                                                                │
└─────────────────────────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────┘
            )"
        }},
        .category=FunctionDocumentation::Category::URL
    });
}

}
