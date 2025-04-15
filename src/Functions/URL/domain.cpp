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

The URL can be specified with or without a protocol. Examples:

```text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://clickhouse.com/time/
```

```text
some.svn-hosting.com
some.svn-hosting.com
clickhouse.com
```
        )",
        .syntax="domain(url)",
        .returned_value="Host name if the input string can be parsed as a URL, otherwise an empty string. String.",
        .examples{
            {
                "domain",
                "SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')",
                R"(
┌─domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')─┐
│ some.svn-hosting.com                                   │
└────────────────────────────────────────────────────────┘
                )"
            }
        },
        .category=FunctionDocumentation::Category::URL
        });

    factory.registerFunction<FunctionDomainRFC>(FunctionDocumentation
        {
        .description=R"(
Similar to `domain` but follows stricter rules to be compatible with RFC 3986 and less performant.
        )",
        .syntax="domainRFC(url)",
        .arguments={{"url", "URL. String."}},
        .returned_value="Host name if the input string can be parsed as a URL, otherwise an empty string. String.",
        .examples{
            {
                "Usage example",
                R"(
SELECT
    domain('http://user:password@example.com:8080/path?query=value#fragment'),
    domainRFC('http://user:password@example.com:8080/path?query=value#fragment');
                )",
                R"(
┌─domain('http://user:password@example.com:8080/path?query=value#fragment')─┬─domainRFC('http://user:password@example.com:8080/path?query=value#fragment')─┐
│                                                                           │ example.com                                                                  │
└───────────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────────────┘
                )"
            }
        },
        .category=FunctionDocumentation::Category::URL
        });
}

}
