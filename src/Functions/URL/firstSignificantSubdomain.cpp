#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/ExtractFirstSignificantSubdomain.h>


namespace DB
{

struct NameFirstSignificantSubdomain { static constexpr auto name = "firstSignificantSubdomain"; };
using FunctionFirstSignificantSubdomain = FunctionStringToString<ExtractSubstringImpl<ExtractFirstSignificantSubdomain<true, false>>, NameFirstSignificantSubdomain>;

struct NameFirstSignificantSubdomainRFC { static constexpr auto name = "firstSignificantSubdomainRFC"; };
using FunctionFirstSignificantSubdomainRFC = FunctionStringToString<ExtractSubstringImpl<ExtractFirstSignificantSubdomain<true, true>>, NameFirstSignificantSubdomainRFC>;

REGISTER_FUNCTION(FirstSignificantSubdomain)
{
    factory.registerFunction<FunctionFirstSignificantSubdomain>(
        FunctionDocumentation{
        .description=R"(
Returns the "first significant subdomain".

The first significant subdomain is a second-level domain if it is 'com', 'net', 'org', or 'co'.
Otherwise, it is a third-level domain.

For example, firstSignificantSubdomain('https://news.clickhouse.com/') = 'clickhouse', firstSignificantSubdomain ('https://news.clickhouse.com.tr/') = 'clickhouse'.

The list of "insignificant" second-level domains and other implementation details may change in the future.
        )",
        .syntax="firstSignificantSubdomain(url)",
        .arguments={
            {"url", "URL. [String](../../sql-reference/data-types/string.md)."}
        },
        .returned_value="The first significant subdomain. [String](../data-types/string.md).",
        .examples{{"firstSignificantSubdomain", "SELECT firstSignificantSubdomain('https://news.clickhouse.com/')", ""}},
        .category=FunctionDocumentation::Category::URL
        });

    factory.registerFunction<FunctionFirstSignificantSubdomainRFC>(
        FunctionDocumentation{
        .description=R"(Returns the "first significant subdomain" according to RFC 1034.)",
        .syntax="firstSignificantSubdomainRFC(url)",
        .arguments={{"url", "URL. [String](../../sql-reference/data-types/string.md)."}},
        .returned_value="The first significant subdomain. [String](../data-types/string.md).",
        .examples{
            {
                "Usage example",
                R"(
SELECT
    firstSignificantSubdomain('http://user:password@example.com:8080/path?query=value#fragment'),
    firstSignificantSubdomainRFC('http://user:password@example.com:8080/path?query=value#fragment');                
                )",
                R"(
┌─firstSignificantSubdomain('http://user:password@example.com:8080/path?query=value#fragment')─┬─firstSignificantSubdomainRFC('http://user:password@example.com:8080/path?query=value#fragment')─┐
│                                                                                              │ example                                                                                         │
└──────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────┘                
                )"
            }
        },
        .category=FunctionDocumentation::Category::URL
        });
}

}
