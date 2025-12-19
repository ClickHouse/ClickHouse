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
    /// domain documentation
    FunctionDocumentation::Description description_domain = R"(
Extracts the hostname from a URL.

The URL can be specified with or without a protocol.
    )";
    FunctionDocumentation::Syntax syntax_domain = "domain(url)";
    FunctionDocumentation::Arguments arguments_domain = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_domain = {"Returns the host name if the input string can be parsed as a URL, otherwise an empty string.", {"String"}};
    FunctionDocumentation::Examples examples_domain = {
    {
        "Usage example",
        R"(
SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk');
        )",
        R"(
┌─domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')─┐
│ some.svn-hosting.com                                   │
└────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_domain = {1, 1};
    FunctionDocumentation::Category category_domain = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_domain = {description_domain, syntax_domain, arguments_domain, returned_value_domain, examples_domain, introduced_in_domain, category_domain};

    factory.registerFunction<FunctionDomain>(documentation_domain);

    /// domainRFC documentation
    FunctionDocumentation::Description description_domainRFC = R"(
Extracts the hostname from a URL.
Similar to [`domain`](#domain), but [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986) conformant.
    )";
    FunctionDocumentation::Syntax syntax_domainRFC = "domainRFC(url)";
    FunctionDocumentation::Arguments arguments_domainRFC = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_domainRFC = {"Returns the host name if the input string can be parsed as a URL, otherwise an empty string.", {"String"}};
    FunctionDocumentation::Examples examples_domainRFC = {
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
    };
    FunctionDocumentation::IntroducedIn introduced_in_domainRFC = {22, 10};
    FunctionDocumentation::Category category_domainRFC = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_domainRFC = {description_domainRFC, syntax_domainRFC, arguments_domainRFC, returned_value_domainRFC, examples_domainRFC, introduced_in_domainRFC, category_domainRFC};

    factory.registerFunction<FunctionDomainRFC>(documentation_domainRFC);
}

}
