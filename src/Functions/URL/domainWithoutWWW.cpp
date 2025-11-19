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
    /// domainWithoutWWW documentation
    FunctionDocumentation::Description description_domainWithoutWWW = R"(
Returns the domain of a URL without leading `www.` if present.
    )";
    FunctionDocumentation::Syntax syntax_domainWithoutWWW = "domainWithoutWWW(url)";
    FunctionDocumentation::Arguments arguments_domainWithoutWWW = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_domainWithoutWWW = {"Returns the domain name if the input string can be parsed as a URL (without leading `www.`), otherwise an empty string.", {"String"}};
    FunctionDocumentation::Examples examples_domainWithoutWWW = {
    {
        "Usage example",
        R"(
SELECT domainWithoutWWW('http://paul@www.example.com:80/');
        )",
        R"(
┌─domainWithoutWWW('http://paul@www.example.com:80/')─┐
│ example.com                                         │
└─────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_domainWithoutWWW = {1, 1};
    FunctionDocumentation::Category category_domainWithoutWWW = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_domainWithoutWWW = {description_domainWithoutWWW, syntax_domainWithoutWWW, arguments_domainWithoutWWW, returned_value_domainWithoutWWW, examples_domainWithoutWWW, introduced_in_domainWithoutWWW, category_domainWithoutWWW};

    factory.registerFunction<FunctionDomainWithoutWWW>(documentation_domainWithoutWWW);

    /// domainWithoutWWWRFC documentation
    FunctionDocumentation::Description description_domainWithoutWWWRFC = R"(
Returns the domain without leading `www.` if present. Similar to [`domainWithoutWWW`](#domainWithoutWWW) but conforms to [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).
    )";
    FunctionDocumentation::Syntax syntax_domainWithoutWWWRFC = "domainWithoutWWWRFC(url)";
    FunctionDocumentation::Arguments arguments_domainWithoutWWWRFC = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_domainWithoutWWWRFC = {"Returns the domain name if the input string can be parsed as a URL (without leading `www.`), otherwise an empty string.", {"String"}};
    FunctionDocumentation::Examples examples_domainWithoutWWWRFC = {
    {
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
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_domainWithoutWWWRFC = {1, 1};
    FunctionDocumentation::Category category_domainWithoutWWWRFC = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_domainWithoutWWWRFC = {description_domainWithoutWWWRFC, syntax_domainWithoutWWWRFC, arguments_domainWithoutWWWRFC, returned_value_domainWithoutWWWRFC, examples_domainWithoutWWWRFC, introduced_in_domainWithoutWWWRFC, category_domainWithoutWWWRFC};

    factory.registerFunction<FunctionDomainWithoutWWWRFC>(documentation_domainWithoutWWWRFC);
}

}
