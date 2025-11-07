#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/domain.h>

namespace DB
{

template<bool conform_rfc>
struct ExtractTopLevelDomain
{
    static size_t getReserveLengthForElement() { return 5; }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        std::string_view host;
        if constexpr (conform_rfc)
            host = getURLHostRFC(data, size);
        else
            host = getURLHost(data, size);

        res_data = data;
        res_size = 0;

        if (!host.empty())
        {
            if (host[host.size() - 1] == '.')
                host.remove_suffix(1);

            const auto * host_end = host.data() + host.size();

            Pos last_dot = find_last_symbols_or_null<'.'>(host.data(), host_end);  /// NOLINT(bugprone-suspicious-stringview-data-usage)
            if (!last_dot)
                return;

            /// For IPv4 addresses select nothing.
            ///
            /// NOTE: it is safe to access last_dot[1]
            /// since getURLHost() will not return a host if there is symbol after dot.
            if (isNumericASCII(last_dot[1]))
                return;

            res_data = last_dot + 1;
            res_size = host_end - res_data;
        }
    }
};

struct NameTopLevelDomain { static constexpr auto name = "topLevelDomain"; };
using FunctionTopLevelDomain = FunctionStringToString<ExtractSubstringImpl<ExtractTopLevelDomain<false>>, NameTopLevelDomain>;

struct NameTopLevelDomainRFC { static constexpr auto name = "topLevelDomainRFC"; };
using FunctionTopLevelDomainRFC = FunctionStringToString<ExtractSubstringImpl<ExtractTopLevelDomain<true>>, NameTopLevelDomainRFC>;

REGISTER_FUNCTION(TopLevelDomain)
{
    /// topLevelDomain documentation
    FunctionDocumentation::Description description_topLevelDomain = R"(
Extracts the the top-level domain from a URL.

:::note
The URL can be specified with or without a protocol.
For example:

```text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://clickhouse.com/time/
```
:::
    )";
    FunctionDocumentation::Syntax syntax_topLevelDomain = "topLevelDomain(url)";
    FunctionDocumentation::Arguments arguments_topLevelDomain = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_topLevelDomain = {"Returns the domain name if the input string can be parsed as a URL. Otherwise, an empty string.", {"String"}};
    FunctionDocumentation::Examples examples_topLevelDomain = {
        {
            "Usage example",
            R"(
SELECT topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk');
            )",
            R"(
┌─topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')─┐
│ com                                                                │
└────────────────────────────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in_topLevelDomain = {1, 1};
    FunctionDocumentation::Category category_topLevelDomain = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_topLevelDomain = {description_topLevelDomain, syntax_topLevelDomain, arguments_topLevelDomain, returned_value_topLevelDomain, examples_topLevelDomain, introduced_in_topLevelDomain, category_topLevelDomain};

    factory.registerFunction<FunctionTopLevelDomain>(documentation_topLevelDomain);

    /// topLevelDomainRFC documentation
    FunctionDocumentation::Description description_topLevelDomainRFC = R"(
Extracts the the top-level domain from a URL.
Similar to [`topLevelDomain`](#topLevelDomain), but conforms to [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).
    )";
    FunctionDocumentation::Syntax syntax_topLevelDomainRFC = "topLevelDomainRFC(url)";
    FunctionDocumentation::Arguments arguments_topLevelDomainRFC = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_topLevelDomainRFC = {"Domain name if the input string can be parsed as a URL. Otherwise, an empty string.", {"String"}};
    FunctionDocumentation::Examples examples_topLevelDomainRFC = {
    {
        "Usage example",
        R"(
SELECT topLevelDomain('http://foo:foo%41bar@foo.com'), topLevelDomainRFC('http://foo:foo%41bar@foo.com');
        )",
        R"(
┌─topLevelDomain('http://foo:foo%41bar@foo.com')─┬─topLevelDomainRFC('http://foo:foo%41bar@foo.com')─┐
│                                                │ com                                               │
└────────────────────────────────────────────────┴───────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_topLevelDomainRFC = {22, 10};
    FunctionDocumentation::Category category_topLevelDomainRFC = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_topLevelDomainRFC = {description_topLevelDomainRFC, syntax_topLevelDomainRFC, arguments_topLevelDomainRFC, returned_value_topLevelDomainRFC, examples_topLevelDomainRFC, introduced_in_topLevelDomainRFC, category_topLevelDomainRFC};

    factory.registerFunction<FunctionTopLevelDomainRFC>(documentation_topLevelDomainRFC);
}

}
