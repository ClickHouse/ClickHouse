#include <Functions/FunctionFactory.h>
#include <Functions/URL/ExtractFirstSignificantSubdomain.h>
#include <Functions/URL/FirstSignificantSubdomainCustomImpl.h>

namespace DB
{

template <bool without_www, bool conform_rfc>
struct CutToFirstSignificantSubdomainCustom
{
    static size_t getReserveLengthForElement() { return 15; }

    static void execute(FirstSignificantSubdomainCustomLookup & tld_lookup, const Pos data, const size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;

        Pos tmp_data;
        size_t tmp_length;
        Pos domain_end;
        ExtractFirstSignificantSubdomain<without_www, conform_rfc>::executeCustom(tld_lookup, data, size, tmp_data, tmp_length, &domain_end);

        if (tmp_length == 0)
            return;

        res_data = tmp_data;
        res_size = domain_end - tmp_data;
    }
};

struct NameCutToFirstSignificantSubdomainCustom { static constexpr auto name = "cutToFirstSignificantSubdomainCustom"; };
using FunctionCutToFirstSignificantSubdomainCustom = FunctionCutToFirstSignificantSubdomainCustomImpl<CutToFirstSignificantSubdomainCustom<true, false>, NameCutToFirstSignificantSubdomainCustom>;

struct NameCutToFirstSignificantSubdomainCustomWithWWW { static constexpr auto name = "cutToFirstSignificantSubdomainCustomWithWWW"; };
using FunctionCutToFirstSignificantSubdomainCustomWithWWW = FunctionCutToFirstSignificantSubdomainCustomImpl<CutToFirstSignificantSubdomainCustom<false, false>, NameCutToFirstSignificantSubdomainCustomWithWWW>;

struct NameCutToFirstSignificantSubdomainCustomRFC { static constexpr auto name = "cutToFirstSignificantSubdomainCustomRFC"; };
using FunctionCutToFirstSignificantSubdomainCustomRFC = FunctionCutToFirstSignificantSubdomainCustomImpl<CutToFirstSignificantSubdomainCustom<true, true>, NameCutToFirstSignificantSubdomainCustomRFC>;

struct NameCutToFirstSignificantSubdomainCustomWithWWWRFC { static constexpr auto name = "cutToFirstSignificantSubdomainCustomWithWWWRFC"; };
using FunctionCutToFirstSignificantSubdomainCustomWithWWWRFC = FunctionCutToFirstSignificantSubdomainCustomImpl<CutToFirstSignificantSubdomainCustom<false, true>, NameCutToFirstSignificantSubdomainCustomWithWWWRFC>;

REGISTER_FUNCTION(CutToFirstSignificantSubdomainCustom)
{
    FunctionDocumentation::Description cutToFirstSignificantSubdomainCustom_description = R"(
Returns the part of the domain that includes top-level subdomains up to the first significant subdomain. Accepts custom [TLD list](https://en.wikipedia.org/wiki/List_of_Internet_top-level_domains) name. This function can be useful if you need a fresh TLD list or if you have a custom list.

**Configuration example**

```yaml
<!-- <top_level_domains_path>/var/lib/clickhouse/top_level_domains/</top_level_domains_path> -->
<top_level_domains_lists>
    <!-- https://publicsuffix.org/list/public_suffix_list.dat -->
    <public_suffix_list>public_suffix_list.dat</public_suffix_list>
    <!-- NOTE: path is under top_level_domains_path -->
</top_level_domains_lists>
```
    )";
    FunctionDocumentation::Syntax cutToFirstSignificantSubdomainCustom_syntax = "cutToFirstSignificantSubdomainCustom(url, tld_list_name)";
    FunctionDocumentation::Arguments cutToFirstSignificantSubdomainCustom_arguments =
    {
        {"url", "URL or domain string to process.", {"String"}},
        {"tld_list_name", "Name of the custom TLD list configured in ClickHouse.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue cutToFirstSignificantSubdomainCustom_returned_value =
    {
        "Returns the part of the domain that includes top-level subdomains up to the first significant subdomain.",
        {"String"}
    };
    FunctionDocumentation::Examples cutToFirstSignificantSubdomainCustom_examples =
    {
    {
        "Using custom TLD list for non-standard domains",
        "SELECT cutToFirstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list')",
        "foo.there-is-no-such-domain"
    }
    };
    FunctionDocumentation::IntroducedIn cutToFirstSignificantSubdomainCustom_introduced_in = {21, 1};
    FunctionDocumentation::Category cutToFirstSignificantSubdomainCustom_category = FunctionDocumentation::Category::URL;
    FunctionDocumentation cutToFirstSignificantSubdomainCustom_documentation = {cutToFirstSignificantSubdomainCustom_description, cutToFirstSignificantSubdomainCustom_syntax, cutToFirstSignificantSubdomainCustom_arguments, cutToFirstSignificantSubdomainCustom_returned_value, cutToFirstSignificantSubdomainCustom_examples, cutToFirstSignificantSubdomainCustom_introduced_in, cutToFirstSignificantSubdomainCustom_category};
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainCustom>(cutToFirstSignificantSubdomainCustom_documentation);

    FunctionDocumentation::Description cutToFirstSignificantSubdomainCustomWithWWW_description = R"(
Returns the part of the domain that includes top-level subdomains up to the first significant subdomain without stripping 'www'. Accepts custom TLD list name. It can be useful if you need a fresh TLD list or if you have a custom list.

**Configuration example**

```yaml
<!-- <top_level_domains_path>/var/lib/clickhouse/top_level_domains/</top_level_domains_path> -->
<top_level_domains_lists>
    <!-- https://publicsuffix.org/list/public_suffix_list.dat -->
    <public_suffix_list>public_suffix_list.dat</public_suffix_list>
    <!-- NOTE: path is under top_level_domains_path -->
</top_level_domains_lists>
    )";
    FunctionDocumentation::Syntax cutToFirstSignificantSubdomainCustomWithWWW_syntax = "cutToFirstSignificantSubdomainCustomWithWWW(url, tld_list_name)";
    FunctionDocumentation::Arguments cutToFirstSignificantSubdomainCustomWithWWW_arguments =
    {
        {"url", "URL or domain string to process."},
        {"tld_list_name", "Name of the custom TLD list configured in ClickHouse."}
    };
    FunctionDocumentation::ReturnedValue cutToFirstSignificantSubdomainCustomWithWWW_returned_value =
    {
        "Part of the domain that includes top-level subdomains up to the first significant subdomain without stripping 'www'.",
        {"String"}
    };
    FunctionDocumentation::Examples cutToFirstSignificantSubdomainCustomWithWWW_examples =
    {
    {
        "Usage example",
        R"(
SELECT cutToFirstSignificantSubdomainCustomWithWWW('www.foo', 'public_suffix_list');
        )",
        R"(
┌─cutToFirstSignificantSubdomainCustomWithWWW('www.foo', 'public_suffix_list')─┐
│ www.foo                                                                      │
└──────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn cutToFirstSignificantSubdomainCustomWithWWW_introduced_in = {21, 1};
    FunctionDocumentation::Category cutToFirstSignificantSubdomainCustomWithWWW_category = FunctionDocumentation::Category::URL;
    FunctionDocumentation cutToFirstSignificantSubdomainCustomWithWWW_documentation = {cutToFirstSignificantSubdomainCustomWithWWW_description, cutToFirstSignificantSubdomainCustomWithWWW_syntax, cutToFirstSignificantSubdomainCustomWithWWW_arguments, cutToFirstSignificantSubdomainCustomWithWWW_returned_value, cutToFirstSignificantSubdomainCustomWithWWW_examples, cutToFirstSignificantSubdomainCustomWithWWW_introduced_in, cutToFirstSignificantSubdomainCustomWithWWW_category};

    factory.registerFunction<FunctionCutToFirstSignificantSubdomainCustomWithWWW>(cutToFirstSignificantSubdomainCustomWithWWW_documentation);

    FunctionDocumentation::Description cutToFirstSignificantSubdomainCustomRFC_description = R"(
Returns the part of the domain that includes top-level subdomains up to the first significant subdomain.
Accepts custom [TLD list](https://en.wikipedia.org/wiki/List_of_Internet_top-level_domains) name.
This function can be useful if you need a fresh TLD list or if you have a custom list.
Similar to [cutToFirstSignificantSubdomainCustom](#cutToFirstSignificantSubdomainCustom) but conforms to RFC 3986.

**Configuration example**

```xml
<!-- <top_level_domains_path>/var/lib/clickhouse/top_level_domains/</top_level_domains_path> -->
<top_level_domains_lists>
    <!-- https://publicsuffix.org/list/public_suffix_list.dat -->
    <public_suffix_list>public_suffix_list.dat</public_suffix_list>
    <!-- NOTE: path is under top_level_domains_path -->
</top_level_domains_lists>
```
    )";
    FunctionDocumentation::Syntax cutToFirstSignificantSubdomainCustomRFC_syntax = "cutToFirstSignificantSubdomainCustomRFC(url, tld_list_name)";
    FunctionDocumentation::Arguments cutToFirstSignificantSubdomainCustomRFC_arguments =
    {
        {"url", "URL or domain string to process according to RFC 3986."},
        {"tld_list_name", "Name of the custom TLD list configured in ClickHouse."}
    };
    FunctionDocumentation::ReturnedValue cutToFirstSignificantSubdomainCustomRFC_returned_value =
    {
        "Returns the part of the domain that includes top-level subdomains up to the first significant subdomain.",
        {"String"}
    };
    FunctionDocumentation::Examples cutToFirstSignificantSubdomainCustomRFC_examples =
    {
    {
        "Usage example",
        R"(
SELECT cutToFirstSignificantSubdomainCustomRFC('www.foo', 'public_suffix_list');
        )",
        R"(
┌─cutToFirstSignificantSubdomainCustomRFC('www.foo', 'public_suffix_list')─────┐
│ www.foo                                                                      │
└──────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn cutToFirstSignificantSubdomainCustomRFC_introduced_in = {22, 10};
    FunctionDocumentation::Category cutToFirstSignificantSubdomainCustomRFC_category = FunctionDocumentation::Category::URL;
    FunctionDocumentation cutToFirstSignificantSubdomainCustomRFC_documentation = {cutToFirstSignificantSubdomainCustomRFC_description, cutToFirstSignificantSubdomainCustomRFC_syntax, cutToFirstSignificantSubdomainCustomRFC_arguments, cutToFirstSignificantSubdomainCustomRFC_returned_value, cutToFirstSignificantSubdomainCustomRFC_examples, cutToFirstSignificantSubdomainCustomRFC_introduced_in, cutToFirstSignificantSubdomainCustomRFC_category};

    factory.registerFunction<FunctionCutToFirstSignificantSubdomainCustomRFC>(cutToFirstSignificantSubdomainCustomRFC_documentation);

    FunctionDocumentation::Description cutToFirstSignificantSubdomainCustomWithWWWRFC_description = R"(
Returns the part of the domain that includes top-level subdomains up to the first significant subdomain without stripping `www`.
Accepts custom TLD list name.
It can be useful if you need a fresh TLD list or if you have a custom list.
Similar to [cutToFirstSignificantSubdomainCustomWithWWW](#cutToFirstSignificantSubdomainCustomWithWWW) but conforms to [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).

**Configuration example**

```xml
<!-- <top_level_domains_path>/var/lib/clickhouse/top_level_domains/</top_level_domains_path> -->
<top_level_domains_lists>
    <!-- https://publicsuffix.org/list/public_suffix_list.dat -->
    <public_suffix_list>public_suffix_list.dat</public_suffix_list>
    <!-- NOTE: path is under top_level_domains_path -->
</top_level_domains_lists>
    )";
    FunctionDocumentation::Syntax cutToFirstSignificantSubdomainCustomWithWWWRFC_syntax = "cutToFirstSignificantSubdomainCustomWithWWWRFC(url, tld_list_name)";
    FunctionDocumentation::Arguments cutToFirstSignificantSubdomainCustomWithWWWRFC_arguments =
    {
        {"url", "URL or domain string to process according to RFC 3986."},
        {"tld_list_name", "Name of the custom TLD list configured in ClickHouse."}
    };
    FunctionDocumentation::ReturnedValue cutToFirstSignificantSubdomainCustomWithWWWRFC_returned_value =
    {
        "Returns the part of the domain that includes top-level subdomains up to the first significant subdomain without stripping `www`.",
        {"String"}
    };
    FunctionDocumentation::Examples cutToFirstSignificantSubdomainCustomWithWWWRFC_examples =
    {
    {
        "RFC 3986 parsing preserving www with custom TLD list",
        "SELECT cutToFirstSignificantSubdomainCustomWithWWWRFC('https://www.subdomain.example.custom', 'public_suffix_list')",
        "www.example.custom"
    }
    };
    FunctionDocumentation::IntroducedIn cutToFirstSignificantSubdomainCustomWithWWWRFC_introduced_in = {22, 10};
    FunctionDocumentation::Category cutToFirstSignificantSubdomainCustomWithWWWRFC_category = FunctionDocumentation::Category::URL;
    FunctionDocumentation cutToFirstSignificantSubdomainCustomWithWWWRFC_documentation = {cutToFirstSignificantSubdomainCustomWithWWWRFC_description, cutToFirstSignificantSubdomainCustomWithWWWRFC_syntax, cutToFirstSignificantSubdomainCustomWithWWWRFC_arguments, cutToFirstSignificantSubdomainCustomWithWWWRFC_returned_value, cutToFirstSignificantSubdomainCustomWithWWWRFC_examples, cutToFirstSignificantSubdomainCustomWithWWWRFC_introduced_in, cutToFirstSignificantSubdomainCustomWithWWWRFC_category};

    factory.registerFunction<FunctionCutToFirstSignificantSubdomainCustomWithWWWRFC>(cutToFirstSignificantSubdomainCustomWithWWWRFC_documentation);
}

}
