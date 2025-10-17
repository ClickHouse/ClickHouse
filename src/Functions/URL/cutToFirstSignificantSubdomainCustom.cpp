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
Returns the part of the domain that includes top-level subdomains up to the first significant subdomain. 
Accepts a custom TLD list name from the configuration.

This function allows you to specify a custom public suffix list, which can be useful if you need a fresh TLD list 
or have custom domain requirements. The TLD list must be configured in the ClickHouse configuration file.
    )";
    FunctionDocumentation::Syntax cutToFirstSignificantSubdomainCustom_syntax = "cutToFirstSignificantSubdomainCustom(url, tld_list_name)";
    FunctionDocumentation::Arguments cutToFirstSignificantSubdomainCustom_arguments = {
        {"url", "URL or domain string to process."},
        {"tld_list_name", "Name of the custom TLD list configured in ClickHouse."}
    };
    FunctionDocumentation::ReturnedValue cutToFirstSignificantSubdomainCustom_returned_value = {
        "Returns the part of the domain from the first significant subdomain up to the top-level domain, without 'www.', using the specified custom TLD list.", 
        {"String"}
    };
    FunctionDocumentation::Examples cutToFirstSignificantSubdomainCustom_examples = {
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
Returns the part of the domain that includes top-level subdomains up to the first significant subdomain, without stripping 'www.'. 
Accepts a custom TLD list name from the configuration.

Similar to [`cutToFirstSignificantSubdomainCustom`](#cutToFirstSignificantSubdomainCustom) but preserves the 'www.' prefix if present. This function allows you to 
specify a custom public suffix list, which can be useful if you need a fresh TLD list or have custom domain requirements.
    )";
    FunctionDocumentation::Syntax cutToFirstSignificantSubdomainCustomWithWWW_syntax = "cutToFirstSignificantSubdomainCustomWithWWW(url, tld_list_name)";
    FunctionDocumentation::Arguments cutToFirstSignificantSubdomainCustomWithWWW_arguments = {
        {"url", "URL or domain string to process."},
        {"tld_list_name", "Name of the custom TLD list configured in ClickHouse."}
    };
    FunctionDocumentation::ReturnedValue cutToFirstSignificantSubdomainCustomWithWWW_returned_value = {
        "Returns the part of the domain from the first significant subdomain up to the top-level domain, preserving 'www.', using the specified custom TLD list.", 
        {"String"}
    };
    FunctionDocumentation::Examples cutToFirstSignificantSubdomainCustomWithWWW_examples = {
    {
        "Preserving www with custom TLD list", 
        "SELECT cutToFirstSignificantSubdomainCustomWithWWW('www.foo', 'public_suffix_list')", 
        "www.foo"
    }
    };
    FunctionDocumentation::IntroducedIn cutToFirstSignificantSubdomainCustomWithWWW_introduced_in = {21, 1};
    FunctionDocumentation::Category cutToFirstSignificantSubdomainCustomWithWWW_category = FunctionDocumentation::Category::URL;
    FunctionDocumentation cutToFirstSignificantSubdomainCustomWithWWW_documentation = {cutToFirstSignificantSubdomainCustomWithWWW_description, cutToFirstSignificantSubdomainCustomWithWWW_syntax, cutToFirstSignificantSubdomainCustomWithWWW_arguments, cutToFirstSignificantSubdomainCustomWithWWW_returned_value, cutToFirstSignificantSubdomainCustomWithWWW_examples, cutToFirstSignificantSubdomainCustomWithWWW_introduced_in, cutToFirstSignificantSubdomainCustomWithWWW_category};

    factory.registerFunction<FunctionCutToFirstSignificantSubdomainCustomWithWWW>(cutToFirstSignificantSubdomainCustomWithWWW_documentation);

    FunctionDocumentation::Description cutToFirstSignificantSubdomainCustomRFC_description = R"(
Similar to [`cutToFirstSignificantSubdomainCustom`](#cutToFirstSignificantSubdomainCustom) but follows stricter rules to be compatible with [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).

This variant performs more thorough URL parsing according to RFC 3986 standards with a custom TLD list, 
which may result in lower performance compared to the standard version but provides more accurate handling of edge cases.
    )";
    FunctionDocumentation::Syntax cutToFirstSignificantSubdomainCustomRFC_syntax = "cutToFirstSignificantSubdomainCustomRFC(url, tld_list_name)";
    FunctionDocumentation::Arguments cutToFirstSignificantSubdomainCustomRFC_arguments = {
        {"url", "URL or domain string to process according to RFC 3986."},
        {"tld_list_name", "Name of the custom TLD list configured in ClickHouse."}
    };
    FunctionDocumentation::ReturnedValue cutToFirstSignificantSubdomainCustomRFC_returned_value = {
        "Returns the part of the domain from the first significant subdomain up to the top-level domain, without 'www.', using the specified custom TLD list and following RFC 3986.", 
        {"String"}
    };
    FunctionDocumentation::Examples cutToFirstSignificantSubdomainCustomRFC_examples = {
    {
        "RFC 3986 parsing with custom TLD list", 
        "SELECT cutToFirstSignificantSubdomainCustomRFC('https://subdomain.example.custom', 'public_suffix_list')", 
        "example.custom"
    }
    };
    FunctionDocumentation::IntroducedIn cutToFirstSignificantSubdomainCustomRFC_introduced_in = {22, 10};
    FunctionDocumentation::Category cutToFirstSignificantSubdomainCustomRFC_category = FunctionDocumentation::Category::URL;
    FunctionDocumentation cutToFirstSignificantSubdomainCustomRFC_documentation = {cutToFirstSignificantSubdomainCustomRFC_description, cutToFirstSignificantSubdomainCustomRFC_syntax, cutToFirstSignificantSubdomainCustomRFC_arguments, cutToFirstSignificantSubdomainCustomRFC_returned_value, cutToFirstSignificantSubdomainCustomRFC_examples, cutToFirstSignificantSubdomainCustomRFC_introduced_in, cutToFirstSignificantSubdomainCustomRFC_category};

    factory.registerFunction<FunctionCutToFirstSignificantSubdomainCustomRFC>(cutToFirstSignificantSubdomainCustomRFC_documentation);

    FunctionDocumentation::Description cutToFirstSignificantSubdomainCustomWithWWWRFC_description = R"(
Similar to [`cutToFirstSignificantSubdomainCustomWithWWW`](#cutToFirstSignificantSubdomainCustomWithWWW) but follows stricter rules to be compatible with [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).

This variant performs more thorough URL parsing according to RFC 3986 standards with a custom TLD list, 
while preserving the 'www.' prefix, which may result in lower performance compared to the standard version 
but provides more accurate handling of edge cases.
    )";
    FunctionDocumentation::Syntax cutToFirstSignificantSubdomainCustomWithWWWRFC_syntax = "cutToFirstSignificantSubdomainCustomWithWWWRFC(url, tld_list_name)";
    FunctionDocumentation::Arguments cutToFirstSignificantSubdomainCustomWithWWWRFC_arguments = {
        {"url", "URL or domain string to process according to RFC 3986."},
        {"tld_list_name", "Name of the custom TLD list configured in ClickHouse."}
    };
    FunctionDocumentation::ReturnedValue cutToFirstSignificantSubdomainCustomWithWWWRFC_returned_value = {
        "Returns the part of the domain from the first significant subdomain up to the top-level domain, preserving 'www.', using the specified custom TLD list and following RFC 3986.", 
        {"String"}
    };
    FunctionDocumentation::Examples cutToFirstSignificantSubdomainCustomWithWWWRFC_examples = {
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
