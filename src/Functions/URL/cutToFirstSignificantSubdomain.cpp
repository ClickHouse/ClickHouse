#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/ExtractFirstSignificantSubdomain.h>


namespace DB
{

template <bool without_www, bool conform_rfc>
struct CutToFirstSignificantSubdomain
{
    static size_t getReserveLengthForElement() { return 15; }

    static void execute(const Pos data, const size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;

        Pos tmp_data;
        size_t tmp_length;
        Pos domain_end;
        ExtractFirstSignificantSubdomain<without_www, conform_rfc>::execute(data, size, tmp_data, tmp_length, &domain_end);

        if (tmp_length == 0)
            return;

        res_data = tmp_data;
        res_size = domain_end - tmp_data;
    }
};

struct NameCutToFirstSignificantSubdomain { static constexpr auto name = "cutToFirstSignificantSubdomain"; };
using FunctionCutToFirstSignificantSubdomain = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain<true, false>>, NameCutToFirstSignificantSubdomain>;

struct NameCutToFirstSignificantSubdomainWithWWW { static constexpr auto name = "cutToFirstSignificantSubdomainWithWWW"; };
using FunctionCutToFirstSignificantSubdomainWithWWW = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain<false, false>>, NameCutToFirstSignificantSubdomainWithWWW>;

struct NameCutToFirstSignificantSubdomainRFC { static constexpr auto name = "cutToFirstSignificantSubdomainRFC"; };
using FunctionCutToFirstSignificantSubdomainRFC = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain<true, true>>, NameCutToFirstSignificantSubdomainRFC>;

struct NameCutToFirstSignificantSubdomainWithWWWRFC { static constexpr auto name = "cutToFirstSignificantSubdomainWithWWWRFC"; };
using FunctionCutToFirstSignificantSubdomainWithWWWRFC = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain<false, true>>, NameCutToFirstSignificantSubdomainWithWWWRFC>;

REGISTER_FUNCTION(CutToFirstSignificantSubdomain)
{
    FunctionDocumentation::Description cutToFirstSignificantSubdomain_description = R"(
Returns the part of the domain that includes top-level subdomains up to the "first significant subdomain".

The first significant subdomain is the subdomain one level higher than the public suffix. 
For example, the first significant subdomain of 'news.clickhouse.com' is 'clickhouse', 
because 'com' is the public suffix.

The function strips 'www.' if present.
    )";
    FunctionDocumentation::Syntax cutToFirstSignificantSubdomain_syntax = "cutToFirstSignificantSubdomain(url)";
    FunctionDocumentation::Arguments cutToFirstSignificantSubdomain_arguments = {
        {"url", "URL or domain string to process.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue cutToFirstSignificantSubdomain_returned_value = {
        "Returns the part of the domain from the first significant subdomain up to the top-level domain, without 'www.'", 
        {"String"}
    };
    FunctionDocumentation::Examples cutToFirstSignificantSubdomain_examples = {
    {
        "Strips www and returns first significant subdomain", 
        "SELECT cutToFirstSignificantSubdomain('https://www.news.bbc.co.uk/')", 
        "bbc.co.uk"
    },
    {
        "Extracts first significant subdomain from nested subdomains", 
        "SELECT cutToFirstSignificantSubdomain('https://docs.clickhouse.com/')", 
        "clickhouse.com"
    },
    {
        "Returns simple domain as is", 
        "SELECT cutToFirstSignificantSubdomain('example.org')", 
        "example.org"
    }
    };
    FunctionDocumentation::IntroducedIn cutToFirstSignificantSubdomain_introduced_in = {1, 1};
    FunctionDocumentation::Category cutToFirstSignificantSubdomain_category = FunctionDocumentation::Category::URL;
    FunctionDocumentation cutToFirstSignificantSubdomain_documentation = {cutToFirstSignificantSubdomain_description, cutToFirstSignificantSubdomain_syntax, cutToFirstSignificantSubdomain_arguments, cutToFirstSignificantSubdomain_returned_value, cutToFirstSignificantSubdomain_examples, cutToFirstSignificantSubdomain_introduced_in, cutToFirstSignificantSubdomain_category};
    
    factory.registerFunction<FunctionCutToFirstSignificantSubdomain>(cutToFirstSignificantSubdomain_documentation);

    FunctionDocumentation::Description cutToFirstSignificantSubdomainWithWWW_description = R"(
Returns the part of the domain that includes top-level subdomains up to the "first significant subdomain", without stripping 'www.'.

Similar to [`cutToFirstSignificantSubdomain`](#cutToFirstSignificantSubdomain) but preserves the 'www.' prefix if present.
    )";
    FunctionDocumentation::Syntax cutToFirstSignificantSubdomainWithWWW_syntax = "cutToFirstSignificantSubdomainWithWWW(url)";
    FunctionDocumentation::Arguments cutToFirstSignificantSubdomainWithWWW_arguments = {
        {"url", "URL or domain string to process.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue cutToFirstSignificantSubdomainWithWWW_returned_value = {
        "Returns the part of the domain from the first significant subdomain up to the top-level domain, preserving 'www.'", 
        {"String"}
    };
    FunctionDocumentation::Examples cutToFirstSignificantSubdomainWithWWW_examples = {
    {
        "Preserves www prefix when present", 
        "SELECT cutToFirstSignificantSubdomainWithWWW('https://www.news.bbc.co.uk/')", 
        "www.bbc.co.uk"
    },
    {
        "Returns domain without www when not present", 
        "SELECT cutToFirstSignificantSubdomainWithWWW('https://docs.github.com/')", 
        "github.com"
    },
    {
        "Handles simple www domains", 
        "SELECT cutToFirstSignificantSubdomainWithWWW('www.example.org')", 
        "www.example.org"
    }
    };
    FunctionDocumentation::IntroducedIn cutToFirstSignificantSubdomainWithWWW_introduced_in = {20, 12};
    FunctionDocumentation::Category cutToFirstSignificantSubdomainWithWWW_category = FunctionDocumentation::Category::URL;
    FunctionDocumentation cutToFirstSignificantSubdomainWithWWW_documentation = {cutToFirstSignificantSubdomainWithWWW_description, cutToFirstSignificantSubdomainWithWWW_syntax, cutToFirstSignificantSubdomainWithWWW_arguments, cutToFirstSignificantSubdomainWithWWW_returned_value, cutToFirstSignificantSubdomainWithWWW_examples, cutToFirstSignificantSubdomainWithWWW_introduced_in, cutToFirstSignificantSubdomainWithWWW_category};
    
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainWithWWW>(cutToFirstSignificantSubdomainWithWWW_documentation);

    FunctionDocumentation::Description cutToFirstSignificantSubdomainRFC_description = R"(
Similar to [`cutToFirstSignificantSubdomain`](#cutToFirstSignificantSubdomain) but follows stricter rules to be compatible with [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).

This variant performs more thorough URL parsing according to RFC 3986 standards, which may result in lower performance 
compared to the standard version but provides more accurate handling of edge cases.
    )";
    FunctionDocumentation::Syntax cutToFirstSignificantSubdomainRFC_syntax = "cutToFirstSignificantSubdomainRFC(url)";
    FunctionDocumentation::Arguments cutToFirstSignificantSubdomainRFC_arguments = {
        {"url", "URL or domain string to process according to RFC 3986.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue cutToFirstSignificantSubdomainRFC_returned_value = {
        "Returns the part of the domain from the first significant subdomain up to the top-level domain, without 'www.', following RFC 3986.", 
        {"String"}
    };
    FunctionDocumentation::Examples cutToFirstSignificantSubdomainRFC_examples = {
    {
        "Strips www prefix using RFC 3986 parsing", 
        "SELECT cutToFirstSignificantSubdomainRFC('https://www.news.bbc.co.uk/')", 
        "bbc.co.uk"
    },
    {
        "Handles subdomains without www", 
        "SELECT cutToFirstSignificantSubdomainRFC('https://docs.github.com/')", 
        "github.com"
    },
    {   
        "Processes simple domains", 
        "SELECT cutToFirstSignificantSubdomainRFC('example.org')", 
        "example.org"
    }
    };
    FunctionDocumentation::IntroducedIn cutToFirstSignificantSubdomainRFC_introduced_in = {22, 10};
    FunctionDocumentation::Category cutToFirstSignificantSubdomainRFC_category = FunctionDocumentation::Category::URL;
    FunctionDocumentation cutToFirstSignificantSubdomainRFC_documentation = {cutToFirstSignificantSubdomainRFC_description, cutToFirstSignificantSubdomainRFC_syntax, cutToFirstSignificantSubdomainRFC_arguments, cutToFirstSignificantSubdomainRFC_returned_value, cutToFirstSignificantSubdomainRFC_examples, cutToFirstSignificantSubdomainRFC_introduced_in, cutToFirstSignificantSubdomainRFC_category};
    
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainRFC>(cutToFirstSignificantSubdomainRFC_documentation);

    FunctionDocumentation::Description cutToFirstSignificantSubdomainWithWWWRFC_description = R"(
Similar to [`cutToFirstSignificantSubdomainWithWWW`](#cutToFirstSignificantSubdomainWithWWW) but follows stricter rules to be compatible with [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).

This variant performs more thorough URL parsing according to RFC 3986 standards, which may result in lower performance 
compared to the standard version but provides more accurate handling of edge cases, while preserving the 'www.' prefix.
    )";
    FunctionDocumentation::Syntax cutToFirstSignificantSubdomainWithWWWRFC_syntax = "cutToFirstSignificantSubdomainWithWWWRFC(url)";
    FunctionDocumentation::Arguments cutToFirstSignificantSubdomainWithWWWRFC_arguments = {
         {"url", "URL or domain string to process according to RFC 3986."}
    };
    FunctionDocumentation::ReturnedValue cutToFirstSignificantSubdomainWithWWWRFC_returned_value = {
        "Returns the part of the domain from the first significant subdomain up to the top-level domain, preserving 'www.', following RFC 3986.", 
        {"String"}
    };
    FunctionDocumentation::Examples cutToFirstSignificantSubdomainWithWWWRFC_examples = {
    {
        "Preserves www prefix with RFC 3986 parsing", 
        "SELECT cutToFirstSignificantSubdomainWithWWWRFC('https://www.news.bbc.co.uk/')", 
        "www.bbc.co.uk"
    },
    {
        "Returns domain without www using strict parsing", 
        "SELECT cutToFirstSignificantSubdomainWithWWWRFC('https://docs.github.com/')", 
        "github.com"
    },
    {
        "Handles www domains with RFC compliance", 
        "SELECT cutToFirstSignificantSubdomainWithWWWRFC('www.example.org')", 
        "www.example.org"
    }
    };
    FunctionDocumentation::IntroducedIn cutToFirstSignificantSubdomainWithWWWRFC_introduced_in = {22, 10};
    FunctionDocumentation::Category cutToFirstSignificantSubdomainWithWWWRFC_category = FunctionDocumentation::Category::URL;
    FunctionDocumentation cutToFirstSignificantSubdomainWithWWWRFC_documentation = {cutToFirstSignificantSubdomainWithWWWRFC_description, cutToFirstSignificantSubdomainWithWWWRFC_syntax, cutToFirstSignificantSubdomainWithWWWRFC_arguments, cutToFirstSignificantSubdomainWithWWWRFC_returned_value, cutToFirstSignificantSubdomainWithWWWRFC_examples, cutToFirstSignificantSubdomainWithWWWRFC_introduced_in, cutToFirstSignificantSubdomainWithWWWRFC_category};
    
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainWithWWWRFC>(cutToFirstSignificantSubdomainWithWWWRFC_documentation);
}

}
