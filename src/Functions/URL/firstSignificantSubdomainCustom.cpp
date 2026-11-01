#include <Functions/FunctionFactory.h>
#include <Functions/URL/ExtractFirstSignificantSubdomain.h>
#include <Functions/URL/FirstSignificantSubdomainCustomImpl.h>


namespace DB
{

struct NameFirstSignificantSubdomainCustom { static constexpr auto name = "firstSignificantSubdomainCustom"; };
using FunctionFirstSignificantSubdomainCustom = FunctionCutToFirstSignificantSubdomainCustomImpl<ExtractFirstSignificantSubdomain<true, false>, NameFirstSignificantSubdomainCustom>;

struct NameFirstSignificantSubdomainCustomRFC { static constexpr auto name = "firstSignificantSubdomainCustomRFC"; };
using FunctionFirstSignificantSubdomainCustomRFC = FunctionCutToFirstSignificantSubdomainCustomImpl<ExtractFirstSignificantSubdomain<true, true>, NameFirstSignificantSubdomainCustomRFC>;

REGISTER_FUNCTION(FirstSignificantSubdomainCustom)
{
    FunctionDocumentation::Description description_custom = R"(
Returns the first significant subdomain of a URL using a custom TLD (Top-Level Domain) list. The custom TLD list name refers to a configuration that defines which domain suffixes should be treated as top-level domains. This is useful for non-standard TLD hierarchies. The function uses a simplified URL parsing algorithm that assumes the protocol and everything following are stripped.
    )";
    FunctionDocumentation::Syntax syntax_custom = "firstSignificantSubdomainCustom(url, tld_list_name)";
    FunctionDocumentation::Arguments arguments_custom = {
        {"url", "The URL to extract the subdomain from.", {"String"}},
        {"tld_list_name", "Name of the custom TLD list from the configuration.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_custom = {"Returns the first significant subdomain.", {"String"}};
    FunctionDocumentation::Examples examples_custom = {{"Basic usage", "SELECT firstSignificantSubdomainCustom('https://news.example.com', 'public_suffix_list')", "example"}};
    FunctionDocumentation::IntroducedIn introduced_in_custom = {21,1};
    FunctionDocumentation::Category category_custom = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_custom = {description_custom, syntax_custom, arguments_custom, {}, returned_value_custom, examples_custom, introduced_in_custom, category_custom};

    factory.registerFunction<FunctionFirstSignificantSubdomainCustom>(documentation_custom);

    FunctionDocumentation::Description description_custom_rfc = R"(
Similar to `firstSignificantSubdomainCustom` but uses RFC 3986 compliant URL parsing instead of the simplified algorithm.
    )";
    FunctionDocumentation::Syntax syntax_custom_rfc = "firstSignificantSubdomainCustomRFC(url, tld_list_name)";
    FunctionDocumentation::Arguments arguments_custom_rfc = {
        {"url", "The URL to extract the subdomain from.", {"String"}},
        {"tld_list_name", "Name of the custom TLD list from the configuration.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_custom_rfc = {"Returns the first significant subdomain.", {"String"}};
    FunctionDocumentation::IntroducedIn introduced_in_custom_rfc = {22,10};
    FunctionDocumentation::Examples examples_custom_rfc = {{"Basic usage", "SELECT firstSignificantSubdomainCustomRFC('https://news.example.com', 'public_suffix_list')", "example"}};
    FunctionDocumentation::Category category_custom_rfc = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_custom_rfc = {description_custom_rfc, syntax_custom_rfc, arguments_custom_rfc, {}, returned_value_custom_rfc, examples_custom_rfc, introduced_in_custom_rfc, category_custom_rfc};

    factory.registerFunction<FunctionFirstSignificantSubdomainCustomRFC>(documentation_custom_rfc);
}

}
