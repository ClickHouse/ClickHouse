#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/protocol.h>


namespace DB
{

struct NameProtocol { static constexpr auto name = "protocol"; };
using FunctionProtocol = FunctionStringToString<ExtractSubstringImpl<ExtractProtocol>, NameProtocol>;

REGISTER_FUNCTION(Protocol)
{
    /// protocol documentation
    FunctionDocumentation::Description description_protocol = R"(
Extracts the protocol from a URL.

Examples of typical returned values: http, https, ftp, mailto, tel, magnet.
    )";
    FunctionDocumentation::Syntax syntax_protocol = "protocol(url)";
    FunctionDocumentation::Arguments arguments_protocol = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_protocol = {"Returns the protocol of the URL, or an empty string if it cannot be determined.", {"String"}};
    FunctionDocumentation::Examples examples_protocol = {
    {
        "Usage example",
        R"(
SELECT protocol('https://clickhouse.com/');
        )",
        R"(
┌─protocol('https://clickhouse.com/')─┐
│ https                               │
└─────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_protocol = {1, 1};
    FunctionDocumentation::Category category_protocol = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_protocol = {description_protocol, syntax_protocol, arguments_protocol, returned_value_protocol, examples_protocol, introduced_in_protocol, category_protocol};

    factory.registerFunction<FunctionProtocol>(documentation_protocol);
}

}
