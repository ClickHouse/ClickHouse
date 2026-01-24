#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiMatchAnyImpl.h>
#include <Functions/IFunctionAdaptors.h>


namespace DB
{
namespace
{

struct NameMultiMatchAny
{
    static constexpr auto name = "multiMatchAny";
};

using FunctionMultiMatchAny = FunctionsMultiStringSearch<MultiMatchAnyImpl<NameMultiMatchAny, /*ResultType*/ UInt8, MultiMatchTraits::Find::Any, /*WithEditDistance*/ false>>;

}

REGISTER_FUNCTION(MultiMatchAny)
{
    FunctionDocumentation::Description description = R"(
Check if at least one of multiple regular expression patterns matches a haystack.

If you only want to search multiple substrings in a string, you can use function [`multiSearchAny`](#multiSearchAny) instead - it works much faster than this function.
    )";
    FunctionDocumentation::Syntax syntax = "multiMatchAny(haystack, pattern1[, pattern2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which patterns are searched.", {"String"}},
        {"pattern1[, pattern2, ...]", "An array of one or more regular expression patterns.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if any pattern matches, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Multiple pattern matching",
        "SELECT multiMatchAny('Hello World', ['Hello.*', 'foo.*'])",
        R"(
┌─multiMatchAny('Hello World', ['Hello.*', 'foo.*'])─┐
│                                                  1 │
└────────────────────────────────────────────────────┘
        )"
    },
    {
        "No patterns match",
        "SELECT multiMatchAny('Hello World', ['goodbye.*', 'foo.*'])",
        R"(
┌─multiMatchAny('Hello World', ['goodbye.*', 'foo.*'])─┐
│                                                    0 │
└──────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiMatchAny>(documentation);
}

FunctionOverloadResolverPtr createInternalMultiMatchAnyOverloadResolver(bool allow_hyperscan, size_t max_hyperscan_regexp_length, size_t max_hyperscan_regexp_total_length, bool reject_expensive_hyperscan_regexps)
{
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionMultiMatchAny>(allow_hyperscan, max_hyperscan_regexp_length, max_hyperscan_regexp_total_length, reject_expensive_hyperscan_regexps));
}

}
