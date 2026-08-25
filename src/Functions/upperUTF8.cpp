#include "config.h"

#if USE_ICU

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/LowerUpperUTF8Impl.h>

namespace DB
{
namespace
{

struct NameUpperUTF8
{
    static constexpr auto name = "upperUTF8";
};

using FunctionUpperUTF8 = FunctionStringToString<LowerUpperUTF8Impl<'a', 'z', true>, NameUpperUTF8>;

}

REGISTER_FUNCTION(UpperUTF8)
{
    FunctionDocumentation::Description description = R"(
Converts a string to uppercase, assuming that the string contains valid UTF-8 encoded text.
If this assumption is violated, no exception is thrown and the result is undefined.

:::note
This function doesn't detect the language, e.g. for Turkish the result might not be exactly correct (i/İ vs. i/I).
If the length of the UTF-8 byte sequence is different for upper and lower case of a code point (such as `ẞ` and `ß`), the result may be incorrect for that code point.
:::
)";
    FunctionDocumentation::Syntax syntax = "upperUTF8(s)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "A string type.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"A String data type value.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT upperUTF8('München') AS Upperutf8",
        R"(
┌─Upperutf8─┐
│ MÜNCHEN   │
└───────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionUpperUTF8>(documentation);
}

}

#endif
