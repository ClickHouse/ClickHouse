#include "config.h"

#if USE_ICU

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/LowerUpperUTF8Impl.h>

namespace DB
{
namespace
{

struct NameLowerUTF8
{
    static constexpr auto name = "lowerUTF8";
};

using FunctionLowerUTF8 = FunctionStringToString<LowerUpperUTF8Impl<'A', 'Z', false>, NameLowerUTF8>;

}

REGISTER_FUNCTION(LowerUTF8)
{
    FunctionDocumentation::Description description
        = R"(Converts a string to lowercase, assuming that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.)";
    FunctionDocumentation::Syntax syntax = "lowerUTF8(input)";
    FunctionDocumentation::Arguments arguments = {{"input", "Input string to convert to lowercase.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a lowercase string.", {"String"}};
    FunctionDocumentation::Examples examples = { {"first", "SELECT lowerUTF8('München') as Lowerutf8;", "münchen"}, };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;

    factory.registerFunction<FunctionLowerUTF8>({description, syntax, arguments, returned_value, examples, introduced_in, category});
}

}

#endif
