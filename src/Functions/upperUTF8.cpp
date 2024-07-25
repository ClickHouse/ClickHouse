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
    FunctionDocumentation::Description description
        = R"(Converts a string to lowercase, assuming that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.)";
    FunctionDocumentation::Syntax syntax = "upperUTF8(input)";
    FunctionDocumentation::Arguments arguments = {{"input", "Input with String type"}};
    FunctionDocumentation::ReturnedValue returned_value = "A String data type value";
    FunctionDocumentation::Examples examples = {
        {"first", "SELECT upperUTF8('München') as Upperutf8;", "MÜNCHEN"},
    };
    FunctionDocumentation::Categories categories = {"String"};

    factory.registerFunction<FunctionUpperUTF8>({description, syntax, arguments, returned_value, examples, categories});
}

}

#endif
