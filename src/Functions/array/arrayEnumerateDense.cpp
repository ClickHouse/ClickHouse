#include "arrayEnumerateExtended.h"
#include <Functions/FunctionFactory.h>


namespace DB
{


class FunctionArrayEnumerateDense : public FunctionArrayEnumerateExtended<FunctionArrayEnumerateDense>
{
    using Base = FunctionArrayEnumerateExtended<FunctionArrayEnumerateDense>;
public:
    static constexpr auto name = "arrayEnumerateDense";
    using Base::create;
};

REGISTER_FUNCTION(ArrayEnumerateDense)
{
    FunctionDocumentation::Description description = "Returns an array of the same size as the source array, indicating where each element first appears in the source array.";
    FunctionDocumentation::Syntax syntax = "arrayEnumerateDense(arr)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "The array to enumerate.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of the same size as `arr`, indicating where each element first appears in the source array", {"Array(T)"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT arrayEnumerateDense([10, 20, 10, 30])", "[1,2,1,3]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {18, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayEnumerateDense>(documentation);
}

}
