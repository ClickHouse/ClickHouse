#include <Functions/array/arrayPop.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

class FunctionArrayPopFront : public FunctionArrayPop
{
public:
    static constexpr auto name = "arrayPopFront";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayPopFront>(); }
    FunctionArrayPopFront() : FunctionArrayPop(true, name) {}
};

REGISTER_FUNCTION(ArrayPopFront)
{
    FunctionDocumentation::Description description = "Removes the first item from the array.";
    FunctionDocumentation::Syntax syntax = "arrayPopFront(arr)";
    FunctionDocumentation::Arguments arguments = {{"arr", "The array for which to remove the first element from.", {"Array(T)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array identical to `arr` but without the first element of `arr`", {"Array(T)"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT arrayPopFront([1, 2, 3]) AS res;", "[2, 3]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayPopFront>(documentation);
}

}
