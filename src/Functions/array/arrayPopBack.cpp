#include "arrayPop.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

class FunctionArrayPopBack : public FunctionArrayPop
{
public:
    static constexpr auto name = "arrayPopBack";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayPopBack>(); }
    FunctionArrayPopBack() : FunctionArrayPop(false, name) {}
};

REGISTER_FUNCTION(ArrayPopBack)
{
    FunctionDocumentation::Description description = "Removes the last element from the array.";
    FunctionDocumentation::Syntax syntax = "arrayPopBack(x)";
    FunctionDocumentation::Arguments arguments = {{"x", "The array for which to remove the last element from. [`Array`](/sql-reference/data-types/array)."}};
    FunctionDocumentation::ReturnedValue returned_value = "Returns an array identical to `x` but without the last element of `x`. [`Array`](/sql-reference/data-types/array).";
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT arrayPopBack([1, 2, 3]) AS res;", "[1,2]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayPopBack>(documentation);
}

}
