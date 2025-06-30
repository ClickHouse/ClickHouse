#include "hasAllAny.h"
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>


namespace DB
{

class FunctionArrayHasAny : public FunctionArrayHasAllAny
{
public:
    static constexpr auto name = "hasAny";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayHasAny>(); }
    FunctionArrayHasAny() : FunctionArrayHasAllAny(GatherUtils::ArraySearchType::Any, name) {}
};

REGISTER_FUNCTION(HasAny)
{
    FunctionDocumentation::Description description = R"(
Checks whether two arrays have intersection by some elements.

- `Null` is processed as a value.
- The order of the values in both of the arrays does not matter.
)";
    FunctionDocumentation::Syntax syntax = "hasAny(arr_x, arr_y)";
    FunctionDocumentation::Arguments arguments = {
        {"arr_x", "Array of any type with a set of elements.", {"Array(T)"}},
        {"arr_y", "Array of any type that shares a common supertype with array `arr_x`.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {R"(
- `1`, if `arr_x` and `arr_y` have one similar element at least.
- `0`, otherwise.

Raises a `NO_COMMON_TYPE` exception if any of the elements of the two arrays do not share a common supertype.
)"};
    FunctionDocumentation::Examples examples = {
        {"One array is empty", "SELECT hasAny([1], [])", "0"},
        {"Arrays containing NULL values", "SELECT hasAny([Null], [Null, 1])", "1"},
        {"Arrays containing values of a different type", "SELECT hasAny([-128, 1., 512], [1])", "1"},
        {"Arrays without a common type", "SELECT hasAny([[1, 2], [3, 4]], ['a', 'c'])", "Raises a `NO_COMMON_TYPE` exception"},
        {"Array of arrays", "SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [1, 2]])", "1"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayHasAny>(documentation);
}

}
