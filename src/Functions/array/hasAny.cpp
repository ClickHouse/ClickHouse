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
    FunctionDocumentation::Syntax syntax = "hasAny(x, y)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Array of any type with a set of elements. [`Array`](/sql-reference/data-types/array)."},
        {"y", "Array of any type that shares a common supertype with array `y`. [`Array`](/sql-reference/data-types/array)."},
    };
    FunctionDocumentation::ReturnedValue returned_value = R"(
- `1`, if `x` and `y` have one similar element at least.
- `0`, otherwise.

Raises an exception `NO_COMMON_TYPE` if any of the elements of the two arrays do not share a common supertype.
)";
    FunctionDocumentation::Examples examples = {
        {"One array is empty", "SELECT hasAny([1], [])", "1"},
        {"Arrays containing NULL values", "SELECT hasAny([Null], [Null, 1])", "1"},
        {"Arrays containing values of a different type", "SELECT hasAny([-128, 1., 512], [1])", "1"},
        {"Arrays without a common type", "SELECT hasAny([[1, 2], [3, 4]], ['a', 'c'])", R"(
Received exception:
Code: 386. DB::Exception:
There is no supertype for types Array(UInt8), String because some of them are Array and some of them are not:
In scope SELECT hasAny([[1, 2], [3, 4]], ['a', 'c']). (NO_COMMON_TYPE)
        )"},
        {"Array of arrays", "SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [1, 2]])", "1"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayHasAny>(documentation);
}

}
