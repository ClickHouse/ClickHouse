#include "hasAllAny.h"
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>


namespace DB
{

class FunctionArrayHasSubstr : public FunctionArrayHasAllAny
{
public:
    static constexpr auto name = "hasSubstr";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayHasSubstr>(); }
    FunctionArrayHasSubstr() : FunctionArrayHasAllAny(GatherUtils::ArraySearchType::Substr, name) {}
};

REGISTER_FUNCTION(HasSubstr)
{
    FunctionDocumentation::Description description = R"(
Checks whether all the elements of array `x` appear in array `y` in the exact same order.
i.e the function checks whether all the elements of `x` are contained in `y` like
the `hasAll` function. In addition it checks that the elements are observed in the same order in
both `x` and `y`.

- The function will return `1` if array `y` is empty.
- `Null` is processed as a value. In other words `hasSubstr([1, 2, NULL, 3, 4], [2,3])` will return `0`. However, `hasSubstr([1, 2, NULL, 3, 4], [2,NULL,3])` will return `1`
- The order of values in both the arrays does matter.
)";
    FunctionDocumentation::Syntax syntax = "hasSubstr(x, y)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Array of any type with a set of elements. [`Array`](/sql-reference/data-types/array)."},
        {"y", "Array of any type with a set of elements. [`Array`](/sql-reference/data-types/array)."},
    };
    FunctionDocumentation::ReturnedValue returned_value = "Returns `1` if array `x` contains array `y`. Otherwise, returns `0`."
    FunctionDocumentation::Examples examples = {
        {"Both arrays are empty", "SELECT hasSubstr([], [])", "1"},
        {"Arrays containing NULL values", "SELECT hasSubstr([1, Null], [Null])", "1"},
        {"Arrays containing values of a different type", "SELECT hasSubstr([1.0, 2, 3, 4], [1, 3])", "0"},
        {"Arrays containing strings", "SELECT hasSubstr(['a', 'b'], ['a'])", "1"},
        {"Arrays with valid ordering", "SELECT hasSubstr(['a', 'b' , 'c'], ['a', 'b'])", "1"},
        {"Arrays with invalid ordering", "SELECT hasSubstr(['a', 'b' , 'c'], ['a', 'c'])", "0"},
        {"Array of arrays", "SELECT hasSubstr([[1, 2], [3, 4], [5, 6]], [[1, 2], [3, 4]])", "1"},
        {"Arrays without a common type", "SELECT hasSubstr([1, 2, NULL, 3, 4], ['a'])", R"(
Received exception:
Code: 386. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString/Enum and some of them are not:
In scope SELECT hasSubstr([1, 2, NULL, 3, 4], ['a']). (NO_COMMON_TYPE)
        )"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayHasSubstr>(documentation);
}

}
