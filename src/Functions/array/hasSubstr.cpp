#include <Functions/array/hasAllAny.h>
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
Checks whether all the elements of array2 appear in a array1 in the same exact order.
Therefore, the function will return `1`, if and only if array1 = prefix + array2 + suffix.

In other words, the functions will check whether all the elements of array2 are contained in array1 like the `hasAll` function.
In addition, it will check that the elements are observed in the same order in both array1 and array2.

- The function will return `1` if array2 is empty.
- `Null` is processed as a value. In other words `hasSubstr([1, 2, NULL, 3, 4], [2,3])` will return `0`. However, `hasSubstr([1, 2, NULL, 3, 4], [2,NULL,3])` will return `1`
- The order of values in both the arrays does matter.

Raises a `NO_COMMON_TYPE` exception if any of the elements of the two arrays do not share a common supertype.
)";
    FunctionDocumentation::Syntax syntax = "hasSubstr(arr1, arr2)";
    FunctionDocumentation::Arguments arguments = {
        {"arr1", "Array of any type with a set of elements.", {"Array(T)"}},
        {"arr2", "Array of any type with a set of elements.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if array `arr1` contains array `arr2`. Otherwise, returns `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {"Both arrays are empty", "SELECT hasSubstr([], [])", "1"},
        {"Arrays containing NULL values", "SELECT hasSubstr([1, Null], [Null])", "1"},
        {"Arrays containing values of a different type", "SELECT hasSubstr([1.0, 2, 3, 4], [1, 3])", "0"},
        {"Arrays containing strings", "SELECT hasSubstr(['a', 'b'], ['a'])", "1"},
        {"Arrays with valid ordering", "SELECT hasSubstr(['a', 'b' , 'c'], ['a', 'b'])", "1"},
        {"Arrays with invalid ordering", "SELECT hasSubstr(['a', 'b' , 'c'], ['a', 'c'])", "0"},
        {"Array of arrays", "SELECT hasSubstr([[1, 2], [3, 4], [5, 6]], [[1, 2], [3, 4]])", "1"},
        {"Arrays without a common type", "SELECT hasSubstr([1, 2, NULL, 3, 4], ['a'])", "Raises a `NO_COMMON_TYPE` exception"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayHasSubstr>(documentation);
}

}
