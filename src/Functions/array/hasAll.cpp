#include <Functions/array/hasAllAny.h>
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>


namespace DB
{

class FunctionArrayHasAll : public FunctionArrayHasAllAny
{
public:
    static constexpr auto name = "hasAll";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayHasAll>(); }
    FunctionArrayHasAll() : FunctionArrayHasAllAny(GatherUtils::ArraySearchType::All, name) {}
};

REGISTER_FUNCTION(HasAll)
{
    FunctionDocumentation::Description description = R"(
Checks whether one array is a subset of another.

- An empty array is a subset of any array.
- `Null` is processed as a value.
- The order of values in both the arrays does not matter.
)";
    FunctionDocumentation::Syntax syntax = "hasAll(set, subset)";
    FunctionDocumentation::Arguments arguments = {
        {"set", "Array of any type with a set of elements.", {"Array(T)"}},
        {"subset", "Array of any type that shares a common supertype with `set` containing elements that should be tested to be a subset of `set`.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {R"(
- `1`, if `set` contains all of the elements from `subset`.
- `0`, otherwise.

Raises a `NO_COMMON_TYPE` exception if the set and subset elements do not share a common supertype.
)"};
    FunctionDocumentation::Examples examples = {
        {"Empty arrays", "SELECT hasAll([], [])", "1"},
        {"Arrays containing NULL values", "SELECT hasAll([1, Null], [Null])", "1"},
        {"Arrays containing values of a different type", "SELECT hasAll([1.0, 2, 3, 4], [1, 3])", "1"},
        {"Arrays containing String values", "SELECT hasAll(['a', 'b'], ['a'])", "1"},
        {"Arrays without a common type", "SELECT hasAll([1], ['a'])", "Raises a NO_COMMON_TYPE exception"},
        {"Array of arrays", "SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [3, 5]])", "0"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayHasAll>(documentation);
}

}
