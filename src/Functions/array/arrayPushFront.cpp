#include <Functions/array/arrayPush.h>
#include <Functions/FunctionFactory.h>


namespace DB
{


class FunctionArrayPushFront : public FunctionArrayPush
{
public:
    static constexpr auto name = "arrayPushFront";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayPushFront>(); }
    FunctionArrayPushFront() : FunctionArrayPush(true, name) {}
};


REGISTER_FUNCTION(ArrayPushFront)
{
    FunctionDocumentation::Description description = "Adds one element to the beginning of the array.";
    FunctionDocumentation::Syntax syntax = "arrayPushFront(arr, x)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "The array for which to add value `x` to the end of. [`Array(T)`](/sql-reference/data-types/array)."},
        {"x", R"(
- Single value to add to the start of the array. [`Array(T)`](/sql-reference/data-types/array).

:::note
- Only numbers can be added to an array with numbers, and only strings can be added to an array of strings.
- When adding numbers, ClickHouse automatically sets the type of `x` for the data type of the array.
- Can be `NULL`. The function adds a `NULL` element to an array, and the type of array elements converts to `Nullable`.

For more information about the types of data in ClickHouse, see [Data types](/sql-reference/data-types).
:::
    )"},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array identical to `arr` but with an additional value `x` at the beginning of the array", {"Array(T)"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT arrayPushFront(['b'], 'a') AS res;", "['a','b']"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayPushFront>(documentation);
}

}
