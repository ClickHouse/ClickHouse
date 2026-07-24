#include <Functions/isNotDistinctFrom.h>


namespace DB
{

REGISTER_FUNCTION(IsNotDistinctFrom)
{
    FunctionDocumentation::Description description = R"(
Performs a null-safe comparison between two `JOIN` keys. This function will consider
two `NULL` values as identical and will return `true`, which is distinct from the usual
equals behavior where comparing two `NULL` values would return `NULL`.

:::info
This function is an internal function used by the implementation of `JOIN ON`.
Please do not use it manually in queries.
:::

For a complete example see: [`NULL` values in `JOIN` keys](/sql-reference/statements/select/join#null-values-in-join-keys).
    )";
    FunctionDocumentation::Syntax syntax = "isNotDistinctFrom(x, y)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "First `JOIN` key to compare.", {"Any"}},
        {"y", "Second `JOIN` key to compare.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `true` when `x` and `y` are both `NULL`, otherwise `false`.", {"Bool"}};
    FunctionDocumentation::Examples examples = {};
    FunctionDocumentation::IntroducedIn introduced_in = {23, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Null;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIsNotDistinctFrom>(documentation);
}

}
