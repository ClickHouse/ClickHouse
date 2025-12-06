#include <Functions/array/arrayIndex.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>

namespace DB
{
struct NameHas { static constexpr auto name = "has"; };

/// has(arr, x) - whether there is an element x in the array.
using FunctionHas = FunctionArrayIndex<HasAction, NameHas>;

REGISTER_FUNCTION(Has)
{
    FunctionDocumentation::Description description
        = "Returns whether the array contains the specified element.\n\n"
          "When the first argument is a constant array and the second argument is a column or expression, "
          "`has(constant_array, column)` behaves like `column IN (constant_array)` and can use primary key "
          "and data-skipping indexes for optimization. For example, `has([1, 10, 100], id)` can leverage "
          "the primary key index if `id` is part of the `PRIMARY KEY`.\n\n"
          "This optimization also applies when the column is wrapped in monotonic functions (e.g., `has([...], toDate(ts))`).";

    FunctionDocumentation::Syntax syntax = "has(arr, x)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "The source array.", {"Array(T)"}},
        {"x", "The value to search for in the array."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the array contains the specified element, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT has([1, 2, 3], 2)", "1"},
        {"Not found", "SELECT has([1, 2, 3], 4)", "0"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHas>(documentation);
}

FunctionOverloadResolverPtr createInternalFunctionHasOverloadResolver()
{
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionHas>());
}

}
