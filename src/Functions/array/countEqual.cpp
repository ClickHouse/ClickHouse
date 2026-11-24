#include <Functions/array/arrayIndex.h>
#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool type_json_skip_null_typed_paths;
}

struct NameCountEqual { static constexpr auto name = "countEqual"; };

using FunctionCountEqual = FunctionArrayIndex<CountEqualAction, NameCountEqual>;

REGISTER_FUNCTION(CountEqual)
{
    FunctionDocumentation::Description description = R"(
Returns the number of elements in the array equal to `x`. Equivalent to `arrayCount(elem -> elem = x, arr)`.

`NULL` elements are handled as separate values.
)";
    FunctionDocumentation::Syntax syntax = "countEqual(arr, x)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "Array to search.", {"Array(T)"}},
        {"x", "Value in the array to count. Any type."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of elements in the array equal to `x`", {"UInt64"}};
    FunctionDocumentation::Examples example = {{"Usage example", "SELECT countEqual([1, 2, NULL, NULL], NULL)", "2"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, example, introduced_in, category};

    static constexpr FunctionResultSetting result_settings[] = {Setting::type_json_skip_null_typed_paths};
    factory.registerFunction(FunctionCountEqual::name, &FunctionCountEqual::create, documentation, FunctionFactory::Case::Sensitive, result_settings);
}
}
