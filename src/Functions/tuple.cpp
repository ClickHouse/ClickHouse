#include <Functions/tuple.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool enable_named_columns_in_function_tuple;
}

FunctionPtr FunctionTuple::create(DB::ContextPtr context)
{
    return std::make_shared<FunctionTuple>(
        FunctionTuple::name,
        context->getSettingsRef()[Setting::enable_named_columns_in_function_tuple],
        false);
}

REGISTER_FUNCTION(Tuple)
{
    factory.registerFunction<FunctionTuple>(FunctionDocumentation{
        .description = R"(
Returns a tuple by grouping input arguments.

For columns C1, C2, ... with the types T1, T2, ..., it returns a named Tuple(C1 T1, C2 T2, ...) type tuple containing these columns if their names are unique and can be treated as unquoted identifiers, otherwise a Tuple(T1, T2, ...) is returned. There is no cost to execute the function.
Tuples are normally used as intermediate values for an argument of IN operators, or for creating a list of formal parameters of lambda functions. Tuples can't be written to a table.

The function implements the operator `(x, y, ...)`.
)",
        .syntax = "tuple([t1[, t2[ ...]])",
        .examples{{"typical", "SELECT tuple(1, 2)", "(1,2)"}},
        .introduced_in = {1, 1},
        .category = FunctionDocumentation::Category::Tuple});

    factory.registerFunction(
        "namedTuple",
        [](ContextPtr) { return std::make_shared<FunctionTuple>("namedTuple", true, true); },
        FunctionDocumentation{
        .description = R"(
Returns a named `Tuple` by grouping input arguments.

The argument names are used as tuple element names. The function throws an exception if any name is duplicated or cannot be used as an unquoted identifier. Unlike `tuple`, `namedTuple` does not depend on `enable_named_columns_in_function_tuple`, and it does not change the behavior of `tuple`.
)",
        .syntax = "namedTuple([expr AS name[, ...]])",
        .examples{{"typical", "SELECT toJSONString(namedTuple(1 AS a, 'x' AS b))", R"({"a":1,"b":"x"})"}},
        .introduced_in = {26, 6},
        .category = FunctionDocumentation::Category::Tuple});
}

}
