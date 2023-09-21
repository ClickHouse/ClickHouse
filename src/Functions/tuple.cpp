#include <Functions/tuple.h>

namespace DB
{

REGISTER_FUNCTION(Tuple)
{
    factory.registerFunction<FunctionTuple<false>>(FunctionDocumentation{
        .description = R"(
Returns a tuple by grouping input arguments.

For columns with the types T1, T2, …, it returns a Tuple(T1, T2, …) type tuple containing these columns. There is no cost to execute the function.
Tuples are normally used as intermediate values for an argument of IN operators, or for creating a list of formal parameters of lambda functions. Tuples can’t be written to a table.

The function implements the operator `(x, y, …)`.
)",
        .examples{{"typical", "SELECT tuple(1, 2)", "(1,2)"}},
        .categories{"Miscellaneous"}});

    factory.registerFunction<FunctionTuple<true>>(FunctionDocumentation{
        .description = R"(
Similar to `tuple`, but returns a named tuple by grouping input arguments with their names.
)",
        .examples{{"typical", "SELECT toTypeName(namedTuple(1, 2))", "Tuple(`1` UInt8, `2` UInt8)"}},
        .categories{"Miscellaneous"}});
}

}
