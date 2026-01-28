#include <Functions/tuple.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

namespace DB
{

REGISTER_FUNCTION(Tuple)
{
    factory.registerFunction<FunctionTuple>(FunctionDocumentation{
        .description = R"(
Returns a tuple by grouping input arguments.

For columns C1, C2, ... with the types T1, T2, ..., it returns a Tuple(T1, T2, ...) type tuple.

Named tuples can be created using the syntax: tuple(name1, name2, ...)(value1, value2, ...), where the first set of parentheses contains the element names as string literals, and the second set contains the corresponding values.

There is no cost to execute the function.
Tuples are normally used as intermediate values for an argument of IN operators, or for creating a list of formal parameters of lambda functions. Tuples can't be written to a table.

The function implements the operator `(x, y, ...)`.
)",
        .examples{
            {"typical", "SELECT tuple(1, 2)", "(1,2)"},
            {"named_tuple", "SELECT tuple('a', 'b')(10, 20)", R"({"a": 10, "b": 20})"}
        },
        .category = FunctionDocumentation::Category::Tuple});
}

}
