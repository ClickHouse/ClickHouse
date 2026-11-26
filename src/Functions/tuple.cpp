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

Named tuples can be created using the syntax: tuple(name1, name2, ...)(value1, value2, ...), where the first set of parentheses contains the element names as string literals, and the second set contains the corresponding values. This explicit syntax works regardless of the analyzer.

With the setting `enable_named_columns_in_function_tuple` enabled, element names are also derived automatically from argument aliases, e.g. tuple(1 AS a, 2 AS b), but this automatic naming is performed only by the analyzer (`enable_analyzer = 1`).

There is no cost to execute the function.
Tuples are normally used as intermediate values for an argument of IN operators, or for creating a list of formal parameters of lambda functions. Tuples can't be written to a table.

The function implements the operator `(x, y, ...)`.
)",
        .syntax = "tuple([t1[, t2[ ...]])",
        .examples{
            {"typical", "SELECT tuple(1, 2)", "(1,2)"},
            /// The element names live in the type, not in the value, so the value is printed the
            /// same way as for an unnamed tuple.
            {"named_tuple", "SELECT tuple('a', 'b')(10, 20)", "(10,20)"},
            {"named_tuple_names", "SELECT tupleNames(tuple('a', 'b')(10, 20))", "['a','b']"}
        },
        .introduced_in = {1, 1},
        .category = FunctionDocumentation::Category::Tuple});
}

}
