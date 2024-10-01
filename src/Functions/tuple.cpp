#include <Functions/tuple.h>

#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool enable_named_columns_in_function_tuple;
}

FunctionPtr FunctionTuple::create(DB::ContextPtr context)
{
    return std::make_shared<FunctionTuple>(context->getSettingsRef()[Setting::enable_named_columns_in_function_tuple]);
}

REGISTER_FUNCTION(Tuple)
{
    factory.registerFunction<FunctionTuple>(FunctionDocumentation{
        .description = R"(
Returns a tuple by grouping input arguments.

For columns C1, C2, ... with the types T1, T2, ..., it returns a named Tuple(C1 T1, C2 T2, ...) type tuple containing these columns if their names are unique and can be treated as unquoted identifiers, otherwise a Tuple(T1, T2, ...) is returned. There is no cost to execute the function.
Tuples are normally used as intermediate values for an argument of IN operators, or for creating a list of formal parameters of lambda functions. Tuples can’t be written to a table.

The function implements the operator `(x, y, ...)`.
)",
        .examples{{"typical", "SELECT tuple(1, 2)", "(1,2)"}},
        .categories{"Miscellaneous"}});
}

}
