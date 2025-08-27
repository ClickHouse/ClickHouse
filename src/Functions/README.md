Contains implementations of regular SQL functions like `startsWith`, `base58Decode` and `formatDateTime`.

All functions are registered in FunctionFactory.h. Virtually all functions inherit from the interface IFunction. Overload resolving is
available via the interfaces in IFunctionAdaptors.h.

Functions have properties like "is_deterministic" and "injective" and they can have aliases. The latter is usually used for compatibility
with other databases like MySQL).

You will need to implement at least two methods:
- `getReturnTypeImpl` - returns the function's return type based on its argument types.
- `executeImpl` - does the actual calculation. The function arguments are provided in the form of a "chunk" (a group of rows with the
                  argument values).

When implementing `getReturnTypeImpl`, make your life easy and use `FunctionArgumentDescriptors` and `validateFunctionArguments` from
src/Functions/FunctionArgumentHelper.h.

Each input argument can be const or non-const, sparse or non-sparse, low-cardinality or non-low-cardinality. IFunction::execute takes care
of unpacking sparse and low-cardinality columns automatically (functions don't need to handle these cases). Const / non-const arguments must
be handled separately per argument. One exception for this rule exists: if _all_ arguments are const, IFunction::execute converts them to
all-non-const arguments. This is supposed to make the programmer's life a bit easier and in most cases not a performance problem as
all-const arguments are usually only used during testing (but the behavior can be surprising). For example, with three arguments, this leads
to 2 x 2 x 2 - 1 = 8 - 1 = 7 cases to take care of.

For new functions, please provide in-source documentation in `registerFunction`, e.g.

```cpp
factory.registerFunction(
    "formatQuery",
    [](ContextPtr context) { return std::make_shared<FunctionFormatQuery>(context, "formatQuery", OutputFormatting::MultiLine, ErrorHandling::Exception); },
    FunctionDocumentation{
        .description = "Returns a formatted, possibly multi-line, version of the given SQL query. Throws in case of a parsing error.\n[example:multiline]",
        .syntax = "formatQuery(query)",
        .arguments = {{"query", "The SQL query to be formatted. [String](../../sql-reference/data-types/string.md)"}},
        .returned_value = "The formatted query. [String](../../sql-reference/data-types/string.md).",
        .examples{
            {"multiline",
             "SELECT formatQuery('select a,    b FRom tab WHERE a > 3 and  b < 3');",
             "SELECT\n"
             "    a,\n"
             "    b\n"
             "FROM tab\n"
             "WHERE (a > 3) AND (b < 3)"}},
        .category{"Other"}});
```
