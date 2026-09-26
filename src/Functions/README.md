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

For new functions, please provide in-source documentation which is used by `registerFunction`, e.g.

```cpp
    FunctionDocumentation::Description description = R"(
Returns the greatest common divisor of two values a and b.

An exception is thrown when dividing by zero or when dividing a minimal
negative number by minus one.
    )";
    FunctionDocumentation::Syntax syntax = "functionName(x, y)";
    FunctionDocumentation::Arguments arguments =
    {
        {"x", "First integer", {"(U)Int*"}},
        {"y", "Second integer", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the greatest common divisor of `x` and `y`.", {"(U)Int*"}};
    FunctionDocumentation::Examples examples =
    {
        {"Usage example", "SELECT gcd(12, 18)", "6"};
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGCD>(documentation);
```

**Notes**:

- Use `R"(...)"` for long multiline strings and examples
- Begin `returned_value` with "Returns ..."
- If an argument is optional, then begin the description with "Optional.". Do not include "(optional)" as part of the argument name.
- Include argument and return types whenever possible. For a list see [FunctionDocumentation.cpp](../Common/FunctionDocumentation.cpp).
- If multiple types are possible you can generalize them. For example instead of `UInt8`, `Int8` prefer `(U)Int8` or `(U)Int*` for all integer types.
