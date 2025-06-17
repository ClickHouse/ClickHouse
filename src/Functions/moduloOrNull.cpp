#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{

struct NameModuloOrNull { static constexpr auto name = "moduloOrNull"; };
using FunctionModuloOrNull = BinaryArithmeticOverloadResolver<ModuloOrNullImpl, NameModuloOrNull, false>;

REGISTER_FUNCTION(ModuloOrNull)
{
    FunctionDocumentation::Description description = R"(
Calculates the remainder when dividing `a` by `b`. Similar to function `modulo` except that `moduloOrNull` will return NULL
if the right argument is 0.
    )";
    FunctionDocumentation::Syntax syntax = "moduloOrNull(x, y)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The dividend. [`(U)Int*`](/sql-reference/data-types/int-uint)/[`Float32/64`](/sql-reference/data-types/float)."},
        {"y", "The divisor (modulus). [`(U)Int*`](/sql-reference/data-types/int-uint)/[`Float32/64`](/sql-reference/data-types/float)."}
    };
    FunctionDocumentation::ReturnedValue returned_value = R"(
Returns the remainder of the division of `x` by `y`, or null when the divisor is zero.
    )";
    FunctionDocumentation::Examples examples = {{"moduloOrNull by zero", "SELECT moduloOrNull(5, 0)", "\\N"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 5};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, categories};

    factory.registerFunction<FunctionModuloOrNull>(documentation);
    factory.registerAlias("modOrNull", "moduloOrNull", FunctionFactory::Case::Insensitive);
}

struct NamePositiveModuloOrNull
{
    static constexpr auto name = "positiveModuloOrNull";
};
using FunctionPositiveModuloOrNll = BinaryArithmeticOverloadResolver<PositiveModuloOrNullImpl, NamePositiveModuloOrNull, false>;

REGISTER_FUNCTION(PositiveModuloOrNull)
{
    FunctionDocumentation::Description description = R"(
Calculates the remainder when dividing `a` by `b`. Similar to function `positiveModulo` except that `positiveModuloOrNull` will return NULL
if the right argument is 0.
    )";
    FunctionDocumentation::Syntax syntax = "positiveModuloOrNull(x, y)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The dividend. [`(U)Int*`](/sql-reference/data-types/int-uint)/[`Float32/64`](/sql-reference/data-types/float)."},
        {"x", "The divisor (modulus). [`(U)Int*`](/sql-reference/data-types/int-uint)/[`Float32/64`](/sql-reference/data-types/float)."}
    };
    FunctionDocumentation::ReturnedValue returned_value = R"(
Returns the difference between `x` and the nearest integer not greater than
`x` divisible by `y`, `null` when the divisor is zero.
    )";
    FunctionDocumentation::Examples examples = {{"positiveModuloOrNull", "SELECT positiveModuloOrNull(5, 0)", "\\N"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 5};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, categories};

    factory.registerFunction<FunctionPositiveModuloOrNll>(documentation, FunctionFactory::Case::Insensitive);

    factory.registerAlias("positive_modulo_or_null", "positiveModuloOrNull", FunctionFactory::Case::Insensitive);
    factory.registerAlias("pmodOrNull", "positiveModuloOrNull", FunctionFactory::Case::Insensitive);
}

}
