#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{

struct NameModuloOrNull { static constexpr auto name = "moduloOrNull"; };
using FunctionModuloOrNull = BinaryArithmeticOverloadResolver<ModuloOrNullImpl, NameModuloOrNull, false>;

REGISTER_FUNCTION(ModuloOrNull)
{
    FunctionDocumentation::Description description = R"(
Calculates the remainder when dividing `a` by `b`. Similar to function `modulo` except that `moduloOrNull` returns `NULL`
when the operation would otherwise raise a floating-point exception. For floating-point arguments this happens only when the
divisor is `0`; for integer arguments it additionally covers the minimal negative value modulo `-1` (e.g. `-128 % -1` for `Int8`).
    )";
    FunctionDocumentation::Syntax syntax = "moduloOrNull(x, y)";
    FunctionDocumentation::Arguments arguments =
    {
        {"x", "The dividend.", {"(U)Int*", "Float*"}},
        {"y", "The divisor (modulus).", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {R"(
Returns the remainder of the division of `x` by `y`, or `NULL` when the operation would raise a floating-point exception:
when the divisor is zero, or, for integer arguments, when computing the minimal negative value modulo `-1`.
    )"};
    FunctionDocumentation::Examples examples = {
        {"moduloOrNull by zero", "SELECT moduloOrNull(5, 0)", "\\N"},
        {"moduloOrNull of the minimal negative integer by -1", "SELECT moduloOrNull(toInt8(-128), toInt8(-1))", "\\N"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

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
Calculates the remainder when dividing `a` by `b`. Similar to function `positiveModulo` except that `positiveModuloOrNull` returns `NULL`
when the operation would otherwise raise a floating-point exception. For floating-point arguments this happens only when the
divisor is `0`; for integer arguments it additionally covers the minimal negative value modulo `-1` (e.g. `-128 % -1` for `Int8`).
    )";
    FunctionDocumentation::Syntax syntax = "positiveModuloOrNull(x, y)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The dividend. [`(U)Int*`](/sql-reference/data-types/int-uint)/[`Float32/64`](/sql-reference/data-types/float)."},
        {"x", "The divisor (modulus). [`(U)Int*`](/sql-reference/data-types/int-uint)/[`Float32/64`](/sql-reference/data-types/float)."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {R"(
Returns the difference between `x` and the nearest integer not greater than
`x` divisible by `y`, or `NULL` when the operation would raise a floating-point exception: when the divisor is zero, or,
for integer arguments, when computing the minimal negative value modulo `-1`.
    )"};
    FunctionDocumentation::Examples examples = {
        {"positiveModuloOrNull by zero", "SELECT positiveModuloOrNull(5, 0)", "\\N"},
        {"positiveModuloOrNull of the minimal negative integer by -1", "SELECT positiveModuloOrNull(toInt8(-128), toInt8(-1))", "\\N"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPositiveModuloOrNll>(documentation, FunctionFactory::Case::Insensitive);

    factory.registerAlias("positive_modulo_or_null", "positiveModuloOrNull", FunctionFactory::Case::Insensitive);
    factory.registerAlias("pmodOrNull", "positiveModuloOrNull", FunctionFactory::Case::Insensitive);
}

}
