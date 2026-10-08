#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

struct NamePositiveModulo
{
    static constexpr auto name = "positiveModulo";
};
using FunctionPositiveModulo = BinaryArithmeticOverloadResolver<PositiveModuloImpl, NamePositiveModulo, false>;

REGISTER_FUNCTION(PositiveModulo)
{
    FunctionDocumentation::Description description = R"(
Calculates the remainder when dividing `x` by `y`. Similar to function
`modulo` except that `positiveModulo` always return non-negative number.
    )";
    FunctionDocumentation::Syntax syntax = "positiveModulo(x, y)";
    FunctionDocumentation::Arguments arguments
        = {{"x", "The dividend.", {"(U)Int*", "Float*", "Decimal"}}, {"y", "The divisor (modulus).", {"(U)Int*", "Float*", "Decimal"}}};
    FunctionDocumentation::ReturnedValue returned_value = {R"(
Returns the difference between `x` and the nearest integer not greater than
`x` divisible by `y`.
    )"};
    FunctionDocumentation::Examples example = {{"Usage example", "SELECT positiveModulo(-1, 10)", "9"}};
    FunctionDocumentation::IntroducedIn introduced_in = {22, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, example, introduced_in, category};

    factory.registerFunction<FunctionPositiveModulo>(documentation, FunctionFactory::Case::Insensitive);

    factory.registerAlias("positive_modulo", "positiveModulo", FunctionFactory::Case::Insensitive);
    /// Compatibility with Spark:
    factory.registerAlias("pmod", "positiveModulo", FunctionFactory::Case::Insensitive);
}

}
