#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{

struct NameModuloOrNull { static constexpr auto name = "moduloOrNull"; };
using FunctionModuloOrNull = BinaryArithmeticOverloadResolver<ModuloOrNullImpl, NameModuloOrNull, false>;

REGISTER_FUNCTION(ModuloOrNull)
{
    factory.registerFunction<FunctionModuloOrNull>();
    factory.registerAlias("modOrNull", "moduloOrNull", FunctionFactory::Case::Insensitive);
}

struct NamePositiveModuloOrNull
{
    static constexpr auto name = "positiveModuloOrNull";
};
using FunctionPositiveModuloOrNll = BinaryArithmeticOverloadResolver<PositiveModuloOrNullImpl, NamePositiveModuloOrNull, false>;

REGISTER_FUNCTION(PositiveModuloOrNull)
{
    factory.registerFunction<FunctionPositiveModuloOrNll>(FunctionDocumentation
        {
            .description = R"(
Calculates the remainder when dividing `a` by `b`. Similar to function `positiveModulo` except that `positiveModuloOrNull` will return NULL
if the right argument is 0.
        )",
            .examples{{"positiveModuloOrNull", "SELECT positiveModuloOrNull(1, 0);", ""}},
            .category = FunctionDocumentation::Category::Arithmetic},
        FunctionFactory::Case::Insensitive);

    factory.registerAlias("positive_modulo_or_null", "positiveModuloOrNull", FunctionFactory::Case::Insensitive);
    factory.registerAlias("pmodOrNull", "positiveModuloOrNull", FunctionFactory::Case::Insensitive);
}

}
