#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

struct NameModuloLegacy
{
    static constexpr auto name = "moduloLegacy";
};
using FunctionModuloLegacy = BinaryArithmeticOverloadResolver<ModuloLegacyImpl, NameModuloLegacy, false>;

REGISTER_FUNCTION(ModuloLegacy)
{
    FunctionDocumentation::Description description = R"(
Calculates the remainder of a division. This is the legacy modulo implementation that uses the C++ `%` operator, which may produce negative results for negative arguments. This function exists for backward compatibility with old table partitioning logic. Use `modulo` or `positiveModulo` for standard behavior.
    )";
    FunctionDocumentation::Syntax syntax = "moduloLegacy(a, b)";
    FunctionDocumentation::Arguments arguments
        = {{"a", "The dividend.", {"(U)Int*", "Float*"}}, {"b", "The divisor.", {"(U)Int*", "Float*"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the remainder of the division.", {"(U)Int*", "Float*"}};
    FunctionDocumentation::Examples examples = {{"Basic usage", "SELECT moduloLegacy(10, 3)", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionModuloLegacy>(documentation);
}

}
