#include <Functions/FastTrig.h>
#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct SinName { static constexpr auto name = "sin"; };
struct SinFast
{
    static constexpr auto name = SinName::name;
    static void fast(const double * src, size_t size, double * dst) { FastTrig::sin(src, size, dst); }
};
struct FunctionSin
{
    static constexpr auto name = SinName::name;
    static FunctionPtr create(ContextPtr context) { return createGatedMathUnary<SinName, SinFast, sin>(context); }
};

}

REGISTER_FUNCTION(Sin)
{
    factory.registerFunction<FunctionSin>(
        FunctionDocumentation{
            .description = "Returns the sine of the argument.",
            .syntax = "sin(x)",
            .arguments = {{"x", "The number whose sine will be returned.", {"(U)Int*", "Float*", "Decimal*"}}},
            .returned_value = {"Returns the sine of x."},
            .examples = {{.name = "simple", .query = "SELECT sin(1.23)", .result = "0.9424888019316975"}},
            .introduced_in = {1, 1},
            .category = FunctionDocumentation::Category::Mathematical},
        FunctionFactory::Case::Insensitive);
}

}
