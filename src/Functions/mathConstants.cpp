#include <Functions/FunctionConstantBase.h>
#include <DataTypes/DataTypesNumber.h>

#include <numbers>


namespace DB
{

namespace
{
    template <typename Impl>
    class FunctionMathConstFloat64 : public FunctionConstantBase<FunctionMathConstFloat64<Impl>, Float64, DataTypeFloat64>
    {
    public:
        static constexpr auto name = Impl::name;
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMathConstFloat64>(); }
        FunctionMathConstFloat64() : FunctionConstantBase<FunctionMathConstFloat64<Impl>, Float64, DataTypeFloat64>(Impl::value) {}
    };


    struct EImpl
    {
        static constexpr char name[] = "e";
        static constexpr double value = std::numbers::e;
    };

    using FunctionE = FunctionMathConstFloat64<EImpl>;


    struct PiImpl
    {
        static constexpr char name[] = "pi";
        static constexpr double value = std::numbers::pi;
    };

    using FunctionPi = FunctionMathConstFloat64<PiImpl>;
}

REGISTER_FUNCTION(E)
{
    FunctionDocumentation::Description description = R"(
Returns Euler's constant (e).
)";
    FunctionDocumentation::Syntax syntax = "e()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns Euler's constant", {"Float64"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT e();", "2.718281828459045"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionE>(documentation);
}

REGISTER_FUNCTION(Pi)
{
    FunctionDocumentation::Description description = R"(
Returns pi (π).
)";
    FunctionDocumentation::Syntax syntax = "pi()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns pi", {"Float64"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT pi();", "3.141592653589793"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPi>(documentation, FunctionFactory::Case::Insensitive);
}

}
