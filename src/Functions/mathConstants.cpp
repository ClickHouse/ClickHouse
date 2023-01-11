#include <Functions/FunctionConstantBase.h>
#include <DataTypes/DataTypesNumber.h>

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
        static constexpr double value = 2.7182818284590452353602874713526624977572470;
    };

    using FunctionE = FunctionMathConstFloat64<EImpl>;


    struct PiImpl
    {
        static constexpr char name[] = "pi";
        static constexpr double value = 3.1415926535897932384626433832795028841971693;
    };

    using FunctionPi = FunctionMathConstFloat64<PiImpl>;
}

REGISTER_FUNCTION(E)
{
    factory.registerFunction<FunctionE>();
}

REGISTER_FUNCTION(Pi)
{
    factory.registerFunction<FunctionPi>(FunctionFactory::CaseInsensitive);
}

}
