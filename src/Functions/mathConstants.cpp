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
    factory.registerFunction<FunctionE>();
}

REGISTER_FUNCTION(Pi)
{
    factory.registerFunction<FunctionPi>({}, FunctionFactory::Case::Insensitive);
}

}
