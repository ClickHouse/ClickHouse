#include <Functions/FunctionNumericPredicate.h>
#include <Functions/FunctionFactory.h>
#include <ext/bit_cast.h>
#include <type_traits>


namespace DB
{

struct IsInfiniteImpl
{
    static constexpr auto name = "isInfinite";
    template <typename T>
    static bool execute(const T t)
    {
        if constexpr (std::is_same_v<T, float>)
            return (ext::bit_cast<uint32_t>(t)
                 & 0b01111111111111111111111111111111)
                == 0b01111111100000000000000000000000;
        else if constexpr (std::is_same_v<T, double>)
            return (ext::bit_cast<uint64_t>(t)
                 & 0b0111111111111111111111111111111111111111111111111111111111111111)
                == 0b0111111111110000000000000000000000000000000000000000000000000000;
        else
        {
            (void)t;
            return false;
        }
    }
};

using FunctionIsInfinite = FunctionNumericPredicate<IsInfiniteImpl>;


void registerFunctionIsInfinite(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsInfinite>();
}

}
