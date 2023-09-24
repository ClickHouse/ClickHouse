#include <type_traits>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <base/bit_cast.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace
{

template <typename T>
requires std::is_integral_v<T> && (sizeof(T) <= sizeof(UInt32))
inline T roundDownToPowerOfTwo(T x)
{
    return x <= 0 ? 0 : (T(1) << (31 - __builtin_clz(x)));
}

template <typename T>
requires std::is_integral_v<T> && (sizeof(T) == sizeof(UInt64))
inline T roundDownToPowerOfTwo(T x)
{
    return x <= 0 ? 0 : (T(1) << (63 - __builtin_clzll(x)));
}

template <typename T>
requires std::is_same_v<T, Float32>
inline T roundDownToPowerOfTwo(T x)
{
    return bit_cast<T>(bit_cast<UInt32>(x) & ~((1ULL << 23) - 1));
}

template <typename T>
requires std::is_same_v<T, Float64>
inline T roundDownToPowerOfTwo(T x)
{
    return bit_cast<T>(bit_cast<UInt64>(x) & ~((1ULL << 52) - 1));
}

template <typename T>
requires is_big_int_v<T>
inline T roundDownToPowerOfTwo(T)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "roundToExp2() for big integers is not implemented");
}


template <typename T>
struct ByteSwapImpl
{
    using ResultType = T;

    static inline T apply(T x)
    {
        // return roundDownToPowerOfTwo<T>(x);
        return x;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

struct NameByteSwap
{
    static constexpr auto name = "byteSwap";
};
using FunctionByteSwap = FunctionUnaryArithmetic<ByteSwapImpl, NameByteSwap, false>;

}

template <>
struct FunctionUnaryArithmeticMonotonicity<FunctionByteSwap> : PositiveMonotonicity
{
};

REGISTER_FUNCTION(ByteSwap)
{
    factory.registerFunction<FunctionByteSwap>();
}

}
