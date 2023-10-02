#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace
{
template <typename T>
requires std::is_same_v<T, UInt8>
inline T byteSwap(T x)
{
    return x;
}

template <typename T>
requires std::is_same_v<T, UInt16>
inline T byteSwap(T x)
{
    return __builtin_bswap16(x);
}

template <typename T>
requires std::is_same_v<T, UInt32>
inline T byteSwap(T x)
{
    return __builtin_bswap32(x);
}

template <typename T>
requires std::is_same_v<T, UInt64>
inline T byteSwap(T x)
{
    return __builtin_bswap64(x);
}

template <typename T>
inline T byteSwap(T)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "byteSwap() is not implemented for {} datatype", demangle(typeid(T).name()));
}

template <typename T>
struct ByteSwapImpl
{
    using ResultType = T;
    // byteSwap on a String/FixedString is equivalent to `reverse` which is already implemented.
    static constexpr const bool allow_string_or_fixed_string = false;
    static inline T apply(T x) { return byteSwap<T>(x); }

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
struct FunctionUnaryArithmeticMonotonicity<NameByteSwap>
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const Field &, const Field &) { return {}; }
};

REGISTER_FUNCTION(ByteSwap)
{
    factory.registerFunction<FunctionByteSwap>();
}

}
