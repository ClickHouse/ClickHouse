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
requires std::is_integral_v<T>
inline T byteSwap(T x)
{
    return std::byteswap(x);
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
using FunctionByteSwap = FunctionUnaryArithmetic<ByteSwapImpl, NameByteSwap, true>;

}

template <>
struct FunctionUnaryArithmeticMonotonicity<NameByteSwap>
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const Field &, const Field &) { return {}; }
};

REGISTER_FUNCTION(ByteSwap)
{
    factory.registerFunction<FunctionByteSwap>(
        FunctionDocumentation{
            .description = R"(
Accepts an integer `operand` and returns the integer which is obtained by swapping the **endianness** of `operand` i.e. reversing the bytes of the `operand`.

Currently, this is supported for up to 64-bit (signed and unsigned) integers.

**Example**

```sql
byteSwap(3351772109)
```

Result:

```result
┌─byteSwap(3351772109)─┐
│           3455829959 │
└──────────────────────┘
```

The above example can be worked out in the following manner:
1. First, convert the integer operand (base 10) to its equivalent hexadecimal interpretation (base 16) in big-endian format i.e. 3351772109 -> C7 C7 FB CD (4 bytes)
2. Then, reverse the bytes i.e. C7 C7 FB CD -> CD FB C7 C7
3. Finally, the convert the hexadecimal number back to an integer assuming big-endian i.e. CD FB C7 C7  -> 3455829959

Note that, in step#1, one can also choose to convert the operand to bytes in little-endian as long as one also assumes little-endian when converting back to integer in step#3.

One use-case of this function is reversing IPv4s:
```result
┌─toIPv4(3351772109)─┐
│ 199.199.251.205    │
└────────────────────┘

┌─toIPv4(byteSwap(3351772109))─┐
│ 205.251.199.199              │
└──────────────────────────────┘
```
)",
            .examples{
                {"8-bit", "SELECT byteSwap(54)", "54"},
                {"16-bit", "SELECT byteSwap(4135)", "10000"},
                {"32-bit", "SELECT byteSwap(3351772109)", "3455829959"},
                {"64-bit", "SELECT byteSwap(123294967295)", "18439412204227788800"},
            },
            .categories{"Mathematical"}},
        FunctionFactory::CaseInsensitive);
}

}
