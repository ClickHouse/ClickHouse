/// Compile-time checks of `llvm-libc` multi-word integers, which are used inside the math functions (e.g. by `DyadicFloat`).
/// The words of `UInt` are stored least-significant first. A 128-bit shift must account for that on big-endian targets (s390x):
/// reinterpreting the words as a native `__uint128_t` there breaks `expm1`, `ldexpl`, `scalbnl`, `sqrtl`.

#include "src/__support/big_int.h"

namespace
{

using UInt128 = LIBC_NAMESPACE::UInt<128>;

constexpr UInt128 shiftLeft(UInt128 x, size_t offset)
{
    x <<= offset;
    return x;
}

constexpr UInt128 shiftRight(UInt128 x, size_t offset)
{
    x >>= offset;
    return x;
}

constexpr UInt128 high_one = shiftLeft(UInt128(1), 64);
static_assert(high_one.val[0] == 0 && high_one.val[1] == 1);

constexpr UInt128 crossing_left = shiftLeft(UInt128(0x8000000000000001ULL), 1);
static_assert(crossing_left.val[0] == 2 && crossing_left.val[1] == 1);

constexpr UInt128 crossing_right = shiftRight(high_one, 63);
static_assert(crossing_right.val[0] == 2 && crossing_right.val[1] == 0);

static_assert(shiftRight(high_one, 64).val[0] == 1 && shiftRight(high_one, 64).val[1] == 0);

}
