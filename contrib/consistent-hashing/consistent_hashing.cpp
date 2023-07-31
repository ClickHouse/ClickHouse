#include "consistent_hashing.h"

#include "bitops.h"

#include "popcount.h"

#include <stdexcept>

/*
 * (all numbers are written in big-endian manner: the least significant digit on the right)
 * (only bit representations are used - no hex or octal, leading zeroes are ommited)
 *
 * Consistent hashing scheme:
 *
 *      (sizeof(TValue) * 8, y]  (y, 0]
 * a =             *             ablock
 * b =             *             cblock
 *
 *      (sizeof(TValue) * 8, k]  (k, 0]
 * c =             *             cblock
 *
 * d =             *
 *
 * k - is determined by 2^(k-1) < n <= 2^k inequality
 * z - is number of ones in cblock
 * y - number of digits after first one in cblock
 *
 * The cblock determines logic of using a- and b- blocks:
 *
 *  bits of cblock | result of a function
 *              0  :   0
 *              1  :   1 (optimization, the next case includes this one)
 *          1?..?  :   1ablock (z is even) or 1bblock (z is odd) if possible (<n)
 *
 * If last case is not possible (>=n), than smooth moving from n=2^(k-1) to n=2^k is applied.
 * Using "*" bits of a-,b-,c-,d- blocks uint64_t value is combined, modulo of which determines
 * if the value should be greather than 2^(k-1) or ConsistentHashing(x, 2^(k-1)) should be used.
 * The last case is optimized according to previous checks.
 */

namespace {

template<class TValue>
TValue PowerOf2(size_t k) {
    return (TValue)0x1 << k;
}

template<class TValue>
TValue SelectAOrBBlock(TValue a, TValue b, TValue cBlock) {
    size_t z = PopCount<uint64_t>(cBlock);
    bool useABlock = z % 2 == 0;
    return useABlock ? a : b;
}

// Gets the exact result for n = k2 = 2 ^ k
template<class TValue>
size_t ConsistentHashingForPowersOf2(TValue a, TValue b, TValue c, TValue k2) {
    TValue cBlock = c & (k2 - 1); // (k, 0] bits of c
    // Zero and one cases
    if (cBlock < 2) {
        // First two cases of result function table: 0 if cblock is 0, 1 if cblock is 1.
        return cBlock;
    }
    size_t y = GetValueBitCount<uint64_t>(cBlock) - 1; // cblock = 0..01?..? (y = number of digits after 1), y > 0
    TValue y2 = PowerOf2<TValue>(y); // y2 = 2^y
    TValue abBlock = SelectAOrBBlock(a, b, cBlock) & (y2 - 1);
    return y2 + abBlock;
}

template<class TValue>
uint64_t GetAsteriskBits(TValue a, TValue b, TValue c, TValue d, size_t k) {
    size_t shift = sizeof(TValue) * 8 - k;
    uint64_t res = (d << shift) | (c >> k);
    ++shift;
    res <<= shift;
    res |= b >> (k - 1);
    res <<= shift;
    res |= a >> (k - 1);

    return res;
}

template<class TValue>
size_t ConsistentHashingImpl(TValue a, TValue b, TValue c, TValue d, size_t n) {
    if (n <= 0)
        throw std::runtime_error("Can't map consistently to a zero values.");

    // Uninteresting case
    if (n == 1) {
        return 0;
    }
    size_t k = GetValueBitCount(n - 1); // 2^(k-1) < n <= 2^k, k >= 1
    TValue k2 = PowerOf2<TValue>(k); // k2 = 2^k
    size_t largeValue;
    {
        // Bit determined variant. Large scheme.
        largeValue = ConsistentHashingForPowersOf2(a, b, c, k2);
        if (largeValue < n) {
            return largeValue;
        }
    }
    // Since largeValue is not assigned yet
    // Smooth moving from one bit scheme to another
    TValue k21 = PowerOf2<TValue>(k - 1);
    {
        size_t s = GetAsteriskBits(a, b, c, d, k) % (largeValue * (largeValue + 1));
        size_t largeValue2 = s / k2 + k21;
        if (largeValue2 < n) {
            return largeValue2;
        }
    }
    // Bit determined variant. Short scheme.
    return ConsistentHashingForPowersOf2(a, b, c, k21); // Do not apply checks. It is always less than k21 = 2^(k-1)
}

} // namespace // anonymous

std::size_t ConsistentHashing(std::uint64_t x, std::size_t n) {
    uint32_t lo = LO_32(x);
    uint32_t hi = HI_32(x);
    return ConsistentHashingImpl<uint16_t>(LO_16(lo), HI_16(lo), LO_16(hi), HI_16(hi), n);
}
std::size_t ConsistentHashing(std::uint64_t lo, std::uint64_t hi, std::size_t n) {
    return ConsistentHashingImpl<uint32_t>(LO_32(lo), HI_32(lo), LO_32(hi), HI_32(hi), n);
}
