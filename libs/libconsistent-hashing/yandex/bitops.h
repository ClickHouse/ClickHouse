#pragma once
#include <stdint.h>
#include <limits>
#include <type_traits>


inline uint16_t LO_16(uint32_t x) { return static_cast<uint16_t>(x & 0x0000FFFF); }
inline uint16_t HI_16(uint32_t x) { return static_cast<uint16_t>(x >> 16); }

inline uint32_t LO_32(uint64_t x) { return static_cast<uint32_t>(x & 0x00000000FFFFFFFF); }
inline uint32_t HI_32(uint64_t x) { return static_cast<uint32_t>(x >> 32); }


/// Clang also defines __GNUC__
#if defined(__GNUC__)
        inline unsigned GetValueBitCountImpl(unsigned int value) noexcept {
            // NOTE: __builtin_clz* have undefined result for zero.
            return std::numeric_limits<unsigned int>::digits - __builtin_clz(value);
        }

        inline unsigned GetValueBitCountImpl(unsigned long value) noexcept {
            return std::numeric_limits<unsigned long>::digits - __builtin_clzl(value);
        }

        inline unsigned GetValueBitCountImpl(unsigned long long value) noexcept {
            return std::numeric_limits<unsigned long long>::digits - __builtin_clzll(value);
        }
#else
        /// Stupid realization for non GCC-like compilers. Can use BSR from x86 instructions set.
        template <typename T>
        inline unsigned GetValueBitCountImpl(T value) noexcept {
            unsigned result = 1; // result == 0 - impossible value, since value cannot be zero
            value >>= 1;
            while (value) {
                value >>= 1;
                ++result;
            }

            return result;
        }
#endif


/**
 * Returns the number of leading 0-bits in `value`, starting at the most significant bit position.
 * NOTE: value cannot be zero
 */
template <typename T>
static inline unsigned GetValueBitCount(T value) noexcept {
    using TCvt = std::make_unsigned_t<std::decay_t<T>>;
    return GetValueBitCountImpl(static_cast<TCvt>(value));
}
