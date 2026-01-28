#pragma once

#include <base/StringViewHash.h>

/// PackedStringRef layout (16 bytes total):
///
///   +----------------------+----------------------+
///   |    low (64 bits)     |    high (64 bits)    |
///   +----------------------+----------------------+
///
/// This is a tagged union encoded in two 64-bit words. The interpretation of `high` depends on the string category.
///
/// ------------------------------------------------------------
/// Small string (len > 0 and len <= MAX_INLINE_LEN = 11 bytes)
///
///   low:
///     [ 0..31 ]   : hash32
///     [32..63 ]   : inline string payload (first part, 4 bytes)
///
///   high:
///     [ 0..55 ]   : inline string payload (second part, 7 bytes)
///     [56..58 ]   : unused
///     [59..62 ]   : inline string length (4 bits, max 15)
///     [63 ]       : MUST be 0 (reserved for large-string tag)
///
/// ------------------------------------------------------------
/// Medium string (len <= 2^32 - 1)
///
///   low:
///     [ 0..31 ]   : hash32
///     [32..63 ]   : length32
///
///   high:
///     [ 0..55 ]   : pointer to string data
///     [56..63 ]   : zero
///
/// Notes:
///   - `high` stores an untagged pointer. Bit 63 of `high` MUST be zero for medium strings.
///   - This is guaranteed by the platform pointer layout and is required for unambiguous mode detection.
///
/// ------------------------------------------------------------
/// Large string (len > 2^32 - 1)
///
///   low:
///     [ 0..63 ]   : full 64-bit length
///
///   high:
///     [ 0..55 ]   : pointer to string data
///     [56..62 ]   : zero
///     [ 63 ]      : LARGE tag bit (borrowed from pointer high bit)
///
/// Notes:
///   - Large string uses a tagged-pointer representation. Bit 63 of `high` is set to distinguish this mode.
///   - This relies on the fact that valid user-space pointers do not occupy the highest 5 address bits on supported platforms.
///
/// ------------------------------------------------------------
/// Empty string (len == 0)
///
///   low is unused
///   high = 0
///
/// Notes:
///   - Checking `high == 0` is sufficient to detect empty strings.
///
/// ------------------------------------------------------------
/// Mode detection summary:
///
///   - isLarge()  : (high & LARGE_TAG) != 0
///   - isSmall()  : !isLarge() && inline_length != 0
///   - isMedium() : !isLarge() && inline_length == 0 && high != 0
struct PackedStringRef
{
    uint64_t low;
    uint64_t high;

    // --- Layout Constants ---
    static constexpr uint64_t HASH_SIZE_BYTES = 4;
    static constexpr uint64_t HASH_SIZE_BITS = 32;

    // Small String: High [59..62] bits store length, bit 63 must be 0
    static constexpr uint64_t SMALL_LEN_SHIFT = 59;
    static constexpr uint64_t MAX_SMALL_LEN   = 11;

    // Large String: Use bit 63 as tag.
    // Most 64-bit CPUs use 48-bit or 52-bit addressing.
    // Masking the top 5 bits (63-59) ensures safety against tagging and TBI.
    static constexpr uint64_t LARGE_TAG    = 1ULL << 63;
    static constexpr uint64_t POINTER_MASK = 0x07FFFFFFFFFFFFFFULL;

    /// ---------- Kind checks ----------

    ALWAYS_INLINE bool isLarge() const
    {
        return (high & LARGE_TAG) != 0;
    }

    ALWAYS_INLINE bool isMedium() const
    {
        return !isLarge() && getSmallSize() == 0;
    }

    ALWAYS_INLINE bool isSmall() const
    {
        return !isLarge() && getSmallSize() != 0;
    }

    /// ---------- Small string ----------

    ALWAYS_INLINE uint8_t getSmallSize() const
    {
        return static_cast<uint8_t>(high >> SMALL_LEN_SHIFT);
    }

    ALWAYS_INLINE const char * getSmallPtr() const
    {
        return reinterpret_cast<const char *>(&low) + HASH_SIZE_BYTES;
    }

    /// ---------- Medium string ----------

    ALWAYS_INLINE uint32_t getMediumSize() const
    {
        return static_cast<uint32_t>(low >> HASH_SIZE_BITS);
    }

    ALWAYS_INLINE const char * getMediumPtr() const
    {
        return reinterpret_cast<const char *>(high);
    }

    /// ---------- Large string ----------

    ALWAYS_INLINE uint64_t getLargeSize() const
    {
        return low;
    }

    ALWAYS_INLINE const char * getLargePtr() const
    {
        /// Clear top 5 bits to retrieve a clean canonical 64-bit pointer
        return reinterpret_cast<const char*>(high & POINTER_MASK);
    }

    /// ---------- Common ----------

    ALWAYS_INLINE uint32_t getHash() const
    {
        return static_cast<uint32_t>(low);
    }

    ALWAYS_INLINE size_t heapSize() const
    {
        if (isMedium())
            return getMediumSize();
        if (isLarge())
            return getLargeSize();
        return 0;
    }

    ALWAYS_INLINE explicit operator std::string_view() const
    {
        if (isSmall())
            return {getSmallPtr(), getSmallSize()};
        if (isMedium())
            return {getMediumPtr(), getMediumSize()};
        return {getLargePtr(), getLargeSize()};
    }

    /// ---------- Builder ----------

    static ALWAYS_INLINE PackedStringRef build(const char * ptr, size_t len, uint32_t hash)
    {
        PackedStringRef r{};

        if (len == 0)
        {
            r.low = 0;
            r.high = 0;
            return r;
        }

        /// 1. Small String Inline
        if (len <= MAX_SMALL_LEN)
        {
            r.low = hash;
            r.high = static_cast<uint64_t>(len) << SMALL_LEN_SHIFT;
            memcpy(reinterpret_cast<char *>(&r.low) + sizeof(uint32_t), ptr, len);
            return r;
        }

        /// 2. Medium String (32-bit length + 32-bit hash)
        if (len <= std::numeric_limits<uint32_t>::max())
        {
            r.low = (static_cast<uint64_t>(len) << 32) | hash;
            r.high = reinterpret_cast<uintptr_t>(ptr);
            return r;
        }

        /// 3. Large String (64-bit length, tagged pointer)
        r.low = len;
        r.high = reinterpret_cast<uintptr_t>(ptr) | LARGE_TAG;
        return r;
    }
};

/// ---------- Equality ----------

inline ALWAYS_INLINE bool operator==(PackedStringRef lhs, PackedStringRef rhs)
{
    if (lhs.low != rhs.low)
        return false;

    if (lhs.high == rhs.high)
        return true;

    if (lhs.isMedium() && rhs.isMedium()) [[likely]]
    {
#if defined(__SSE2__) || (defined(__aarch64__) && defined(__ARM_NEON))
        return memequalWide(lhs.getMediumPtr(), rhs.getMediumPtr(), lhs.getMediumSize());
#else
        return memcmp(lhs.getMediumPtr(), rhs.getMediumPtr(), lhs.getMediumSize()) == 0;
#endif
    }

    if (lhs.isLarge() && rhs.isLarge()) [[unlikely]]
    {
#if defined(__SSE2__) || (defined(__aarch64__) && defined(__ARM_NEON))
        return memequalWide(lhs.getLargePtr(), rhs.getLargePtr(), lhs.getLargeSize());
#else
        return memcmp(lhs.getLargePtr(), rhs.getLargePtr(), lhs.getLargeSize()) == 0;
#endif
    }

    return false;
}

namespace ZeroTraits
{

inline bool check(const PackedStringRef & x)
{
    return x.high == 0;
}

inline void set(PackedStringRef & x)
{
    x.high = 0;
}

}
