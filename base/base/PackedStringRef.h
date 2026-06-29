#pragma once

#include <bit>
#include <cstring>

#include <base/StringViewHash.h>

/// PackedStringRef is a compact, 16-byte string representation that encodes
/// payload and tag bits inside two 64-bit words.
///
/// The encoding uses a byte-stable layout: every field lives at a fixed byte
/// offset inside the struct, independent of host endianness. Writes use
/// `memcpy` at those byte offsets (and explicit little-endian shift helpers
/// for the 56-bit packed pointer / 64-bit length), and reads mirror that.
///
/// Because `operator==` compares `low`/`high` as `uint64_t`, equality still
/// holds across two values built by the same code: identical byte sequences
/// yield identical `uint64_t` values regardless of endianness.
///
/// ------------------------------------------------------------
/// Byte layout (16 bytes total, identical on little-endian and big-endian):
///
///   bytes  0..3  : hash32         (small / medium)
///                  low 32 bits of length (large)
///   bytes  4..7  : payload 0..3   (small)
///                  length32       (medium)
///                  high 32 bits of length (large)
///   bytes  8..14 : payload 4..10  (small)
///                  low 56 bits of pointer in little-endian byte order (medium / large)
///   byte   15    : tag byte
///                    bit 7    : LARGE flag
///                    bits 3-6 : SMALL length (4 bits, 0 if not small)
///                    bits 0-2 : reserved (0)
///
/// Empty: all 16 bytes are 0 (in particular, `high == 0`).
///
/// ------------------------------------------------------------
/// Mode detection summary:
///   - isLarge()  : tag byte has bit 7 set
///   - isSmall()  : !isLarge() && SMALL length nibble != 0
///   - isMedium() : !isLarge() && !isSmall() && high != 0
///   - isEmpty()  : high == 0
struct PackedStringRef
{
    uint64_t low;
    uint64_t high;

    // --- Layout Constants ---
    static constexpr size_t HASH_SIZE_BYTES = 4;
    static constexpr size_t TAG_BYTE_OFFSET = 15;
    static constexpr size_t PACKED_POINTER_BYTES = 7;
    static constexpr uint64_t MAX_SMALL_LEN = 11;

    /// Tag byte bit layout.
    static constexpr uint8_t LARGE_TAG_BYTE = 0x80;
    static constexpr uint8_t SMALL_LEN_SHIFT_IN_BYTE = 3;
    static constexpr uint8_t SMALL_LEN_NIBBLE_MASK = 0x78;

private:
    ALWAYS_INLINE uint8_t * rawBytes() { return reinterpret_cast<uint8_t *>(this); }
    ALWAYS_INLINE const uint8_t * rawBytes() const { return reinterpret_cast<const uint8_t *>(this); }

    ALWAYS_INLINE uint8_t getTagByte() const { return rawBytes()[TAG_BYTE_OFFSET]; }

    /// Store the low `n` bytes (n <= 8) of `value` at `dst` in little-endian
    /// byte order, regardless of host endianness.
    static ALWAYS_INLINE void storeLE(uint8_t * dst, uint64_t value, size_t n)
    {
        if constexpr (std::endian::native == std::endian::little)
        {
            std::memcpy(dst, &value, n);
        }
        else
        {
            for (size_t i = 0; i < n; ++i)
                dst[i] = static_cast<uint8_t>(value >> (i * 8));
        }
    }

    /// Load `n` bytes (n <= 8) from `src` interpreted as a little-endian
    /// unsigned integer, regardless of host endianness.
    static ALWAYS_INLINE uint64_t loadLE(const uint8_t * src, size_t n)
    {
        uint64_t value = 0;
        if constexpr (std::endian::native == std::endian::little)
        {
            std::memcpy(&value, src, n);
        }
        else
        {
            for (size_t i = 0; i < n; ++i)
                value |= static_cast<uint64_t>(src[i]) << (i * 8);
        }
        return value;
    }

public:
    /// ---------- Kind checks ----------

    ALWAYS_INLINE bool isLarge() const
    {
        return (getTagByte() & LARGE_TAG_BYTE) != 0;
    }

    ALWAYS_INLINE bool isEmpty() const
    {
        return high == 0;
    }

    ALWAYS_INLINE bool isMedium() const
    {
        return !isLarge() && getSmallSize() == 0 && !isEmpty();
    }

    ALWAYS_INLINE bool isSmall() const
    {
        return !isLarge() && getSmallSize() != 0;
    }

    /// ---------- Small string ----------

    ALWAYS_INLINE uint8_t getSmallSize() const
    {
        return static_cast<uint8_t>((getTagByte() & SMALL_LEN_NIBBLE_MASK) >> SMALL_LEN_SHIFT_IN_BYTE);
    }

    ALWAYS_INLINE const char * getSmallPtr() const
    {
        return reinterpret_cast<const char *>(rawBytes() + HASH_SIZE_BYTES);
    }

    /// ---------- Medium string ----------

    ALWAYS_INLINE uint32_t getMediumSize() const
    {
        uint32_t size = 0;
        std::memcpy(&size, rawBytes() + HASH_SIZE_BYTES, sizeof(size));
        return size;
    }

    ALWAYS_INLINE const char * getMediumPtr() const
    {
        return reinterpret_cast<const char *>(loadLE(rawBytes() + sizeof(uint64_t), PACKED_POINTER_BYTES));
    }

    /// ---------- Large string ----------

    ALWAYS_INLINE uint64_t getLargeSize() const
    {
        return loadLE(rawBytes(), sizeof(uint64_t));
    }

    ALWAYS_INLINE const char * getLargePtr() const
    {
        return reinterpret_cast<const char *>(loadLE(rawBytes() + sizeof(uint64_t), PACKED_POINTER_BYTES));
    }

    /// ---------- Common ----------

    ALWAYS_INLINE uint32_t getHash() const
    {
        uint32_t hash = 0;
        std::memcpy(&hash, rawBytes(), sizeof(hash));
        return hash;
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
        if (isEmpty())
            return {};
        if (isSmall())
            return {getSmallPtr(), getSmallSize()};
        if (isMedium())
            return {getMediumPtr(), getMediumSize()};
        return {getLargePtr(), getLargeSize()};
    }

    /// Set the medium-string pointer (low 56 bits) and clear the tag byte.
    /// Used by `keyHolderPersistKey` to rebind the key to arena-owned memory.
    ALWAYS_INLINE void setMediumPointer(const char * ptr)
    {
        storeLE(rawBytes() + sizeof(uint64_t), reinterpret_cast<uintptr_t>(ptr), PACKED_POINTER_BYTES);
        rawBytes()[TAG_BYTE_OFFSET] = 0;
    }

    /// Set the large-string pointer (low 56 bits) and set the LARGE tag byte.
    ALWAYS_INLINE void setLargePointer(const char * ptr)
    {
        storeLE(rawBytes() + sizeof(uint64_t), reinterpret_cast<uintptr_t>(ptr), PACKED_POINTER_BYTES);
        rawBytes()[TAG_BYTE_OFFSET] = LARGE_TAG_BYTE;
    }

    /// ---------- Builder ----------

    static ALWAYS_INLINE PackedStringRef build(const char * ptr, size_t len, uint32_t hash)
    {
        PackedStringRef r{};

        if (len == 0)
            return r;

        /// 1. Small String Inline
        if (len <= MAX_SMALL_LEN)
        {
            std::memcpy(r.rawBytes(), &hash, sizeof(hash));
            std::memcpy(r.rawBytes() + HASH_SIZE_BYTES, ptr, len);
            r.rawBytes()[TAG_BYTE_OFFSET] = static_cast<uint8_t>(static_cast<uint8_t>(len) << SMALL_LEN_SHIFT_IN_BYTE);
            return r;
        }

        /// 2. Medium String (32-bit length + 32-bit hash)
        if (len <= std::numeric_limits<uint32_t>::max())
        {
            uint32_t len32 = static_cast<uint32_t>(len);
            std::memcpy(r.rawBytes(), &hash, sizeof(hash));
            std::memcpy(r.rawBytes() + HASH_SIZE_BYTES, &len32, sizeof(len32));
            storeLE(r.rawBytes() + sizeof(uint64_t), reinterpret_cast<uintptr_t>(ptr), PACKED_POINTER_BYTES);
            return r;
        }

        /// 3. Large String (64-bit length, packed pointer + LARGE tag)
        storeLE(r.rawBytes(), len, sizeof(uint64_t));
        storeLE(r.rawBytes() + sizeof(uint64_t), reinterpret_cast<uintptr_t>(ptr), PACKED_POINTER_BYTES);
        r.rawBytes()[TAG_BYTE_OFFSET] = LARGE_TAG_BYTE;
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
    /// Canonicalize the whole empty value to all-zero bytes, matching the documented
    /// empty invariant above. `getHash` reads the low word, so leaving `low` with an
    /// arbitrary byte pattern from uninitialized zero-cell storage would make the stored
    /// empty key report a non-zero hash and land in a different bucket from subsequent
    /// empty keys during `convertToTwoLevel`.
    x.low = 0;
    x.high = 0;
}

}
