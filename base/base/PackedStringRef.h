#pragma once

#include <bit>
#include <cstring>

#include <base/StringViewHash.h>
#include <base/defines.h>

/// PackedStringRef is a compact, 16-byte string representation that encodes
/// payload and tag bits inside two 64-bit words.
///
/// Every field lives at a fixed byte offset inside the struct. The packed
/// pointer and the 64-bit large length are stored in little-endian byte order
/// via `storeLE`/`loadLE`; the multi-byte integer fields (hash32, length32)
/// are stored in native byte order, so the byte image is stable within a host
/// but not identical across hosts of different endianness. That is sufficient:
/// the value never crosses processes, reads mirror writes, and `operator==`
/// compares `low`/`high` as `uint64_t`, so equality holds between any two
/// values built by the same code on the same host.
///
/// ------------------------------------------------------------
/// Byte layout (16 bytes total):
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

    static constexpr size_t HASH_SIZE_BYTES = 4;
    static constexpr size_t SMALL_PAYLOAD_OFFSET = HASH_SIZE_BYTES;
    static constexpr size_t MEDIUM_LENGTH_OFFSET = HASH_SIZE_BYTES;
    static constexpr size_t TAG_BYTE_OFFSET = 15;
    static constexpr size_t PACKED_POINTER_BYTES = 7;
    static constexpr uint64_t MAX_SMALL_LEN = 11;

    /// Tag byte bit layout.
    static constexpr uint8_t LARGE_TAG_BYTE = 0x80;
    static constexpr uint8_t SMALL_LEN_SHIFT_IN_BYTE = 3;
    static constexpr uint8_t SMALL_LEN_NIBBLE_MASK = 0x78;

    /// Low 56 bits of `high`: the packed pointer of the medium / large encodings.
    /// 56 bits fit any user-space pointer on the supported targets: Linux x86-64 keeps user
    /// addresses below 2^56 even with 5-level paging, and AArch64 virtual addresses take at
    /// most 52 bits. Pointer tagging that sets the top bits (e.g. HWASan) would make the
    /// truncation lossy; the setters assert the invariant in debug builds.
    static constexpr uint64_t POINTER_MASK = (uint64_t(1) << (PACKED_POINTER_BYTES * 8)) - 1;

private:
    ALWAYS_INLINE uint8_t * rawBytes() { return reinterpret_cast<uint8_t *>(this); }
    ALWAYS_INLINE const uint8_t * rawBytes() const { return reinterpret_cast<const uint8_t *>(this); }

    /// Every `build` path value-initializes both words before any byte is read, so the tag byte is
    /// always defined; the analyzer cannot see that through the byte-punned `rawBytes` read.
    // NOLINTNEXTLINE(clang-analyzer-core.uninitialized.UndefReturn)
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

    ALWAYS_INLINE uint8_t getSmallSize() const
    {
        return static_cast<uint8_t>((getTagByte() & SMALL_LEN_NIBBLE_MASK) >> SMALL_LEN_SHIFT_IN_BYTE);
    }

    ALWAYS_INLINE const char * getSmallPtr() const
    {
        return reinterpret_cast<const char *>(rawBytes() + SMALL_PAYLOAD_OFFSET);
    }

    ALWAYS_INLINE uint32_t getMediumSize() const
    {
        uint32_t size = 0;
        std::memcpy(&size, rawBytes() + MEDIUM_LENGTH_OFFSET, sizeof(size));
        return size;
    }

    ALWAYS_INLINE const char * getMediumPtr() const
    {
        return reinterpret_cast<const char *>(loadLE(rawBytes() + sizeof(uint64_t), PACKED_POINTER_BYTES));
    }

    ALWAYS_INLINE uint64_t getLargeSize() const
    {
        return loadLE(rawBytes(), sizeof(uint64_t));
    }

    ALWAYS_INLINE const char * getLargePtr() const
    {
        return reinterpret_cast<const char *>(loadLE(rawBytes() + sizeof(uint64_t), PACKED_POINTER_BYTES));
    }

    ALWAYS_INLINE uint32_t getHash() const
    {
        if constexpr (std::endian::native == std::endian::little)
            return static_cast<uint32_t>(low);

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
        chassert((reinterpret_cast<uintptr_t>(ptr) >> (PACKED_POINTER_BYTES * 8)) == 0);
        storeLE(rawBytes() + sizeof(uint64_t), reinterpret_cast<uintptr_t>(ptr), PACKED_POINTER_BYTES);
        rawBytes()[TAG_BYTE_OFFSET] = 0;
    }

    /// Set the large-string pointer (low 56 bits) and set the LARGE tag byte.
    ALWAYS_INLINE void setLargePointer(const char * ptr)
    {
        chassert((reinterpret_cast<uintptr_t>(ptr) >> (PACKED_POINTER_BYTES * 8)) == 0);
        storeLE(rawBytes() + sizeof(uint64_t), reinterpret_cast<uintptr_t>(ptr), PACKED_POINTER_BYTES);
        rawBytes()[TAG_BYTE_OFFSET] = LARGE_TAG_BYTE;
    }

    /// Build a packed value, computing the 32-bit content hash with `hash_fn(ptr, len)`.
    /// The functor is invoked only for the encodings that store a hash (small and medium,
    /// i.e. 1 <= len <= UINT32_MAX): large strings use the length as a hash surrogate and
    /// the empty value is all zeros.
    ///
    /// On little-endian targets the small-string path reads whole words starting at `ptr`,
    /// so the caller must guarantee at least 7 readable bytes past the end of the string
    /// (`ColumnString::Chars` is a `PaddedPODArray` and provides more than that).
    template <typename HashFn>
    static ALWAYS_INLINE PackedStringRef build(const char * ptr, size_t len, HashFn && hash_fn)
    {
        PackedStringRef r{};

        if (len == 0)
            return r;

        if (len <= MAX_SMALL_LEN)
        {
            const uint32_t hash = hash_fn(ptr, len);
            if constexpr (std::endian::native == std::endian::little)
            {
                /// Compose both words in registers. Byte-offset stores followed by a reload
                /// of `low`/`high` defeat store-to-load forwarding, and the variable-length
                /// payload copy compiles to a libc `memcpy` call; both dominate the per-row
                /// cost of hash table probes with short keys.
                uint64_t w0 = 0;
                std::memcpy(&w0, ptr, sizeof(w0));
                uint64_t w1 = 0;
                /// Zero the bytes past the end of the string: they are part of the key identity.
                if (len <= sizeof(w0))
                {
                    w0 &= ~uint64_t(0) >> ((sizeof(w0) - len) * 8);
                }
                else
                {
                    std::memcpy(&w1, ptr + sizeof(w0), sizeof(w1));
                    w1 &= ~uint64_t(0) >> ((2 * sizeof(w0) - len) * 8);
                }
                r.low = hash | ((w0 & 0xFFFFFFFF) << 32);
                r.high = (w0 >> 32) | (w1 << 32) | (len << (56 + SMALL_LEN_SHIFT_IN_BYTE));
            }
            else
            {
                std::memcpy(r.rawBytes(), &hash, sizeof(hash));
                std::memcpy(r.rawBytes() + SMALL_PAYLOAD_OFFSET, ptr, len);
                r.rawBytes()[TAG_BYTE_OFFSET] = static_cast<uint8_t>(static_cast<uint8_t>(len) << SMALL_LEN_SHIFT_IN_BYTE);
            }
            return r;
        }

        if (len <= std::numeric_limits<uint32_t>::max())
        {
            const uint32_t hash = hash_fn(ptr, len);
            if constexpr (std::endian::native == std::endian::little)
            {
                r.low = hash | (len << 32);
                r.high = reinterpret_cast<uintptr_t>(ptr) & POINTER_MASK;
            }
            else
            {
                uint32_t len32 = static_cast<uint32_t>(len);
                std::memcpy(r.rawBytes(), &hash, sizeof(hash));
                std::memcpy(r.rawBytes() + MEDIUM_LENGTH_OFFSET, &len32, sizeof(len32));
                storeLE(r.rawBytes() + sizeof(uint64_t), reinterpret_cast<uintptr_t>(ptr), PACKED_POINTER_BYTES);
            }
            return r;
        }

        /// Large string.
        if constexpr (std::endian::native == std::endian::little)
        {
            r.low = len;
            r.high = (reinterpret_cast<uintptr_t>(ptr) & POINTER_MASK) | (uint64_t(LARGE_TAG_BYTE) << 56);
        }
        else
        {
            storeLE(r.rawBytes(), len, sizeof(uint64_t));
            storeLE(r.rawBytes() + sizeof(uint64_t), reinterpret_cast<uintptr_t>(ptr), PACKED_POINTER_BYTES);
            r.rawBytes()[TAG_BYTE_OFFSET] = LARGE_TAG_BYTE;
        }
        return r;
    }
};

inline ALWAYS_INLINE bool operator==(PackedStringRef lhs, PackedStringRef rhs)
{
    if (lhs.low != rhs.low)
        return false;

    if (lhs.high == rhs.high)
        return true;

    if constexpr (std::endian::native == std::endian::little)
    {
        /// Same `low`, different `high`: only medium/large values of the same kind can
        /// still be equal. For small values the whole payload lives in the two words,
        /// so the mismatch is final; empty cannot reach here because medium/large store
        /// a non-zero length in `low`. Everything below is derived from the two words
        /// already in registers - re-extracting fields through byte-offset loads adds
        /// a chain of dependent instructions on the hot probe path.
        const uint64_t lhs_tag = lhs.high >> 56;
        const uint64_t rhs_tag = rhs.high >> 56;
        if (lhs_tag != rhs_tag || (lhs_tag & PackedStringRef::SMALL_LEN_NIBBLE_MASK))
            return false;

        const char * lhs_ptr = reinterpret_cast<const char *>(lhs.high & PackedStringRef::POINTER_MASK);
        const char * rhs_ptr = reinterpret_cast<const char *>(rhs.high & PackedStringRef::POINTER_MASK);
        const size_t size = (lhs_tag & PackedStringRef::LARGE_TAG_BYTE) ? lhs.low : (lhs.low >> 32);
#if defined(__SSE2__) || (defined(__aarch64__) && defined(__ARM_NEON))
        return memequalWide(lhs_ptr, rhs_ptr, size);
#else
        return memcmp(lhs_ptr, rhs_ptr, size) == 0;
#endif
    }
    else
    {
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
