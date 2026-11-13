#pragma once

#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string_view>

#include <base/StringViewHash.h>
#include <base/defines.h>

/// The two words of a value. Without a hash they are 4-byte aligned, so that a row can hold the value at any multiple
/// of 4 bytes. `PackedStringRef` keeps the 8-byte alignment of its hash table cells.
template <size_t HASH_BYTES>
struct PackedStringRefWords
{
    uint64_t low;
    uint64_t high;
};

template <>
struct __attribute__((packed, aligned(4))) PackedStringRefWords<0>
{
    uint64_t low;
    uint64_t high;
};

/// `PackedStringRefT` is a compact, 16-byte string representation that encodes
/// payload and tag bits inside two 64-bit words. `HASH_BYTES` is the size of the content
/// hash that starts a small or medium value: 4 in `PackedStringRef`, 0 in `UnhashedPackedStringRef`.
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
/// Byte layout of `PackedStringRef` (16 bytes total):
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
///
/// ------------------------------------------------------------
/// `UnhashedPackedStringRef` has no hash and no medium encoding: a small string of
/// up to 15 bytes starts at byte 0, and every longer string is large. The tag byte
/// and the large encoding are the same as above:
///
///   bytes  0..14 : the string, then zeros (small)
///   bytes  0..7  : length (large)
///   bytes  8..14 : low 56 bits of pointer in little-endian byte order (large)
///   byte   15    : tag byte, as above
///
/// Two small values are equal as 16 bytes if and only if their strings are equal.
/// A large value holds the address of its string, so the values of equal strings at
/// different addresses differ.
template <size_t HASH_BYTES>
struct PackedStringRefT : PackedStringRefWords<HASH_BYTES>
{
    static_assert(HASH_BYTES == 0 || HASH_BYTES == 4);

    using PackedStringRefWords<HASH_BYTES>::low;
    using PackedStringRefWords<HASH_BYTES>::high;

    static constexpr size_t HASH_SIZE_BYTES = HASH_BYTES;
    static constexpr size_t SMALL_PAYLOAD_OFFSET = HASH_SIZE_BYTES;
    static constexpr size_t MEDIUM_LENGTH_OFFSET = HASH_SIZE_BYTES;
    static constexpr size_t TAG_BYTE_OFFSET = 15;
    static constexpr size_t PACKED_POINTER_BYTES = 7;
    static constexpr uint64_t MAX_SMALL_LEN = TAG_BYTE_OFFSET - HASH_SIZE_BYTES;

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
    [[nodiscard]] ALWAYS_INLINE const uint8_t * rawBytes() const { return reinterpret_cast<const uint8_t *>(this); }

    /// Every `build` path value-initializes both words before any byte is read, so the tag byte is
    /// always defined; the analyzer cannot see that through the byte-punned `rawBytes` read.
    // NOLINTNEXTLINE(clang-analyzer-core.uninitialized.UndefReturn)
    [[nodiscard]] ALWAYS_INLINE uint8_t getTagByte() const { return rawBytes()[TAG_BYTE_OFFSET]; }

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

    ALWAYS_INLINE void buildLarge(const char * ptr, size_t len)
    {
        if constexpr (std::endian::native == std::endian::little)
        {
            low = len;
            high = (reinterpret_cast<uintptr_t>(ptr) & POINTER_MASK) | (uint64_t(LARGE_TAG_BYTE) << 56);
        }
        else
        {
            storeLE(rawBytes(), len, sizeof(uint64_t));
            storeLE(rawBytes() + sizeof(uint64_t), reinterpret_cast<uintptr_t>(ptr), PACKED_POINTER_BYTES);
            rawBytes()[TAG_BYTE_OFFSET] = LARGE_TAG_BYTE;
        }
    }

    /// A struct and not a `std::array`, because the hardened `operator[]` of `std::array` would check the bounds on every row.
    struct ShortStringMasks
    {
        alignas(16) uint8_t bytes[16][16];
    };

    /// For `len` at most 15, `short_string_masks.bytes[len]` has 0xFF in the bytes [0, len) and `len` shifted left by
    /// `SMALL_LEN_SHIFT_IN_BYTE` in byte 15.
    static constexpr ShortStringMasks short_string_masks = []
    {
        ShortStringMasks masks{};
        for (size_t len = 0; len < 16; ++len)
        {
            for (size_t i = 0; i < len; ++i)
                masks.bytes[len][i] = 0xFF;
            masks.bytes[len][TAG_BYTE_OFFSET] = static_cast<uint8_t>(len << SMALL_LEN_SHIFT_IN_BYTE);
        }
        return masks;
    }();

public:
    [[nodiscard]] ALWAYS_INLINE bool isLarge() const
    {
        return (getTagByte() & LARGE_TAG_BYTE) != 0;
    }

    [[nodiscard]] ALWAYS_INLINE bool isEmpty() const
    {
        return high == 0;
    }

    [[nodiscard]] ALWAYS_INLINE bool isMedium() const
        requires(HASH_BYTES > 0)
    {
        return !isLarge() && getSmallSize() == 0 && !isEmpty();
    }

    [[nodiscard]] ALWAYS_INLINE bool isSmall() const
    {
        return !isLarge() && getSmallSize() != 0;
    }

    [[nodiscard]] ALWAYS_INLINE uint8_t getSmallSize() const
    {
        return static_cast<uint8_t>((getTagByte() & SMALL_LEN_NIBBLE_MASK) >> SMALL_LEN_SHIFT_IN_BYTE);
    }

    [[nodiscard]] ALWAYS_INLINE const char * getSmallPtr() const
    {
        return reinterpret_cast<const char *>(rawBytes() + SMALL_PAYLOAD_OFFSET);
    }

    [[nodiscard]] ALWAYS_INLINE uint32_t getMediumSize() const
        requires(HASH_BYTES > 0)
    {
        uint32_t size = 0;
        std::memcpy(&size, rawBytes() + MEDIUM_LENGTH_OFFSET, sizeof(size));
        return size;
    }

    [[nodiscard]] ALWAYS_INLINE const char * getMediumPtr() const
        requires(HASH_BYTES > 0)
    {
        return getHeapPtr();
    }

    [[nodiscard]] ALWAYS_INLINE uint64_t getLargeSize() const
    {
        return loadLE(rawBytes(), sizeof(uint64_t));
    }

    [[nodiscard]] ALWAYS_INLINE const char * getLargePtr() const
    {
        return reinterpret_cast<const char *>(loadLE(rawBytes() + sizeof(uint64_t), sizeof(uint64_t)) & POINTER_MASK);
    }

    [[nodiscard]] ALWAYS_INLINE uint32_t getHash() const
        requires(HASH_BYTES > 0)
    {
        if constexpr (std::endian::native == std::endian::little)
            return static_cast<uint32_t>(low);

        uint32_t hash = 0;
        std::memcpy(&hash, rawBytes(), sizeof(hash));
        return hash;
    }

    [[nodiscard]] ALWAYS_INLINE size_t heapSize() const
    {
        if constexpr (HASH_BYTES > 0)
        {
            if (isMedium())
                return getMediumSize();
        }
        if (isLarge())
            return getLargeSize();
        return 0;
    }

    /// The out-of-line bytes of a medium or large string, `heapSize` of them: both encodings
    /// keep the pointer in the same low 56 bits.
    [[nodiscard]] ALWAYS_INLINE const char * getHeapPtr() const
    {
        return reinterpret_cast<const char *>(loadLE(rawBytes() + sizeof(uint64_t), PACKED_POINTER_BYTES));
    }

    [[nodiscard]] ALWAYS_INLINE size_t size() const
        requires(HASH_BYTES == 0)
    {
        return isLarge() ? getLargeSize() : getSmallSize();
    }

    /// Copies the value to `dst` and returns the end of the copy. A small value is copied as the
    /// whole 16-byte ref, which is cheaper than a variable length copy, so 16 bytes at `dst` must
    /// be writable, even when the value is empty.
    ALWAYS_INLINE char8_t * copyTo(char8_t * dst) const
        requires(HASH_BYTES == 0)
    {
        if (isLarge())
        {
            std::memcpy(dst, getLargePtr(), getLargeSize());
            return dst + getLargeSize();
        }
        std::memcpy(dst, rawBytes(), sizeof(*this));
        return dst + getSmallSize();
    }

    /// A small string is viewed in place, so the view is valid while this value is.
    ALWAYS_INLINE explicit operator std::string_view() const
    {
        if (isEmpty())
            return {};
        if (isSmall())
            return {getSmallPtr(), getSmallSize()};
        if constexpr (HASH_BYTES > 0)
        {
            if (isMedium())
                return {getMediumPtr(), getMediumSize()};
        }
        return {getLargePtr(), getLargeSize()};
    }

    /// Points a medium or large string at a copy of its out-of-line bytes, keeping the encoding,
    /// which the tag byte holds. Used to rebind a key to memory that outlives the bytes it viewed
    /// (see `keyHolderPersistKey`).
    ALWAYS_INLINE void setHeapPointer(const char * ptr)
    {
        chassert(heapSize() != 0);
        chassert((reinterpret_cast<uintptr_t>(ptr) >> (PACKED_POINTER_BYTES * 8)) == 0);
        storeLE(rawBytes() + sizeof(uint64_t), reinterpret_cast<uintptr_t>(ptr), PACKED_POINTER_BYTES);
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
    static ALWAYS_INLINE PackedStringRefT build(const char * ptr, size_t len, HashFn && hash_fn)
        requires(HASH_BYTES > 0)
    {
        PackedStringRefT r{};

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

        r.buildLarge(ptr, len);
        return r;
    }

    /// Returns the ref of any string. For a string of at most 15 bytes it reads 16 bytes from `ptr`, as `buildShort` does.
    /// Code that knows the length class should call `buildShort` or `buildLong`.
    static ALWAYS_INLINE PackedStringRefT build(const char * ptr, size_t len)
        requires(HASH_BYTES == 0)
    {
        return len <= MAX_SMALL_LEN ? buildShort(ptr, len) : buildLong(ptr, len);
    }

    /// Returns the ref of a string longer than 15 bytes. The ref points to `ptr`.
    static ALWAYS_INLINE PackedStringRefT buildLong(const char * ptr, size_t len)
        requires(HASH_BYTES == 0)
    {
        PackedStringRefT r{};
        r.buildLarge(ptr, len);
        return r;
    }

    /// Returns the ref of a string of at most 15 bytes. It reads 16 bytes from `ptr`, even for an empty string, so 16
    /// bytes at `ptr` must be readable. `ColumnString::Chars` provides them: a `PaddedPODArray` has 63 readable bytes
    /// after its end.
    static ALWAYS_INLINE PackedStringRefT buildShort(const char * ptr, size_t len)
        requires(HASH_BYTES == 0)
    {
        chassert(len <= MAX_SMALL_LEN);

        /// Two `uint64_t` words, because clang reads them with one 16-byte load. From a byte array, clang loads bytes 0 to 14
        /// as 8 + 4 + 2 + 1 bytes.
        uint64_t words[2];
        uint64_t mask[2];
        std::memcpy(words, ptr, sizeof(words));
        /// A table of masks instead of a branch on `len`.
        std::memcpy(mask, short_string_masks.bytes[len], sizeof(mask));
        PackedStringRefT r{};
        r.low = words[0] & mask[0];
        /// Setting byte 15 to 0xFF makes the AND keep byte 15 of the mask, which holds the size. Byte 15 is the highest
        /// byte of the second word on little-endian and the lowest on big-endian.
        constexpr uint64_t tag_byte = std::endian::native == std::endian::little ? uint64_t(0xFF) << 56 : uint64_t(0xFF);
        r.high = (words[1] | tag_byte) & mask[1];
        return r;
    }
};

using PackedStringRef = PackedStringRefT<4>;
using UnhashedPackedStringRef = PackedStringRefT<0>;

/// Default string-view based hash function for PackedStringRef
struct PackedStringRefHash
{
    ALWAYS_INLINE uint32_t operator()(const char * data, size_t size) const
    {
        return static_cast<uint32_t>(StringViewHash()(std::string_view(data, size)));
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
        chassert(lhs_ptr && rhs_ptr);
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

/// Two values are equal if their strings are. Small values are equal when their 16 bytes are. Large values with different
/// addresses are compared by content.
inline ALWAYS_INLINE bool operator==(UnhashedPackedStringRef lhs, UnhashedPackedStringRef rhs)
{
    if (lhs.low == rhs.low && lhs.high == rhs.high)
        return true;

    if (!lhs.isLarge() || !rhs.isLarge() || lhs.getLargeSize() != rhs.getLargeSize())
        return false;

#if defined(__SSE2__) || (defined(__aarch64__) && defined(__ARM_NEON))
    return memequalWide(lhs.getLargePtr(), rhs.getLargePtr(), lhs.getLargeSize());
#else
    return memcmp(lhs.getLargePtr(), rhs.getLargePtr(), lhs.getLargeSize()) == 0;
#endif
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
