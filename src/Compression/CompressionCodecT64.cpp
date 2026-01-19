#include <bit>
#include <cstring>
#include <type_traits>

#if defined(__BMI2__) || defined(__PCLMUL__)
#include <immintrin.h>
#endif
#ifdef __ARM_FEATURE_SVE2
#include <arm_sve.h>
#endif

#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <base/unaligned.h>
#include <Common/SipHash.h>
#include <Common/TargetSpecific.h>

namespace
{

/// These are bit_compress()/bit_expand() implementations from
/// https://github.com/eisenwave/cxx26-bit-permutations/blob/master/bit_permutations.hpp
/// They could be removed if bit-permutations bacome a standard.

[[nodiscard]] constexpr int log2Floor(unsigned x) noexcept
{
    return std::numeric_limits<unsigned>::digits - std::countl_zero(x) - 1;
}

/// Each bit in `x` is converted to the parity a bit and all bits to its right.
[[nodiscard]] constexpr unsigned long long bitwiseInclusiveRightParity64(unsigned long long x) noexcept
{
    using T = unsigned long long;

#ifdef __PCLMUL__
    const __m128i x_128 = _mm_set_epi64x(0, x);
    const __m128i neg1_128 = _mm_set_epi64x(0, -1);
    const __m128i result_128 = _mm_clmulepi64_si128(x_128, neg1_128, 0);
    return static_cast<T>(_mm_extract_epi64(result_128, 0));
#else
    constexpr int N = std::numeric_limits<T>::digits;
    for (int i = 1; i < N; i <<= 1)
        x ^= x << i;
    return x;
#endif
}

[[nodiscard]] constexpr unsigned long long bitCompress64(unsigned long long x, unsigned long long m) noexcept
{
    using T = unsigned long long;
    constexpr int N = std::numeric_limits<T>::digits;

#ifdef __BMI2__
    return _pext_u64(x, m);
#endif

#ifdef __ARM_FEATURE_SVE2
    auto sv_result = svbext_u64(svdup_u64(x), svdup_u64(m));
    return static_cast<T>(svorv_u64(svptrue_b64(), sv_result));
#endif

    x &= m;
    T mk = ~m << 1;

    for (int i = 1; i < N; i <<= 1)
    {
        const T mk_parity = bitwiseInclusiveRightParity64(mk);

        const T move = mk_parity & m;
        m = (m ^ move) | (move >> i);

        const T t = x & move;
        x = (x ^ t) | (t >> i);

        mk &= ~mk_parity;
    }
    return x;
}

[[nodiscard]] constexpr unsigned long long bitExpand64(unsigned long long x, unsigned long long m) noexcept
{
    using T = unsigned long long;
    constexpr int N = std::numeric_limits<T>::digits;
    constexpr int log_N = log2Floor(std::bit_ceil<unsigned>(N));

#ifdef __BMI2__
    return _pdep_u64(x, m);
#endif

#ifdef __ARM_FEATURE_SVE2
    auto sv_result = svbdep_u64(svdup_u64(x), svdup_u64(m));
    return static_cast<T>(svorv_u64(svptrue_b64(), sv_result));
#endif

    const T initial_m = m;

    T array[log_N];
    T mk = ~m << 1;

    for (int i = 0; i < log_N; ++i)
    {
        const T mk_parity = bitwiseInclusiveRightParity64(mk);
        const T move = mk_parity & m;
        m = (m ^ move) | (move >> (1 << i));
        array[i] = move;
        mk &= ~mk_parity;
    }

    for (int i = log_N - 1; i >= 0; --i)
    {
        const T move = array[i];
        const T t = x << (1 << i);
        x = (x & ~move) | (t & move);
    }

    return x & initial_m;
}

}


namespace DB
{

/// Get 64 integer values, makes 64x64 bit matrix, transpose it and crop unused bits (most significant zeroes).
/// In example, if we have UInt8 with only 0 and 1 inside 64xUInt8 would be compressed into 1xUInt64.
/// It detects unused bits by calculating min and max values of data part, saving them in header in compression phase.
/// There's a special case with signed integers parts with crossing zero data. Here it stores one more bit to detect sign of value.
class CompressionCodecT64 : public ICompressionCodec
{
public:
    static constexpr UInt32 HEADER_SIZE = 1 + 2 * sizeof(UInt64);
    static constexpr UInt32 MAX_COMPRESSED_BLOCK_SIZE = sizeof(UInt64) * 64;

    /// There're 4 compression variants:
    /// Byte - transpose bit matrix by bytes (only the last not full byte is transposed by bits). It's default.
    /// Bit - full bit-transpose of the bit matrix. It uses more resources and leads to better compression with ZSTD (but worse with LZ4).
    /// Mask variants use PEXT/PDEP x86 instruction masking logic instead of min-max over int prefixes
    enum class Variant : uint8_t
    {
        Byte = 0,
        Bit = 1,
        ByteMask = 2,   // it must be 0b10
        BitMask = 3,    // it must be 0b11
    };

    // type_idx_ is required for compression, but not for decompression.
    CompressionCodecT64(std::optional<TypeIndex> type_idx_, Variant variant_);

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * src, UInt32 src_size, char * dst) const override;
    UInt32 doDecompressData(const char * src, UInt32 src_size, char * dst, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override
    {
        /// uncompressed_size - (uncompressed_size % (sizeof(T) * 64)) + sizeof(UInt64) * sizeof(T) + header_size
        return uncompressed_size + MAX_COMPRESSED_BLOCK_SIZE + HEADER_SIZE;
    }

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }
    String getDescription() const override
    {
        return "Preprocessor. Crops unused high bits; puts them into a 64x64 bit matrix; optimized for 64-bit data types.";
    }

private:
    std::optional<TypeIndex> type_idx;
    Variant variant;
};


namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

namespace
{

/// Fixed TypeIds that numbers would not be changed between versions.
enum class MagicNumber : uint8_t
{
    UInt8       = 1,
    UInt16      = 2,
    UInt32      = 3,
    UInt64      = 4,
    Int8        = 6,
    Int16       = 7,
    Int32       = 8,
    Int64       = 9,
    Date        = 13,
    DateTime    = 14,
    DateTime64  = 15,
    Enum8       = 17,
    Enum16      = 18,
    Decimal32   = 19,
    Decimal64   = 20,
    IPv4        = 21,
    Date32      = 22,
    Time        = 23,
    Time64      = 24,
};

MagicNumber serializeTypeId(std::optional<TypeIndex> type_id)
{
    if (!type_id)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "T64 codec doesn't support compression without information about column type");

    switch (*type_id)
    {
        case TypeIndex::UInt8:      return MagicNumber::UInt8;
        case TypeIndex::UInt16:     return MagicNumber::UInt16;
        case TypeIndex::UInt32:     return MagicNumber::UInt32;
        case TypeIndex::UInt64:     return MagicNumber::UInt64;
        case TypeIndex::Int8:       return MagicNumber::Int8;
        case TypeIndex::Int16:      return MagicNumber::Int16;
        case TypeIndex::Int32:      return MagicNumber::Int32;
        case TypeIndex::Int64:      return MagicNumber::Int64;
        case TypeIndex::Date:       return MagicNumber::Date;
        case TypeIndex::Date32:     return MagicNumber::Date32;
        case TypeIndex::Time:       return MagicNumber::Time;
        case TypeIndex::Time64:     return MagicNumber::Time64;
        case TypeIndex::DateTime:   return MagicNumber::DateTime;
        case TypeIndex::DateTime64: return MagicNumber::DateTime64;
        case TypeIndex::Enum8:      return MagicNumber::Enum8;
        case TypeIndex::Enum16:     return MagicNumber::Enum16;
        case TypeIndex::Decimal32:  return MagicNumber::Decimal32;
        case TypeIndex::Decimal64:  return MagicNumber::Decimal64;
        case TypeIndex::IPv4:       return MagicNumber::IPv4;
        default:
            break;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Type is not supported by T64 codec: {}", static_cast<UInt32>(*type_id));
}

TypeIndex deserializeTypeId(uint8_t serialized_type_id)
{
    MagicNumber magic = static_cast<MagicNumber>(serialized_type_id);
    switch (magic)
    {
        case MagicNumber::UInt8:        return TypeIndex::UInt8;
        case MagicNumber::UInt16:       return TypeIndex::UInt16;
        case MagicNumber::UInt32:       return TypeIndex::UInt32;
        case MagicNumber::UInt64:       return TypeIndex::UInt64;
        case MagicNumber::Int8:         return TypeIndex::Int8;
        case MagicNumber::Int16:        return TypeIndex::Int16;
        case MagicNumber::Int32:        return TypeIndex::Int32;
        case MagicNumber::Int64:        return TypeIndex::Int64;
        case MagicNumber::Date:         return TypeIndex::Date;
        case MagicNumber::Date32:       return TypeIndex::Date32;
        case MagicNumber::DateTime:     return TypeIndex::DateTime;
        case MagicNumber::DateTime64:   return TypeIndex::DateTime64;
        case MagicNumber::Time:         return TypeIndex::Time;
        case MagicNumber::Time64:       return TypeIndex::Time64;
        case MagicNumber::Enum8:        return TypeIndex::Enum8;
        case MagicNumber::Enum16:       return TypeIndex::Enum16;
        case MagicNumber::Decimal32:    return TypeIndex::Decimal32;
        case MagicNumber::Decimal64:    return TypeIndex::Decimal64;
        case MagicNumber::IPv4:         return TypeIndex::IPv4;
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Bad magic number in T64 codec: {}", static_cast<UInt32>(serialized_type_id));
}


UInt8 codecId()
{
    return static_cast<UInt8>(CompressionMethodByte::T64);
}

TypeIndex baseType(TypeIndex type_idx)
{
    switch (type_idx)
    {
        case TypeIndex::Int8:
            return TypeIndex::Int8;
        case TypeIndex::Int16:
            return TypeIndex::Int16;
        case TypeIndex::Int32:
        case TypeIndex::Time:
        case TypeIndex::Decimal32:
        case TypeIndex::Date32:
            return TypeIndex::Int32;
        case TypeIndex::Int64:
        case TypeIndex::Decimal64:
        case TypeIndex::Time64:
        case TypeIndex::DateTime64:
            return TypeIndex::Int64;
        case TypeIndex::UInt8:
        case TypeIndex::Enum8:
            return TypeIndex::UInt8;
        case TypeIndex::UInt16:
        case TypeIndex::Enum16:
        case TypeIndex::Date:
            return TypeIndex::UInt16;
        case TypeIndex::UInt32:
        case TypeIndex::DateTime:
        case TypeIndex::IPv4:
            return TypeIndex::UInt32;
        case TypeIndex::UInt64:
            return TypeIndex::UInt64;
        default:
            break;
    }

    return TypeIndex::Nothing;
}

TypeIndex typeIdx(const IDataType * data_type)
{
    if (!data_type)
        return TypeIndex::Nothing;

    WhichDataType which(*data_type);
    switch (which.idx)
    {
        case TypeIndex::Int8:
        case TypeIndex::UInt8:
        case TypeIndex::Enum8:
        case TypeIndex::Int16:
        case TypeIndex::UInt16:
        case TypeIndex::Enum16:
        case TypeIndex::Date:
        case TypeIndex::Date32:
        case TypeIndex::Int32:
        case TypeIndex::UInt32:
        case TypeIndex::IPv4:
        case TypeIndex::Time:
        case TypeIndex::Time64:
        case TypeIndex::DateTime:
        case TypeIndex::DateTime64:
        case TypeIndex::Decimal32:
        case TypeIndex::Int64:
        case TypeIndex::UInt64:
        case TypeIndex::Decimal64:
            return which.idx;
        default:
            break;
    }

    return TypeIndex::Nothing;
}

void transpose64x8(UInt64 * src_dst)
{
    const auto * src8 = reinterpret_cast<const UInt8 *>(src_dst);
    UInt64 dst[8] = {};

    for (UInt32 i = 0; i < 64; ++i)
    {
        UInt64 value = src8[i];
        dst[0] |= (value & 0x1) << i;
        dst[1] |= ((value >> 1) & 0x1) << i;
        dst[2] |= ((value >> 2) & 0x1) << i;
        dst[3] |= ((value >> 3) & 0x1) << i;
        dst[4] |= ((value >> 4) & 0x1) << i;
        dst[5] |= ((value >> 5) & 0x1) << i;
        dst[6] |= ((value >> 6) & 0x1) << i;
        dst[7] |= ((value >> 7) & 0x1) << i;
    }

    memcpy(src_dst, dst, 8 * sizeof(UInt64));
}

void reverseTranspose64x8(UInt64 * src_dst)
{
    UInt8 dst8[64];

    for (UInt32 i = 0; i < 64; ++i)
    {
        dst8[i] = static_cast<UInt8>(
            ((src_dst[0] >> i) & 0x1)
            | (((src_dst[1] >> i) & 0x1) << 1)
            | (((src_dst[2] >> i) & 0x1) << 2)
            | (((src_dst[3] >> i) & 0x1) << 3)
            | (((src_dst[4] >> i) & 0x1) << 4)
            | (((src_dst[5] >> i) & 0x1) << 5)
            | (((src_dst[6] >> i) & 0x1) << 6)
            | (((src_dst[7] >> i) & 0x1) << 7));
    }

    memcpy(src_dst, dst8, 8 * sizeof(UInt64));
}

template <typename T>
void transposeBytes(T value, UInt64 * matrix, UInt32 col)
{
    UInt8 * matrix8 = reinterpret_cast<UInt8 *>(matrix);
    const UInt8 * value8 = reinterpret_cast<const UInt8 *>(&value);

    if constexpr (sizeof(T) > 4)
    {
        matrix8[64 * 7 + col] = value8[7];
        matrix8[64 * 6 + col] = value8[6];
        matrix8[64 * 5 + col] = value8[5];
        matrix8[64 * 4 + col] = value8[4];
    }

    if constexpr (sizeof(T) > 2)
    {
        matrix8[64 * 3 + col] = value8[3];
        matrix8[64 * 2 + col] = value8[2];
    }

    if constexpr (sizeof(T) > 1)
        matrix8[64 * 1 + col] = value8[1];

    matrix8[64 * 0 + col] = value8[0];
}

template <typename T>
void reverseTransposeBytes(const UInt64 * matrix, UInt32 col, T & value)
{
    const auto * matrix8 = reinterpret_cast<const UInt8 *>(matrix);

    if constexpr (sizeof(T) > 4)
    {
        value |= static_cast<UInt64>(matrix8[64 * 7 + col]) << (8 * 7);
        value |= static_cast<UInt64>(matrix8[64 * 6 + col]) << (8 * 6);
        value |= static_cast<UInt64>(matrix8[64 * 5 + col]) << (8 * 5);
        value |= static_cast<UInt64>(matrix8[64 * 4 + col]) << (8 * 4);
    }

    if constexpr (sizeof(T) > 2)
    {
        value |= static_cast<UInt32>(matrix8[64 * 3 + col]) << (8 * 3);
        value |= static_cast<UInt32>(matrix8[64 * 2 + col]) << (8 * 2);
    }

    if constexpr (sizeof(T) > 1)
        value |= static_cast<UInt32>(matrix8[64 * 1 + col]) << (8 * 1);

    value |= static_cast<UInt32>(matrix8[col]);
}


template <typename T>
void load(const char * src, T * buf, UInt32 tail = 64)
{
    if constexpr (std::endian::native == std::endian::little)
    {
        memcpy(buf, src, tail * sizeof(T));
    }
    else
    {
        /// Since the algorithm uses little-endian integers, data is loaded
        /// as little-endian types on big-endian machine (s390x, etc).
        for (UInt32 i = 0; i < tail; ++i)
        {
            buf[i] = unalignedLoadLittleEndian<T>(src + i * sizeof(T));
        }
    }
}

template <typename T>
void store(const T * buf, char * dst, UInt32 tail = 64)
{
    memcpy(dst, buf, tail * sizeof(T));
}

template <typename T>
void clear(T * buf)
{
    for (UInt32 i = 0; i < 64; ++i)
        buf[i] = 0;
}


/// UIntX[64] -> UInt64[N] transposed matrix, N <= X
template <typename T, bool full = false>
void transpose(const T * src, char * dst, UInt32 num_bits, UInt32 tail = 64)
{
    UInt32 full_bytes = num_bits / 8;
    UInt32 part_bits = num_bits % 8;

    UInt64 matrix[64] = {};
    for (UInt32 col = 0; col < tail; ++col)
        transposeBytes(src[col], matrix, col);

    if constexpr (full)
    {
        UInt64 * matrix_line = matrix;
        for (UInt32 byte = 0; byte < full_bytes; ++byte, matrix_line += 8)
            transpose64x8(matrix_line);
    }

    UInt32 full_size = sizeof(UInt64) * (num_bits - part_bits);
    memcpy(dst, matrix, full_size);
    dst += full_size;

    /// transpose only partially filled last byte
    if (part_bits)
    {
        UInt64 * matrix_line = &matrix[full_bytes * 8];
        transpose64x8(matrix_line);
        memcpy(dst, matrix_line, part_bits * sizeof(UInt64));
    }
}

MULTITARGET_FUNCTION_AVX512BW_AVX2(
MULTITARGET_FUNCTION_HEADER(
template <typename T, bool full>
void), reverseTransposeImpl, MULTITARGET_FUNCTION_BODY((const char * src, T * buf, UInt32 num_bits, UInt32 tail) /// NOLINT
{
    UInt64 matrix[64] = {};
    memcpy(matrix, src, num_bits * sizeof(UInt64));

    UInt32 full_bytes = num_bits / 8;
    UInt32 part_bits = num_bits % 8;

    if constexpr (full)
    {
        UInt64 * matrix_line = matrix;
        for (UInt32 byte = 0; byte < full_bytes; ++byte, matrix_line += 8)
            reverseTranspose64x8(matrix_line);
    }

    if (part_bits)
    {
        UInt64 * matrix_line = &matrix[full_bytes * 8];
        reverseTranspose64x8(matrix_line);
    }

    clear(buf);
    for (UInt32 col = 0; col < tail; ++col)
        reverseTransposeBytes(matrix, col, buf[col]);
})
)

/// UInt64[N] transposed matrix -> UIntX[64]
template <typename T, bool full = false>
ALWAYS_INLINE void reverseTranspose(const char * src, T * buf, UInt32 num_bits, UInt32 tail = 64)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::AVX512BW))
    {
        reverseTransposeImplAVX512BW<T, full>(src, buf, num_bits, tail);
        return;
    }
    if (isArchSupported(TargetArch::AVX2))
    {
        reverseTransposeImplAVX2<T, full>(src, buf, num_bits, tail);
        return;
    }
#endif
    {
        reverseTransposeImpl<T, full>(src, buf, num_bits, tail);
    }
}

template <typename T, typename MinMaxT = std::conditional_t<is_signed_v<T>, Int64, UInt64>>
void restoreUpperBits(T * buf, T upper_min, T upper_max [[maybe_unused]], T sign_bit [[maybe_unused]], UInt32 tail = 64)
{
    if constexpr (is_signed_v<T>)
    {
        /// Restore some data as negatives and others as positives
        if (sign_bit)
        {
            for (UInt32 col = 0; col < tail; ++col)
            {
                T & value = buf[col];

                if (value & sign_bit)
                    value |= upper_min;
                else
                    value |= upper_max;
            }

            return;
        }
    }

    for (UInt32 col = 0; col < tail; ++col)
        buf[col] |= upper_min;
}


UInt32 getValuableBitsNumber(UInt64 min, UInt64 max)
{
    UInt64 diff_bits = min ^ max;
    if (diff_bits)
        return 64 - std::countl_zero(diff_bits);
    return 0;
}

UInt32 getValuableBitsNumber(Int64 min, Int64 max)
{
    if (min < 0 && max >= 0)
    {
        if (min + max >= 0)
            return getValuableBitsNumber(0ull, static_cast<UInt64>(max)) + 1;
        return getValuableBitsNumber(0ull, static_cast<UInt64>(~min)) + 1;
    }
    return getValuableBitsNumber(static_cast<UInt64>(min), static_cast<UInt64>(max));
}


template <typename T>
void findMinMax(const char * src, UInt32 src_size, T & min, T & max)
{
    min = unalignedLoad<T>(src);
    max = unalignedLoad<T>(src);

    const char * end = src + src_size;
    for (; src < end; src += sizeof(T))
    {
        auto current = unalignedLoad<T>(src);
        if (current < min)
            min = current;
        if (current > max)
            max = current;
    }
}

template <typename T>
void findMasks(const char * src, UInt32 src_size, T & positive_mask, T & negative_mask, T & positive, T & negative)
{
    T pos_and = -1ll;
    T pos_or = 0;
    T neg_and = -1ll;
    T neg_or = 0;

    const char * end = src + src_size;
    for (; src < end; src += sizeof(T))
    {
        auto current = unalignedLoad<T>(src);
        if constexpr (is_signed_v<T>)
        {
            if (current >= 0)
            {
                positive = current;
                pos_and &= current;
                pos_or |= current;
            }
            else
            {
                negative = current;
                neg_and &= current;
                neg_or |= current;
            }
        }
        else
        {
            positive = current;
            pos_and &= current;
            pos_or |= current;
        }
    }

    /// and : 0-bit mean where was at least one zero
    /// or : 1-bit means where was at least one not zero
    /// mask : 1-bit means there were both zero and not zero, so pass it to PEXT as 1
    positive_mask = ~pos_and & pos_or;
    negative_mask = ~neg_and & neg_or;

    /// which bits set to 1 after PDEP
    positive &= ~positive_mask;
    negative &= ~negative_mask;
}

template <typename T>
UInt32 getMaskBitsNumber(T positive_mask, T negative_mask, T & negative_bit)
{
    using UT = std::conditional_t<std::is_same_v<T, Int8>, unsigned char, std::make_unsigned_t<T>>;

    UT pos_mask = positive_mask;
    UT neg_mask = negative_mask;

    UInt32 num_bits = std::max(std::popcount(pos_mask), std::popcount(neg_mask));

    if constexpr (is_signed_v<T>)
    {
        if (negative_bit && num_bits == 8 * sizeof(T))
            negative_bit = 0; /// We have negatives but do not need additional bit for them

        if (negative_bit)
        {
            negative_bit = T(1) << num_bits;
            ++num_bits;
        }
    }
    else
        negative_bit = 0;

    return num_bits;
}

template <typename T>
void pext(T * buf, T positive_mask, T negative_mask, T negative_bit, UInt32 tail = 64)
{
    for (UInt32 i = 0; i < tail; ++i)
    {
        T current = buf[i];
        if (current >= 0)
            buf[i] = static_cast<T>(bitCompress64(current, positive_mask));
        else
            buf[i] = static_cast<T>(bitCompress64(current, negative_mask) | negative_bit);
    }
}

template <typename T>
void pdep(T * buf, T positive_mask, T negative_mask, T positive, T negative, T negative_bit, UInt32 tail = 64)
{
    for (UInt32 i = 0; i < tail; ++i)
    {
        T current = buf[i];
        if (current & negative_bit)
        {
            current &= ~negative_bit;
            buf[i] = negative | bitExpand64(current, negative_mask);
        }
        else
            buf[i] = positive | bitExpand64(current, positive_mask);
    }
}


using Variant = CompressionCodecT64::Variant;

template <typename T, bool full, bool with_mask = false>
UInt32 compressData(const char * src, UInt32 bytes_size, char * dst)
{
    using MinMaxType = std::conditional_t<is_signed_v<T>, Int64, UInt64>;

    static constexpr const UInt32 matrix_size = 64;
    static constexpr const UInt32 header_size = with_mask ? 4 * sizeof(T) : 2 * sizeof(MinMaxType);

    UInt8 bytes_to_skip = bytes_size % sizeof(T);
    bytes_size -= bytes_to_skip;
    memcpy(dst, src, bytes_to_skip);
    src += bytes_to_skip;
    dst += bytes_to_skip;

    if (bytes_size == 0)
        return bytes_to_skip;

    UInt32 src_size = bytes_size / sizeof(T);
    UInt32 num_full = src_size / matrix_size;
    UInt32 tail = src_size % matrix_size;
    UInt32 num_bits = 0;

    T positive_mask = 0;
    T negative_mask = 0;
    T negative_bit = 0;

    if constexpr (with_mask)
    {
        T header[4] = {0, 0, 0, 0}; /// positive_mask, negative_mask, positive, negative
        findMasks<T>(src, bytes_size, header[0], header[1], header[2], header[3]);

        positive_mask = header[0];
        negative_mask = header[1];
        negative_bit = negative_mask || header[3]; // in-out: has-negtives -> negative-bit-position
        num_bits = getMaskBitsNumber(positive_mask, negative_mask, negative_bit);

        /// Write header
        store(header, dst, 4);
        dst += header_size;
    }
    else
    {
        T min = 0;
        T max = 0;
        findMinMax<T>(src, bytes_size, min, max);
        MinMaxType header[2] = {min, max};

        num_bits = getValuableBitsNumber(header[0], header[1]);

        /// Write header
        store(header, dst, 2);
        dst += header_size;
    }

    if (!num_bits)
        return header_size + bytes_to_skip;

    T buf[matrix_size];
    UInt32 src_shift = sizeof(T) * matrix_size;
    UInt32 dst_shift = sizeof(UInt64) * num_bits;
    for (UInt32 i = 0; i < num_full; ++i)
    {
        load<T>(src, buf, matrix_size);
        if constexpr (with_mask)
            pext<T>(buf, positive_mask, negative_mask, negative_bit, matrix_size);
        transpose<T, full>(buf, dst, num_bits);
        src += src_shift;
        dst += dst_shift;
    }

    UInt32 dst_bytes = num_full * dst_shift;

    if (tail)
    {
        load<T>(src, buf, tail);
        if constexpr (with_mask)
            pext<T>(buf, positive_mask, negative_mask, negative_bit, tail);
        transpose<T, full>(buf, dst, num_bits, tail);
        dst_bytes += dst_shift;
    }

    return header_size + dst_bytes + bytes_to_skip;
}

template <typename T, bool full, bool with_mask = false>
UInt32 decompressData(const char * src, UInt32 bytes_size, char * dst, UInt32 uncompressed_size)
{
    using MinMaxType = std::conditional_t<is_signed_v<T>, Int64, UInt64>;

    static constexpr const UInt32 matrix_size = 64;
    static constexpr const UInt32 header_size = with_mask ? 4 * sizeof(T) : 2 * sizeof(UInt64);

    const char * const original_dst = dst;

    UInt8 bytes_to_skip = uncompressed_size % sizeof(T);
    memcpy(dst, src, bytes_to_skip);

    uncompressed_size -= bytes_to_skip;
    bytes_size -= bytes_to_skip;
    src += bytes_to_skip;
    dst += bytes_to_skip;

    if (uncompressed_size % sizeof(T) != 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress T64-encoded data, unexpected uncompressed size ({})"
                        " isn't a multiple of the data type size ({})",
                        uncompressed_size, sizeof(T));

    if (uncompressed_size == 0)
        return static_cast<UInt32>(dst - original_dst);

    UInt64 num_elements = uncompressed_size / sizeof(T);
    MinMaxType min;
    MinMaxType max;

    UInt32 num_bits = 0;

    T positive_mask [[maybe_unused]] = 0;
    T negative_mask [[maybe_unused]] = 0;
    T positive [[maybe_unused]] = 0;
    T negative [[maybe_unused]] = 0;
    T negative_bit [[maybe_unused]] = 0;

    /// Read header
    if constexpr (with_mask)
    {
        T header[4] = {0, 0, 0, 0}; /// positive_mask, negative_mask, positive, negative
        memcpy(&header, src, 4 * sizeof(T));
        src += header_size;
        bytes_size -= header_size;

        positive_mask = header[0];
        negative_mask = header[1];
        positive = header[2];
        negative = header[3];

        negative_bit = negative_mask || header[3]; // in-out: has-negtives -> negative-bit-position
        num_bits = getMaskBitsNumber(positive_mask, negative_mask, negative_bit);
    }
    else
    {
        memcpy(&min, src, sizeof(MinMaxType));
        memcpy(&max, src + 8, sizeof(MinMaxType));
        src += header_size;
        bytes_size -= header_size;

        num_bits = getValuableBitsNumber(min, max);
    }

    if (!num_bits)
    {
        T min_value = with_mask ? positive : static_cast<T>(min);
        for (UInt32 i = 0; i < num_elements; ++i, dst += sizeof(T))
            unalignedStore<T>(dst, min_value);
        return static_cast<UInt32>(dst - original_dst);
    }

    UInt32 src_shift = sizeof(UInt64) * num_bits;
    UInt32 dst_shift = sizeof(T) * matrix_size;

    if (!bytes_size || bytes_size % src_shift)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress T64-encoded data, data size ({}) is not a multiplier of {}",
                        bytes_size, src_shift);

    UInt32 num_full = bytes_size / src_shift;
    UInt32 tail = num_elements % matrix_size;
    if (tail)
        --num_full;

    UInt64 expected = static_cast<UInt64>(num_full) * matrix_size + tail;    /// UInt64 to avoid overflow.
    if (expected != num_elements)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress, the number of elements in the compressed data ({})"
                        " is not equal to the expected number of elements in the decompressed data ({})",
                        expected, num_elements);

    T upper_min [[maybe_unused]] = 0;
    T upper_max [[maybe_unused]] = 0;
    T sign_bit [[maybe_unused]] = 0;

    if constexpr (!with_mask)
    {
        if (num_bits < 64)
            upper_min = static_cast<T>(static_cast<UInt64>(min) >> num_bits << num_bits);

        if constexpr (is_signed_v<T>)
        {
            if (min < 0 && max >= 0 && num_bits < 64)
            {
                sign_bit = static_cast<T>(1ull << (num_bits - 1));
                upper_max = static_cast<T>(static_cast<UInt64>(max) >> num_bits << num_bits);
            }
        }
    }

    T buf[matrix_size];
    for (UInt32 i = 0; i < num_full; ++i)
    {
        reverseTranspose<T, full>(src, buf, num_bits);
        if constexpr (with_mask)
            pdep(buf, positive_mask, negative_mask, positive, negative, negative_bit, matrix_size);
        else
            restoreUpperBits(buf, upper_min, upper_max, sign_bit);
        store<T>(buf, dst, matrix_size);
        src += src_shift;
        dst += dst_shift;
    }

    if (tail)
    {
        reverseTranspose<T, full>(src, buf, num_bits, tail);
        if constexpr (with_mask)
            pdep(buf, positive_mask, negative_mask, positive, negative, negative_bit, tail);
        else
            restoreUpperBits(buf, upper_min, upper_max, sign_bit, tail);
        store<T>(buf, dst, tail);
        dst += tail * sizeof(T);
    }

    return static_cast<UInt32>(dst - original_dst);
}

template <typename T>
UInt32 compressData(const char * src, UInt32 src_size, char * dst, Variant variant)
{
    switch (variant)
    {
        case Variant::Byte:
            return compressData<T, false>(src, src_size, dst);
        case Variant::Bit:
            return compressData<T, true>(src, src_size, dst);
        case Variant::ByteMask:
            return compressData<T, false, true>(src, src_size, dst);
        case Variant::BitMask:
            return compressData<T, true, true>(src, src_size, dst);
    }
}

template <typename T>
UInt32 decompressData(const char * src, UInt32 src_size, char * dst, UInt32 uncompressed_size, Variant variant)
{
    switch (variant)
    {
        case Variant::Byte:
            return decompressData<T, false>(src, src_size, dst, uncompressed_size);
        case Variant::Bit:
            return decompressData<T, true>(src, src_size, dst, uncompressed_size);
        case Variant::ByteMask:
            return decompressData<T, false, true>(src, src_size, dst, uncompressed_size);
        case Variant::BitMask:
            return decompressData<T, true, true>(src, src_size, dst, uncompressed_size);
    }
}

}


UInt32 CompressionCodecT64::doCompressData(const char * src, UInt32 src_size, char * dst) const
{
    UInt8 bit_flag = static_cast<UInt8>(static_cast<UInt8>(variant) << 7);
    UInt8 mask_flag = static_cast<UInt8>((static_cast<UInt8>(variant) & 0x2) << 5);
    UInt8 cookie = static_cast<UInt8>(serializeTypeId(type_idx)) | bit_flag | mask_flag;
    memcpy(dst, &cookie, 1);
    dst += 1;
    switch (baseType(*type_idx))
    {
        case TypeIndex::Int8:
            return 1 + compressData<Int8>(src, src_size, dst, variant);
        case TypeIndex::Int16:
            return 1 + compressData<Int16>(src, src_size, dst, variant);
        case TypeIndex::Int32:
            return 1 + compressData<Int32>(src, src_size, dst, variant);
        case TypeIndex::Int64:
            return 1 + compressData<Int64>(src, src_size, dst, variant);
        case TypeIndex::UInt8:
            return 1 + compressData<UInt8>(src, src_size, dst, variant);
        case TypeIndex::UInt16:
            return 1 + compressData<UInt16>(src, src_size, dst, variant);
        case TypeIndex::UInt32:
            return 1 + compressData<UInt32>(src, src_size, dst, variant);
        case TypeIndex::UInt64:
            return 1 + compressData<UInt64>(src, src_size, dst, variant);
        default:
            break;
    }

    throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with T64 codec");
}

UInt32 CompressionCodecT64::doDecompressData(const char * src, UInt32 src_size, char * dst, UInt32 uncompressed_size) const
{
    if (!src_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress T64-encoded data");

    UInt8 cookie = unalignedLoad<UInt8>(src);
    src += 1;
    src_size -= 1;

    UInt8 bit_flag = cookie >> 7;
    UInt8 mask_flag = (cookie >> 5) & 0x2;
    auto saved_variant = static_cast<Variant>(bit_flag | mask_flag);
    TypeIndex saved_type_id = deserializeTypeId(cookie & 0x3F);

    switch (baseType(saved_type_id))
    {
        case TypeIndex::Int8:
            return decompressData<Int8>(src, src_size, dst, uncompressed_size, saved_variant);
        case TypeIndex::Int16:
            return decompressData<Int16>(src, src_size, dst, uncompressed_size, saved_variant);
        case TypeIndex::Int32:
            return decompressData<Int32>(src, src_size, dst, uncompressed_size, saved_variant);
        case TypeIndex::Int64:
            return decompressData<Int64>(src, src_size, dst, uncompressed_size, saved_variant);
        case TypeIndex::UInt8:
            return decompressData<UInt8>(src, src_size, dst, uncompressed_size, saved_variant);
        case TypeIndex::UInt16:
            return decompressData<UInt16>(src, src_size, dst, uncompressed_size, saved_variant);
        case TypeIndex::UInt32:
            return decompressData<UInt32>(src, src_size, dst, uncompressed_size, saved_variant);
        case TypeIndex::UInt64:
            return decompressData<UInt64>(src, src_size, dst, uncompressed_size, saved_variant);
        default:
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress T64-encoded data");
    }
}

uint8_t CompressionCodecT64::getMethodByte() const
{
    return codecId();
}

CompressionCodecT64::CompressionCodecT64(std::optional<TypeIndex> type_idx_, Variant variant_)
    : type_idx(type_idx_)
    , variant(variant_)
{
    switch (variant)
    {
        case Variant::Byte:
            setCodecDescription("T64");
            break;
        case Variant::Bit:
            setCodecDescription("T64", {std::make_shared<ASTLiteral>("bit")});
            break;
        case Variant::ByteMask:
            setCodecDescription("T64", {std::make_shared<ASTLiteral>("byte_mask")});
            break;
        case Variant::BitMask:
            setCodecDescription("T64", {std::make_shared<ASTLiteral>("bit_mask")});
            break;
    }
}

void CompressionCodecT64::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
    hash.update(type_idx.value_or(TypeIndex::Nothing));
    hash.update(variant);
}

void registerCodecT64(CompressionCodecFactory & factory)
{
    auto reg_func = [&](const ASTPtr & arguments, const IDataType * type) -> CompressionCodecPtr
    {
        Variant variant = Variant::Byte;

        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 1)
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "T64 support zero or one parameter, given {}",
                                arguments->children.size());

            const auto children = arguments->children;
            const auto * literal = children[0]->as<ASTLiteral>();
            if (!literal)
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Wrong modification for T64. Expected: 'bit', 'byte', 'bit_mask', 'byte_mask')");
            String name = literal->value.safeGet<String>();

            if (name == "byte")
                variant = Variant::Byte;
            else if (name == "bit")
                variant = Variant::Bit;
            else if (name == "byte_mask")
                variant = Variant::ByteMask;
            else if (name == "bit_mask")
                variant = Variant::BitMask;
            else
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Wrong modification for T64: {}", name);
        }

        std::optional<TypeIndex> type_idx;
        if (type)
        {
            type_idx = typeIdx(type);
            if (type_idx == TypeIndex::Nothing)
                throw Exception(
                    ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "T64 codec is not supported for specified type {}", type->getName());
        }
        return std::make_shared<CompressionCodecT64>(type_idx, variant);
    };

    factory.registerCompressionCodecWithType("T64", codecId(), reg_func);
}
}
