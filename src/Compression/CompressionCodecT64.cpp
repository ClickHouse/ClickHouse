#include <array>
#include <bit>
#include <cstring>
#include <utility>
#include <Compression/CompressionCodecT64Transpose.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/registerCompressionCodecs.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <base/unaligned.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Common/TargetSpecific.h>

namespace DB
{

/// Get 64 integer values, makes 64x64 bit matrix, transpose it and crop unused bits (most significant zeroes).
/// In example, if we have UInt8 with only 0 and 1 inside 64xUInt8 would be compressed into 1xUInt64.
/// It detects unused bits by calculating min and max values of data part, saving them in header in compression phase.
/// There's a special case with signed integers parts with crossing zero data. Here it stores one more bit to detect sign of value.
///
/// With 'frame_of_reference' option, stores each value as its distance from the block minimum,
/// so values that are large but close together compress as if they were small.
class CompressionCodecT64 : public ICompressionCodec
{
public:
    /// On-disk layout per codec invocation:
    ///
    ///   [ cookie            |  COOKIE_SIZE             ]  type id (low 6 bits) | frame_of_reference (bit 6) | variant (high bit)
    ///   [ unaligned prefix  |  input_size % sizeof(T)  ]  leftover bytes that don't fill a full T element, copied verbatim (bytes_to_skip)
    ///   [ min/max header    |  HEADER_SIZE             ]  one (min, max) pair covering the entire matrix portion
    ///   [ matrix block 0    |  8·n_bits                ]  n_bits transposed bit-planes for MATRIX_SIZE elements
    ///   [ matrix block 1    |  ditto                   ]
    ///   ...
    ///   [ matrix block k-1  |  ditto                   ]  last block zero-pads if fewer than MATRIX_SIZE real elements
    ///
    /// n_bits = number of low bits stored per value after dropping high bits common to all values
    /// (with frame_of_reference: the number of bits of `max - min`, values are stored as distances from min).
    /// Edge cases:
    ///   - n_bits = 0 (constant column): min/max header present, no matrix blocks.
    ///   - input_size < sizeof(T): no min/max header, no matrix blocks; output is just cookie + prefix.
    /// MAX_COMPRESSED_BLOCK_SIZE bounds the payload of one matrix block (when n_bits = 64).
    static constexpr UInt32 COOKIE_SIZE = 1;
    static constexpr UInt32 MATRIX_SIZE = 64;
    static constexpr UInt32 HEADER_SIZE = 2 * sizeof(UInt64);
    static constexpr UInt32 MAX_COMPRESSED_BLOCK_SIZE = sizeof(UInt64) * MATRIX_SIZE;

    /// There're 2 compression variants:
    /// Byte - transpose bit matrix by bytes (only the last not full byte is transposed by bits). It's default.
    /// Bits - full bit-transpose of the bit matrix. It uses more resources and leads to better compression with ZSTD (but worse with LZ4).
    enum class Variant : uint8_t
    {
        Byte,
        Bit
    };

    // type_idx, variant, frame_of_reference_ is required for compression, but not for decompression.
    CompressionCodecT64(std::optional<TypeIndex> type_idx_, Variant variant_, bool frame_of_reference_ = false);

    [[nodiscard]] uint8_t getMethodByte() const override;
    [[nodiscard]] ASTPtr getCodecDescription() const override;
    void updateHash(SipHash & hash) const override;
    std::optional<UInt32> tryGetCompressedSize(const char * source, UInt32 source_size) const override;

protected:
    UInt32 doCompressData(const char * src, UInt32 src_size, char * dst) const override;
    UInt32 doDecompressData(const char * src, UInt32 src_size, char * dst, UInt32 uncompressed_size) const override;

    [[nodiscard]] UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override
    {
        return uncompressed_size + MAX_COMPRESSED_BLOCK_SIZE + COOKIE_SIZE + HEADER_SIZE;
    }

    [[nodiscard]] bool isCompression() const override { return true; }
    [[nodiscard]] bool isGenericCompression() const override { return false; }
    [[nodiscard]] String getDescription() const override
    {
        return "Preprocessor. Crops unused bits; puts them into a 64x64 bit matrix; optimized for 64-bit data types.";
    }

private:
    // type_idx, variant, frame_of_reference_ is required for compression, but not for decompression.
    std::optional<TypeIndex> type_idx;
    Variant variant;
    bool frame_of_reference;
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
/// Max 64 int types (the cookie keeps 6 bits for the type id).
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
using UnsignedOf = std::make_unsigned_t<T>;

/// Two's complement maps
///
///   unsigned:  0    1  ...  127  128  129  ...  255
///   signed:    0    1  ...  127 -128 -127  ...   -1
///
/// Subtracting min measures the distance between two positions on this circle.
///
///   both negative:  value= -50, umin=-100 -> 11001110 - 10011100 = 00110010 = 50
///   cross zero:     value=  50, umin=-100 -> 00110010 - 10011100 = 10010110 = 150
///   both positive:  value= 100, umin=  50 -> 01100100 - 00110010 = 00110010 = 50
///
/// We cast to unsigned to make wraparound well-defined.
///
/// __restrict__ tells the compiler src and buf never overlap, removes
/// aliasing check the autovectorizer otherwise emits for char* parameters and inlines
MULTITARGET_FUNCTION_X86_V4(
MULTITARGET_FUNCTION_HEADER(
template <typename T>
void), loadDeltaImpl, MULTITARGET_FUNCTION_BODY((const char * __restrict__ src, UnsignedOf<T> * __restrict__ buf, T min_val, UInt32 tail) /// NOLINT
{
    using U = UnsignedOf<T>;
    U umin = static_cast<U>(min_val);
    if constexpr (std::endian::native == std::endian::little)
    {
        for (UInt32 i = 0; i < tail; ++i)
            buf[i] = static_cast<U>(unalignedLoad<T>(src + i * sizeof(T))) - umin;
    }
    else
    {
        for (UInt32 i = 0; i < tail; ++i)
            buf[i] = static_cast<U>(unalignedLoadLittleEndian<T>(src + i * sizeof(T))) - umin;
    }
})
)

template <typename T>
ALWAYS_INLINE void loadDelta(const char * src, UnsignedOf<T> * buf, T min_val, UInt32 tail = 64)
{
#if USE_X86_64_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        loadDeltaImpl_x86_64_v4<T>(src, buf, min_val, tail);
        return;
    }
#endif
    {
        loadDeltaImpl<T>(src, buf, min_val, tail);
    }
}

/// Restore original values by adding min back to each unsigned delta.
///
/// Adding min moves each value back to its original position on the circle.
/// Any carry bit beyond 2^N is simply discarded, giving back the original bits:
///
/// both negative:
/// delta= 50, umin=156 -> 00110010 + 10011100 = (0)11001110 -> 11001110 = Int8  -50
/// cross zero:
/// delta=150, umin=156 -> 10010110 + 10011100 = (1)00110010 -> 00110010 = Int8   50
/// both positive:
/// delta= 50, umin=50 -> 00110010 + 00110010 = (0)01100100 -> 01100100 = Int8  100
/// Same as above: buf and dst are always separate allocations at every call site.
MULTITARGET_FUNCTION_X86_V4(
MULTITARGET_FUNCTION_HEADER(
template <typename T>
void), storeDeltaImpl, MULTITARGET_FUNCTION_BODY((const UnsignedOf<T> * __restrict__ buf, char * __restrict__ dst, T min_val, UInt32 tail) /// NOLINT
{
    using U = UnsignedOf<T>;
    U umin = static_cast<U>(min_val);
    for (UInt32 i = 0; i < tail; ++i)
        unalignedStore<T>(dst + i * sizeof(T), static_cast<T>(buf[i] + umin));
})
)

template <typename T>
ALWAYS_INLINE void storeDelta(const UnsignedOf<T> * buf, char * dst, T min_val, UInt32 tail = 64)
{
#if USE_X86_64_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        storeDeltaImpl_x86_64_v4<T>(buf, dst, min_val, tail);
        return;
    }
#endif
    {
        storeDeltaImpl<T>(buf, dst, min_val, tail);
    }
}

/// `matrix8[64 * byte + col]` = byte-th byte of `src[col]`, for a full matrix of 8-byte values. One
/// iteration transposes the 8 columns whose bytes occupy one 64-byte group, then spreads the
/// resulting rows across the eight matrix lines they belong to.

MULTITARGET_FUNCTION_X86_V4(
MULTITARGET_FUNCTION_HEADER(
template <typename T, bool full>
void), transposeImpl, MULTITARGET_FUNCTION_BODY((const T * src, char * dst, UInt32 num_bits, UInt32 tail) /// NOLINT
{
    UInt32 full_bytes = num_bits / 8;
    UInt32 part_bits = num_bits % 8;

    UInt64 matrix[64] = {};
    T64Transpose::active::transposeMatrixBytes(src, matrix, tail);

    if constexpr (full)
    {
        UInt64 * matrix_line = matrix;
        for (UInt32 byte = 0; byte < full_bytes; ++byte, matrix_line += 8)
            T64Transpose::active::transpose64x8(matrix_line);
    }

    UInt32 full_size = sizeof(UInt64) * (num_bits - part_bits);
    memcpy(dst, matrix, full_size);
    dst += full_size;

    /// transpose only partially filled last byte
    if (part_bits)
    {
        UInt64 * matrix_line = &matrix[full_bytes * 8];
        T64Transpose::active::transpose64x8(matrix_line);
        memcpy(dst, matrix_line, part_bits * sizeof(UInt64));
    }
})
)

/// UIntX[64] -> UInt64[N] transposed matrix, N <= X
template <typename T, bool full = false>
ALWAYS_INLINE void transpose(const T * src, char * dst, UInt32 num_bits, UInt32 tail = 64)
{
#if USE_X86_64_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        transposeImpl_x86_64_v4<T, full>(src, dst, num_bits, tail);
        return;
    }
#endif
    {
        transposeImpl<T, full>(src, dst, num_bits, tail);
    }
}

/// one_bit_expansion[b][j] = bit j of byte b.
/// With one stored bit, eight consecutive values share a byte. The row unpacks it with one 8-byte load instead of eight shifts.
constexpr auto one_bit_expansion = []
{
    std::array<std::array<UInt8, 8>, 256> table{};
    for (size_t value = 0; value < table.size(); ++value)
        for (size_t bit = 0; bit < 8; ++bit)
            table[value][bit] = (value >> bit) & 1;
    return table;
}();

/// `num_bits == 1` (flags, booleans) is common. With one stored bit, there are no planes to transpose, so the transpose is skipped.
/// Tightly vectorised. Better not to touch this function unless you really know what you are doing.
template <typename T>
NO_INLINE void decompressOneBit(const char * src, char * dst, UInt32 num_elements, T common_negative, T common_positive, T sign_bit)
{
    const UInt32 full_bytes = num_elements / 8;
    /// The loop within is vectorised. Vectorising this outer loop gave `Int8`, `Int16` and `Int32` a second copy that spilled registers.
#pragma clang loop vectorize(disable)
    for (UInt32 i = 0; i < full_bytes; ++i)
    {
        UInt32 byte_index = i;
        if constexpr (std::endian::native == std::endian::big)
            byte_index ^= 7;
        const auto & values = one_bit_expansion[static_cast<UInt8>(src[byte_index])];
        for (UInt32 bit = 0; bit < 8; ++bit)
        {
            T value = T64Transpose::restoreCommonBits(static_cast<T>(values[bit]), common_negative, common_positive, sign_bit);
            unalignedStore<T>(dst + bit * sizeof(T), value);
        }
        dst += 8 * sizeof(T);
    }

    const UInt32 tail = num_elements % 8;
    if (tail)
    {
        UInt32 byte_index = full_bytes;
        if constexpr (std::endian::native == std::endian::big)
            byte_index ^= 7;
        const auto & values = one_bit_expansion[static_cast<UInt8>(src[byte_index])];
        for (UInt32 bit = 0; bit < tail; ++bit)
        {
            T value = T64Transpose::restoreCommonBits(static_cast<T>(values[bit]), common_negative, common_positive, sign_bit);
            unalignedStore<T>(dst + bit * sizeof(T), value);
        }
    }
}

using MatrixLine [[gnu::vector_size(64)]] = UInt64;

ALWAYS_INLINE void loadPartialLine(MatrixLine & line, const char * src, UInt32 part)
{
    switch (part)
    {
        case 7: line[6] = unalignedLoad<UInt64>(src + 48); [[fallthrough]];
        case 6: line[5] = unalignedLoad<UInt64>(src + 40); [[fallthrough]];
        case 5: line[4] = unalignedLoad<UInt64>(src + 32); [[fallthrough]];
        case 4: line[3] = unalignedLoad<UInt64>(src + 24); [[fallthrough]];
        case 3: line[2] = unalignedLoad<UInt64>(src + 16); [[fallthrough]];
        case 2: line[1] = unalignedLoad<UInt64>(src + 8); [[fallthrough]];
        case 1: line[0] = unalignedLoad<UInt64>(src); [[fallthrough]];
        default: break;
    }
}

template <size_t N>
ALWAYS_INLINE void loadPlanes(UInt64 (&matrix)[N], const char * src, UInt32 num_bits)
{
    static_assert(N % 8 == 0);
    if constexpr (N == 8)
    {
        if (num_bits == N)
        {
            memcpy(matrix, src, sizeof(matrix));
            return;
        }
        for (UInt32 i = 0; i < N; ++i)
            matrix[i] = i < num_bits ? unalignedLoad<UInt64>(src + i * sizeof(UInt64)) : 0;
    }
    else
    {
        const UInt32 full_lines = num_bits / 8;
#pragma clang loop unroll(full)
        for (UInt32 line_index = 0; line_index < N / 8; ++line_index)
        {
            MatrixLine line = {};
            if (line_index < full_lines)
                memcpy(&line, src + sizeof(MatrixLine) * line_index, sizeof(line));
            else if (line_index == full_lines)
                loadPartialLine(line, src + sizeof(MatrixLine) * line_index, num_bits % 8);
            memcpy(&matrix[8 * line_index], &line, sizeof(line));
        }
    }
}

MULTITARGET_FUNCTION_X86_V4(
MULTITARGET_FUNCTION_HEADER(
template <typename T, bool full>
void), reverseTransposeImpl, MULTITARGET_FUNCTION_BODY((
    const char * src, char * dst, UInt32 num_bits, T common_negative, T common_positive, T sign_bit, UInt32 tail) /// NOLINT
{
    UInt32 part_bits = num_bits % 8;

    /// Small ranges often need at most eight stored bits.
    /// A 64-byte matrix avoids clearing unused planes and reconstructing zero high bytes.
    if (num_bits <= 8)
    {
        alignas(64) UInt64 matrix[8];
        loadPlanes(matrix, src, num_bits);

        /// Always the plane loop. The shuffle measured slower here even at six to eight planes.
        if (full || part_bits)
            T64Transpose::reverseTransposePlanes(matrix, num_bits);

        const auto * values = reinterpret_cast<const unsigned char *>(matrix);
        for (UInt32 col = 0; col < tail; ++col)
        {
            T value = static_cast<T>(values[col]);
            value = T64Transpose::restoreCommonBits(value, common_negative, common_positive, sign_bit);
            unalignedStore<T>(dst + col * sizeof(T), value);
        }
        return;
    }

    /// Aligned to its size, so that the matrix never straddles a page boundary. Otherwise, depending on the stack layout,
    /// the vector stores filling it may split across two pages on every call, which is very slow.
    alignas(sizeof(UInt64) * 64) UInt64 matrix[64];
    loadPlanes(matrix, src, num_bits);

    UInt32 full_bytes = num_bits / 8;

    if constexpr (full)
    {
        UInt64 * matrix_line = matrix;
        for (UInt32 byte = 0; byte < full_bytes; ++byte, matrix_line += 8)
            T64Transpose::active::reverseTranspose64x8(matrix_line);
    }

    if (part_bits)
    {
        UInt64 * matrix_line = &matrix[full_bytes * 8];
        /// The shuffle wins from four planes in the bit variant (`full`) and loses at every count in the byte variant.
        if (full && part_bits >= 4)
            T64Transpose::active::reverseTranspose64x8(matrix_line);
        else
            T64Transpose::reverseTransposePlanes(matrix_line, part_bits);
    }

    T64Transpose::active::reverseTransposeMatrixBytes(matrix, dst, tail, common_negative, common_positive, sign_bit);
})
)

/// UInt64[N] transposed matrix -> T[tail], upper bits restored
template <typename T, bool full = false>
ALWAYS_INLINE void reverseTranspose(const char * src, char * dst, UInt32 num_bits, T common_negative, T common_positive, T sign_bit, UInt32 tail = 64)
{
#if USE_X86_64_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        reverseTransposeImpl_x86_64_v4<T, full>(src, dst, num_bits, common_negative, common_positive, sign_bit, tail);
        return;
    }
#endif
    {
        reverseTransposeImpl<T, full>(src, dst, num_bits, common_negative, common_positive, sign_bit, tail);
    }
}


UInt32 getValuableBitsNumber(UInt64 min, UInt64 max)
{
    UInt64 diff_bits = min ^ max;
    if (diff_bits)
        return 64 - std::countl_zero(diff_bits);
    return 0;
}

// SIGNED cross-zero (min=-5, max=10): XOR breaks
// -5  = 1111...11111011
// 10  = 0000...00001010
// XOR = 1111...11110001 -> sign bit pollutes, says 64 bits
// so: which side needs more bits?
//   min+max >= 0 -> positive side larger
//   count bits of max: 10 = 1010 -> 4 bits + 1 sign = 5
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

// Frame of reference:
// range = max - min = 10 - (-5) = 15 -> 4 bits, no special case
UInt32 getDeltaBitsNumber(UInt64 range)
{
    if (range)
        return 64 - std::countl_zero(range);
    return 0;
}

/// The number of bits stored per value, for the block with the given min and max.
template <typename T, bool frame_of_reference, typename MinMax>
UInt32 getStoredBitsNumber(MinMax min64, MinMax max64)
{
    if constexpr (frame_of_reference)
    {
        using U = UnsignedOf<T>;
        U delta_range = static_cast<U>(static_cast<T>(max64)) - static_cast<U>(static_cast<T>(min64));
        return getDeltaBitsNumber(static_cast<UInt64>(delta_range));
    }
    else
    {
        return getValuableBitsNumber(min64, max64);
    }
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


using Variant = CompressionCodecT64::Variant;

template <typename T>
using MinMaxType = std::conditional_t<is_signed_v<T>, Int64, UInt64>;

template <typename T>
struct T64Layout
{
    UInt8 bytes_to_skip = 0;
    UInt32 bytes_to_compress = 0;
    UInt32 full_matrices_count = 0;
    UInt32 tail_elements = 0;
    UInt32 valuable_bits = 0;
    MinMaxType<T> min64 = 0;
    MinMaxType<T> max64 = 0;
    UInt32 total_size = 0;
};

template <typename T, bool frame_of_reference>
T64Layout<T> computeT64Layout(const char * src, UInt32 bytes_size)
{
    T64Layout<T> layout;
    layout.bytes_to_skip = bytes_size % sizeof(T);
    layout.bytes_to_compress = bytes_size - layout.bytes_to_skip;

    if (layout.bytes_to_compress == 0)
    {
        layout.total_size = layout.bytes_to_skip;
        return layout;
    }

    const UInt32 src_size = layout.bytes_to_compress / sizeof(T);
    layout.full_matrices_count = src_size / CompressionCodecT64::MATRIX_SIZE;
    layout.tail_elements = src_size % CompressionCodecT64::MATRIX_SIZE;

    T min;
    T max;
    findMinMax<T>(src + layout.bytes_to_skip, layout.bytes_to_compress, min, max);
    layout.min64 = static_cast<MinMaxType<T>>(min);
    layout.max64 = static_cast<MinMaxType<T>>(max);

    layout.valuable_bits = getStoredBitsNumber<T, frame_of_reference>(layout.min64, layout.max64);
    if (layout.valuable_bits == 0)
    {
        layout.total_size = CompressionCodecT64::HEADER_SIZE + layout.bytes_to_skip;
        return layout;
    }

    const UInt32 dst_shift = sizeof(UInt64) * layout.valuable_bits;
    const UInt32 dst_bytes = layout.full_matrices_count * dst_shift + (layout.tail_elements ? dst_shift : 0);
    layout.total_size = CompressionCodecT64::HEADER_SIZE + dst_bytes + layout.bytes_to_skip;
    return layout;
}

template <typename T, bool full, bool frame_of_reference>
UInt32 compressData(const char * src, UInt32 bytes_size, char * dst)
{
    const T64Layout<T> layout = computeT64Layout<T, frame_of_reference>(src, bytes_size);

    memcpy(dst, src, layout.bytes_to_skip);
    src += layout.bytes_to_skip;
    dst += layout.bytes_to_skip;

    if (layout.bytes_to_compress == 0)
        return layout.total_size;

    /// Write header
    memcpy(dst, &layout.min64, sizeof(MinMaxType<T>));
    memcpy(dst + 8, &layout.max64, sizeof(MinMaxType<T>));
    dst += CompressionCodecT64::HEADER_SIZE;

    if (layout.valuable_bits == 0)
        return layout.total_size;

    const UInt32 src_shift = sizeof(T) * CompressionCodecT64::MATRIX_SIZE;
    const UInt32 dst_shift = sizeof(UInt64) * layout.valuable_bits;

    if constexpr (frame_of_reference)
    {
        using U = UnsignedOf<T>;
        const T min_val = static_cast<T>(layout.min64);
        U delta_buf[CompressionCodecT64::MATRIX_SIZE];
        for (UInt32 i = 0; i < layout.full_matrices_count; ++i)
        {
            loadDelta<T>(src, delta_buf, min_val, CompressionCodecT64::MATRIX_SIZE);
            transpose<U, full>(delta_buf, dst, layout.valuable_bits);
            src += src_shift;
            dst += dst_shift;
        }

        if (layout.tail_elements)
        {
            loadDelta<T>(src, delta_buf, min_val, layout.tail_elements);
            transpose<U, full>(delta_buf, dst, layout.valuable_bits, layout.tail_elements);
        }
    }
    else
    {
        T buf[CompressionCodecT64::MATRIX_SIZE];
        for (UInt32 i = 0; i < layout.full_matrices_count; ++i)
        {
            load<T>(src, buf, CompressionCodecT64::MATRIX_SIZE);
            transpose<T, full>(buf, dst, layout.valuable_bits);
            src += src_shift;
            dst += dst_shift;
        }

        if (layout.tail_elements)
        {
            load<T>(src, buf, layout.tail_elements);
            transpose<T, full>(buf, dst, layout.valuable_bits, layout.tail_elements);
        }
    }

    return layout.total_size;
}

/// Frame of reference: values are stored as distances from min, so num_bits covers
/// only the range [0, max-min]. storeDelta adds min back on decompression.
///
/// No frame-of-reference adjustment: num_bits is derived via XOR of min and max, which may require
/// an extra sign bit for signed types spanning zero. On decompression, the
/// stripped upper bits must be restored - either uniformly from upper_min,
/// or conditionally from upper_max for the cross-zero signed case.
template <typename T, bool full, bool frame_of_reference>
UInt32 decompressData(const char * src, UInt32 bytes_size, char * dst, UInt32 uncompressed_size)
{
    const char * const original_dst = dst;
    UInt8 bytes_to_skip = uncompressed_size % sizeof(T);
    if (bytes_to_skip > bytes_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress T64-encoded data: compressed size ({}) is smaller"
                        " than the trailing unaligned bytes ({})", bytes_size, static_cast<UInt32>(bytes_to_skip));
    memcpy(dst, src, bytes_to_skip);

    uncompressed_size -= bytes_to_skip;
    bytes_size -= bytes_to_skip;
    src += bytes_to_skip;
    dst += bytes_to_skip;

    if (uncompressed_size == 0)
        return static_cast<UInt32>(dst - original_dst);

    UInt64 num_elements = uncompressed_size / sizeof(T);
    MinMaxType<T> min;
    MinMaxType<T> max;

    /// Read header
    {
        if (bytes_size < CompressionCodecT64::HEADER_SIZE)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress T64-encoded data: compressed size ({}) is too small"
                            " to contain the min/max header ({} bytes)", bytes_size, CompressionCodecT64::HEADER_SIZE);
        memcpy(&min, src, sizeof(MinMaxType<T>));
        memcpy(&max, src + 8, sizeof(MinMaxType<T>));
        src += CompressionCodecT64::HEADER_SIZE;
        bytes_size -= CompressionCodecT64::HEADER_SIZE;
    }

    UInt32 num_bits = getStoredBitsNumber<T, frame_of_reference>(min, max);
    if (!num_bits)
    {
        T min_value = static_cast<T>(min);
        for (UInt32 i = 0; i < num_elements; ++i, dst += sizeof(T))
            unalignedStore<T>(dst, min_value);
        return static_cast<UInt32>(dst - original_dst);
    }

    UInt32 src_shift = sizeof(UInt64) * num_bits;
    UInt32 dst_shift = sizeof(T) * CompressionCodecT64::MATRIX_SIZE;

    if (!bytes_size || bytes_size % src_shift)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress T64-encoded data, data size ({}) is not a multiplier of {}",
                        bytes_size, src_shift);

    UInt32 num_full = bytes_size / src_shift;
    UInt32 tail = num_elements % CompressionCodecT64::MATRIX_SIZE;
    if (tail)
        --num_full;

    UInt64 expected = static_cast<UInt64>(num_full) * CompressionCodecT64::MATRIX_SIZE + tail;    /// UInt64 to avoid overflow.
    if (expected != num_elements)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress, the number of elements in the compressed data ({})"
                        " is not equal to the expected number of elements in the decompressed data ({})",
                        expected, num_elements);

    if constexpr (frame_of_reference)
    {
        /// The stored values are unsigned distances from min, so no common upper bits are restored here.
        using U = UnsignedOf<T>;
        const T min_val = static_cast<T>(min);
        U delta_buf[CompressionCodecT64::MATRIX_SIZE];
        for (UInt32 i = 0; i < num_full; ++i)
        {
            reverseTranspose<U, full>(src, reinterpret_cast<char *>(delta_buf), num_bits, U(0), U(0), U(0));
            storeDelta<T>(delta_buf, dst, min_val, CompressionCodecT64::MATRIX_SIZE);
            src += src_shift;
            dst += dst_shift;
        }

        if (tail)
        {
            reverseTranspose<U, full>(src, reinterpret_cast<char *>(delta_buf), num_bits, U(0), U(0), U(0), tail);
            storeDelta<T>(delta_buf, dst, min_val, tail);
            dst += tail * sizeof(T);
        }

        return static_cast<UInt32>(dst - original_dst);
    }

    T common_negative = 0;
    T common_positive = 0;
    T sign_bit = 0;
    if (num_bits < 64)
        common_negative = static_cast<T>(static_cast<UInt64>(min) >> num_bits << num_bits);

    if constexpr (is_signed_v<T>)
    {
        if (min < 0 && max >= 0 && num_bits < 64)
        {
            sign_bit = static_cast<T>(1ull << (num_bits - 1));
            common_positive = static_cast<T>(static_cast<UInt64>(max) >> num_bits << num_bits);
        }
    }

    if (num_bits == 1)
    {
        decompressOneBit(src, dst, static_cast<UInt32>(num_elements), common_negative, common_positive, sign_bit);
        dst += uncompressed_size;
        return static_cast<UInt32>(dst - original_dst);
    }

    for (UInt32 i = 0; i < num_full; ++i)
    {
        reverseTranspose<T, full>(src, dst, num_bits, common_negative, common_positive, sign_bit);
        src += src_shift;
        dst += dst_shift;
    }

    if (tail)
    {
        reverseTranspose<T, full>(src, dst, num_bits, common_negative, common_positive, sign_bit, tail);
        dst += tail * sizeof(T);
    }

    return static_cast<UInt32>(dst - original_dst);
}

template <typename T>
UInt32 compressData(const char * src, UInt32 src_size, char * dst, Variant variant, bool do_frame_of_reference)
{
    if (do_frame_of_reference)
    {
        if (variant == Variant::Bit)
            return compressData<T, true, true>(src, src_size, dst);
        return compressData<T, false, true>(src, src_size, dst);
    }

    if (variant == Variant::Bit)
        return compressData<T, true, false>(src, src_size, dst);
    return compressData<T, false, false>(src, src_size, dst);
}

template <typename T>
UInt32 calculateCompressedDataSize(const char * src, UInt32 bytes_size, bool do_frame_of_reference)
{
    if (do_frame_of_reference)
        return computeT64Layout<T, true>(src, bytes_size).total_size;
    return computeT64Layout<T, false>(src, bytes_size).total_size;
}

template <typename T>
UInt32 decompressData(const char * src, UInt32 src_size, char * dst, UInt32 uncompressed_size, Variant variant, bool do_frame_of_reference)
{
    if (do_frame_of_reference)
    {
        if (variant == Variant::Bit)
            return decompressData<T, true, true>(src, src_size, dst, uncompressed_size);
        return decompressData<T, false, true>(src, src_size, dst, uncompressed_size);
    }

    if (variant == Variant::Bit)
        return decompressData<T, true, false>(src, src_size, dst, uncompressed_size);
    return decompressData<T, false, false>(src, src_size, dst, uncompressed_size);
}

}


std::optional<UInt32> CompressionCodecT64::tryGetCompressedSize(const char * source, UInt32 source_size) const
{
    if (!type_idx.has_value())
        return std::nullopt;

    /// Cookie byte + per-type payload (matches doCompressData output)
    switch (baseType(*type_idx))
    {
        case TypeIndex::Int8:
            return COOKIE_SIZE + calculateCompressedDataSize<Int8>(source, source_size, frame_of_reference);
        case TypeIndex::Int16:
            return COOKIE_SIZE + calculateCompressedDataSize<Int16>(source, source_size, frame_of_reference);
        case TypeIndex::Int32:
            return COOKIE_SIZE + calculateCompressedDataSize<Int32>(source, source_size, frame_of_reference);
        case TypeIndex::Int64:
            return COOKIE_SIZE + calculateCompressedDataSize<Int64>(source, source_size, frame_of_reference);
        case TypeIndex::UInt8:
            return COOKIE_SIZE + calculateCompressedDataSize<UInt8>(source, source_size, frame_of_reference);
        case TypeIndex::UInt16:
            return COOKIE_SIZE + calculateCompressedDataSize<UInt16>(source, source_size, frame_of_reference);
        case TypeIndex::UInt32:
            return COOKIE_SIZE + calculateCompressedDataSize<UInt32>(source, source_size, frame_of_reference);
        case TypeIndex::UInt64:
            return COOKIE_SIZE + calculateCompressedDataSize<UInt64>(source, source_size, frame_of_reference);
        default:
            return std::nullopt;
    }
}

UInt32 CompressionCodecT64::doCompressData(const char * src, UInt32 src_size, char * dst) const
{
    /// Cookie layout (1 byte):
    ///   bit 7: Variant (0=Byte, 1=Bit)
    ///   bit 6: frame_of_reference flag (0=original, 1=frame_of_reference)
    ///   bits 0-5: MagicNumber (type id)
    UInt8 cookie = static_cast<UInt8>(serializeTypeId(type_idx))
                 | static_cast<UInt8>(static_cast<UInt8>(variant) << 7)
                 | static_cast<UInt8>(static_cast<UInt8>(frame_of_reference) << 6);
    memcpy(dst, &cookie, COOKIE_SIZE);
    dst += COOKIE_SIZE;
    switch (baseType(*type_idx))
    {
        case TypeIndex::Int8:
            return COOKIE_SIZE + compressData<Int8>(src, src_size, dst, variant, frame_of_reference);
        case TypeIndex::Int16:
            return COOKIE_SIZE + compressData<Int16>(src, src_size, dst, variant, frame_of_reference);
        case TypeIndex::Int32:
            return COOKIE_SIZE + compressData<Int32>(src, src_size, dst, variant, frame_of_reference);
        case TypeIndex::Int64:
            return COOKIE_SIZE + compressData<Int64>(src, src_size, dst, variant, frame_of_reference);
        case TypeIndex::UInt8:
            return COOKIE_SIZE + compressData<UInt8>(src, src_size, dst, variant, frame_of_reference);
        case TypeIndex::UInt16:
            return COOKIE_SIZE + compressData<UInt16>(src, src_size, dst, variant, frame_of_reference);
        case TypeIndex::UInt32:
            return COOKIE_SIZE + compressData<UInt32>(src, src_size, dst, variant, frame_of_reference);
        case TypeIndex::UInt64:
            return COOKIE_SIZE + compressData<UInt64>(src, src_size, dst, variant, frame_of_reference);
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
    src += COOKIE_SIZE;
    src_size -= COOKIE_SIZE;

    auto saved_variant = static_cast<Variant>((cookie >> 7) & 0x1);
    auto saved_frame_of_reference = static_cast<bool>((cookie >> 6) & 0x1);
    TypeIndex saved_type_id = deserializeTypeId(cookie & 0x3F);

    switch (baseType(saved_type_id))
    {
        case TypeIndex::Int8:
            return decompressData<Int8>(src, src_size, dst, uncompressed_size, saved_variant, saved_frame_of_reference);
        case TypeIndex::Int16:
            return decompressData<Int16>(src, src_size, dst, uncompressed_size, saved_variant, saved_frame_of_reference);
        case TypeIndex::Int32:
            return decompressData<Int32>(src, src_size, dst, uncompressed_size, saved_variant, saved_frame_of_reference);
        case TypeIndex::Int64:
            return decompressData<Int64>(src, src_size, dst, uncompressed_size, saved_variant, saved_frame_of_reference);
        case TypeIndex::UInt8:
            return decompressData<UInt8>(src, src_size, dst, uncompressed_size, saved_variant, saved_frame_of_reference);
        case TypeIndex::UInt16:
            return decompressData<UInt16>(src, src_size, dst, uncompressed_size, saved_variant, saved_frame_of_reference);
        case TypeIndex::UInt32:
            return decompressData<UInt32>(src, src_size, dst, uncompressed_size, saved_variant, saved_frame_of_reference);
        case TypeIndex::UInt64:
            return decompressData<UInt64>(src, src_size, dst, uncompressed_size, saved_variant, saved_frame_of_reference);
        default:
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress T64-encoded data");
    }
}

uint8_t CompressionCodecT64::getMethodByte() const
{
    return codecId();
}

CompressionCodecT64::CompressionCodecT64(std::optional<TypeIndex> type_idx_, Variant variant_, bool frame_of_reference_)
    : type_idx(type_idx_)
    , variant(variant_)
    , frame_of_reference(frame_of_reference_)
{
}

ASTPtr CompressionCodecT64::getCodecDescription() const
{
    /// T64, T64('bit'), T64(true), T64('bit', true)
    ASTs params;
    if (variant == Variant::Bit)
        params.push_back(make_intrusive<ASTLiteral>("bit"));
    if (frame_of_reference)
        params.push_back(make_intrusive<ASTLiteral>(true));

    if (params.empty())
        return makeCodecDescription("T64");
    return makeCodecDescription("T64", params);
}

void CompressionCodecT64::updateHash(SipHash & hash) const
{
    getCodecDescription()->updateTreeHash(hash, /*ignore_aliases=*/ true);
    hash.update(type_idx.value_or(TypeIndex::Nothing));
    hash.update(variant);
    hash.update(frame_of_reference);
}

namespace
{

bool isApplicableToType(const IDataType & type)
{
    return baseType(type.getTypeId()) != TypeIndex::Nothing;
}

}

void registerCodecT64(CompressionCodecFactory & factory)
{
    auto reg_func = [&](const ASTPtr & arguments, const IDataType * type) -> CompressionCodecPtr
    {
        Variant variant = Variant::Byte;
        bool frame_of_reference = false;

        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 2)
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "T64 supports zero, one, or two parameters, given {}",
                                arguments->children.size());

            for (const auto & child : arguments->children)
            {
                const auto * literal = child->as<ASTLiteral>();
                if (!literal)
                    throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Wrong parameter for T64. Expected: 'bit', 'byte', or a boolean");

                if (literal->value.getType() == Field::Types::Bool)
                {
                    frame_of_reference = literal->value.safeGet<bool>();
                }
                else
                {
                    String name = literal->value.safeGet<String>();
                    /// The variant is a keyword and is matched case-insensitively, like the codec name itself.
                    if (equalsCaseInsensitive(name, "byte"))
                        variant = Variant::Byte;
                    else if (equalsCaseInsensitive(name, "bit"))
                        variant = Variant::Bit;
                    else
                        throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Wrong parameter for T64: {}", name);
                }
            }
        }

        std::optional<TypeIndex> type_idx;
        if (type)
        {
            type_idx = type->getTypeId();
            if (!isApplicableToType(*type))
                throw Exception(
                    ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "T64 codec is not supported for specified type {}", type->getName());
        }
        return std::make_shared<CompressionCodecT64>(type_idx, variant, frame_of_reference);
    };

    factory.registerCompressionCodecWithType("T64", codecId(), reg_func, {.is_applicable_to_type = isApplicableToType});
}
}
