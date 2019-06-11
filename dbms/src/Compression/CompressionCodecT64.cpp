#include <cstring>

#include <Compression/CompressionCodecT64.h>
#include <Compression/CompressionFactory.h>
#include <common/unaligned.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
extern const int ILLEGAL_CODEC_PARAMETER;
extern const int LOGICAL_ERROR;
}

namespace
{

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
        case TypeIndex::Decimal32:
            return TypeIndex::Int32;
        case TypeIndex::Int64:
        case TypeIndex::Decimal64:
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
            return TypeIndex::UInt32;
        case TypeIndex::UInt64:
            return TypeIndex::UInt64;
        default:
            break;
    }

    return TypeIndex::Nothing;
}

TypeIndex typeIdx(const DataTypePtr & data_type)
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
        case TypeIndex::Int32:
        case TypeIndex::UInt32:
        case TypeIndex::DateTime:
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

void transpose64x8(const UInt64 * src, UInt64 * dst, UInt32)
{
    auto * src8 = reinterpret_cast<const UInt8 *>(src);

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
}

void revTranspose64x8(const UInt64 * src, UInt64 * dst, UInt32)
{
    auto * dst8 = reinterpret_cast<UInt8 *>(dst);

    for (UInt32 i = 0; i < 64; ++i)
    {
        dst8[i] = ((src[0] >> i) & 0x1)
            | (((src[1] >> i) & 0x1) << 1)
            | (((src[2] >> i) & 0x1) << 2)
            | (((src[3] >> i) & 0x1) << 3)
            | (((src[4] >> i) & 0x1) << 4)
            | (((src[5] >> i) & 0x1) << 5)
            | (((src[6] >> i) & 0x1) << 6)
            | (((src[7] >> i) & 0x1) << 7);
    }
}

/// UIntX[64] -> UInt64[N] transposed matrix, N <= X
template <typename _T>
void transpose(const char * src, char * dst, UInt32 num_bits, UInt32 tail = 64)
{
    UInt32 full_bytes = num_bits / 8;
    UInt32 part_bits = num_bits % 8;
    UInt32 meaning_bytes = full_bytes + (part_bits ? 1 : 0);

    UInt64 mx[64] = {};
    for (UInt32 row = 0; row < meaning_bytes; ++row)
    {
        UInt8 * same_byte_data = reinterpret_cast<UInt8 *>(&mx[row * 8]);
        const char * byte = src + row;
        for (UInt32 col = 0; col < tail; ++col, byte += sizeof(_T))
            same_byte_data[col] = unalignedLoad<UInt8>(byte);
    }

    UInt32 full_size = sizeof(UInt64) * (num_bits - part_bits);
    memcpy(dst, mx, full_size);
    dst += full_size;

    /// transpose only partially filled last byte
    if (part_bits)
    {
        UInt64 * partial = &mx[full_bytes * 8];
        UInt64 res[8] = {};
        transpose64x8(partial, res, part_bits);
        memcpy(dst, res, part_bits * sizeof(UInt64));
    }
}

/// UInt64[N] transposed matrix -> UIntX[64]
template <typename _T, typename _MinMaxT = std::conditional_t<std::is_signed_v<_T>, Int64, UInt64>>
void revTranspose(const char * src, char * dst, UInt32 num_bits, _MinMaxT min, _MinMaxT max [[maybe_unused]], UInt32 tail = 64)
{
    UInt64 mx[64] = {};
    memcpy(mx, src, num_bits * sizeof(UInt64));

    UInt32 full_bytes = num_bits / 8;
    UInt32 part_bits = num_bits % 8;
    UInt32 meaning_bytes = full_bytes + (part_bits ? 1 : 0);
    UInt64 * partial = &mx[full_bytes * 8];

    if (part_bits)
    {
        UInt64 res[8] = {};
        revTranspose64x8(partial, res, part_bits);
        memcpy(partial, res, 8 * sizeof(UInt64));
    }

    auto * mx8 = reinterpret_cast<const UInt8 *>(mx);

    _T upper_min = 0;
    if (num_bits < 64)
        upper_min = min >> num_bits << num_bits;

    _T buf[64];

    if constexpr (std::is_signed_v<_T>)
    {
        /// Restore some data as negatives and others as positives
        if (min < 0 && max >= 0 && num_bits > 0 && num_bits < 64)
        {
            _T sign_bit = 1ull << (num_bits-1);
            _T upper_max = max >> num_bits << num_bits;

            for (UInt32 col = 0; col < tail; ++col)
            {
                _T & value = buf[col];
                value = 0;

                for (UInt32 row = 0; row < meaning_bytes; ++row)
                    value |= _T(mx8[64 * row + col]) << (8 * row);

                if (value & sign_bit)
                    value |= upper_min;
                else
                    value |= upper_max;
            }

            memcpy(dst, buf, tail * sizeof(_T));
            return;
        }
    }

    _T * p_value = buf;
    for (UInt32 col = 0; col < tail; ++col, ++p_value)
    {
        *p_value = upper_min;

        for (UInt32 row = 0; row < meaning_bytes; ++row)
            *p_value |= _T(mx8[64 * row + col]) << (8 * row);
    }

    memcpy(dst, buf, tail * sizeof(_T));
}


UInt32 getValuableBitsNumber(UInt64 min, UInt64 max)
{
    UInt64 diff_bits = min ^ max;
    if (diff_bits)
        return 64 - __builtin_clzll(diff_bits);
    return 0;
}

UInt32 getValuableBitsNumber(Int64 min, Int64 max)
{
    if (min < 0 && max >= 0)
    {
        if (min + max >= 0)
            return getValuableBitsNumber(0ull, UInt64(max)) + 1;
        else
            return getValuableBitsNumber(0ull, UInt64(~min)) + 1;
    }
    else
        return getValuableBitsNumber(UInt64(min), UInt64(max));
}


template <typename _T>
void findMinMax(const char * src, UInt32 src_size, _T & min, _T & max)
{
    min = unalignedLoad<_T>(src);
    max = unalignedLoad<_T>(src);

    const char * end = src + src_size;
    for (; src < end; src += sizeof(_T))
    {
        auto current = unalignedLoad<_T>(src);
        if (current < min)
            min = current;
        if (current > max)
            max = current;
    }
}

template <typename _T>
UInt32 compressData(const char * src, UInt32 bytes_size, char * dst)
{
    using MinMaxType = std::conditional_t<std::is_signed_v<_T>, Int64, UInt64>;

    const UInt32 mx_size = 64;
    const UInt32 header_size = 2 * sizeof(UInt64);

    if (bytes_size % sizeof(_T))
        throw Exception("Cannot compress, data size " + toString(bytes_size) + " is not multiplier of " + toString(sizeof(_T)),
                        ErrorCodes::CANNOT_COMPRESS);

    UInt32 src_size = bytes_size / sizeof(_T);
    UInt32 num_full = src_size / mx_size;
    UInt32 tail = src_size % mx_size;

    _T min, max;
    findMinMax<_T>(src, bytes_size, min, max);
    MinMaxType min64 = min;
    MinMaxType max64 = max;

    /// Write header
    {
        memcpy(dst, &min64, sizeof(MinMaxType));
        memcpy(dst + 8, &max64, sizeof(MinMaxType));
        dst += header_size;
    }

    UInt32 num_bits = getValuableBitsNumber(min64, max64);
    if (!num_bits)
        return header_size;

    UInt32 src_shift = sizeof(_T) * mx_size;
    UInt32 dst_shift = sizeof(UInt64) * num_bits;
    for (UInt32 i = 0; i < num_full; ++i)
    {
        transpose<_T>(src, dst, num_bits);
        src += src_shift;
        dst += dst_shift;
    }

    UInt32 dst_bytes = num_full * dst_shift;

    if (tail)
    {
        transpose<_T>(src, dst, num_bits, tail);
        dst_bytes += dst_shift;
    }

    return header_size + dst_bytes;
}

template <typename _T>
void decompressData(const char * src, UInt32 bytes_size, char * dst, UInt32 uncompressed_size)
{
    using MinMaxType = std::conditional_t<std::is_signed_v<_T>, Int64, UInt64>;

    const UInt32 header_size = 2 * sizeof(UInt64);

    if (bytes_size < header_size)
        throw Exception("Cannot decompress, data size " + toString(bytes_size) + " is less then T64 header",
                        ErrorCodes::CANNOT_DECOMPRESS);

    if (uncompressed_size % sizeof(_T))
        throw Exception("Cannot decompress, unexpected uncompressed size " + toString(uncompressed_size),
                        ErrorCodes::CANNOT_DECOMPRESS);

    UInt64 num_elements = uncompressed_size / sizeof(_T);
    MinMaxType min;
    MinMaxType max;

    /// Read header
    {
        memcpy(&min, src, sizeof(MinMaxType));
        memcpy(&max, src + 8, sizeof(MinMaxType));
        src += header_size;
        bytes_size -= header_size;
    }

    UInt32 num_bits = getValuableBitsNumber(min, max);
    if (!num_bits)
    {
        _T min_value = min;
        for (UInt32 i = 0; i < num_elements; ++i, dst += sizeof(_T))
            unalignedStore(dst, min_value);
        return;
    }

    UInt32 src_shift = sizeof(UInt64) * num_bits;
    UInt32 dst_shift = sizeof(_T) * 64;

    if (!bytes_size || bytes_size % src_shift)
        throw Exception("Cannot decompress, data size " + toString(bytes_size) + " is not multiplier of " + toString(src_shift),
                        ErrorCodes::CANNOT_DECOMPRESS);

    UInt32 num_full = bytes_size / src_shift;
    UInt32 tail = num_elements % 64;
    if (tail)
        --num_full;

    for (UInt32 i = 0; i < num_full; ++i)
    {
        revTranspose<_T>(src, dst, num_bits, min, max);
        src += src_shift;
        dst += dst_shift;
    }

    if (tail)
        revTranspose<_T>(src, dst, num_bits, min, max, tail);
}

}

UInt32 CompressionCodecT64::doCompressData(const char * src, UInt32 src_size, char * dst) const
{
    memcpy(dst, &type_idx, 1);
    dst += 1;

    switch (baseType(type_idx))
    {
        case TypeIndex::Int8:
            return 1 + compressData<Int8>(src, src_size, dst);
        case TypeIndex::Int16:
            return 1 + compressData<Int16>(src, src_size, dst);
        case TypeIndex::Int32:
            return 1 + compressData<Int32>(src, src_size, dst);
        case TypeIndex::Int64:
            return 1 + compressData<Int64>(src, src_size, dst);
        case TypeIndex::UInt8:
            return 1 + compressData<UInt8>(src, src_size, dst);
        case TypeIndex::UInt16:
            return 1 + compressData<UInt16>(src, src_size, dst);
        case TypeIndex::UInt32:
            return 1 + compressData<UInt32>(src, src_size, dst);
        case TypeIndex::UInt64:
            return 1 + compressData<UInt64>(src, src_size, dst);
        default:
            break;
    }

    throw Exception("Connot compress with T64", ErrorCodes::CANNOT_COMPRESS);
}

void CompressionCodecT64::doDecompressData(const char * src, UInt32 src_size, char * dst, UInt32 uncompressed_size) const
{
    if (!src_size)
        throw Exception("Connot decompress with T64", ErrorCodes::CANNOT_DECOMPRESS);

    UInt8 saved_type_id = unalignedLoad<UInt8>(src);
    src += 1;
    src_size -= 1;

    TypeIndex actual_type_id = type_idx;
    if (actual_type_id == TypeIndex::Nothing)
        actual_type_id = static_cast<TypeIndex>(saved_type_id);

    switch (baseType(actual_type_id))
    {
        case TypeIndex::Int8:
            return decompressData<Int8>(src, src_size, dst, uncompressed_size);
        case TypeIndex::Int16:
            return decompressData<Int16>(src, src_size, dst, uncompressed_size);
        case TypeIndex::Int32:
            return decompressData<Int32>(src, src_size, dst, uncompressed_size);
        case TypeIndex::Int64:
            return decompressData<Int64>(src, src_size, dst, uncompressed_size);
        case TypeIndex::UInt8:
            return decompressData<UInt8>(src, src_size, dst, uncompressed_size);
        case TypeIndex::UInt16:
            return decompressData<UInt16>(src, src_size, dst, uncompressed_size);
        case TypeIndex::UInt32:
            return decompressData<UInt32>(src, src_size, dst, uncompressed_size);
        case TypeIndex::UInt64:
            return decompressData<UInt64>(src, src_size, dst, uncompressed_size);
        default:
            break;
    }

    throw Exception("Connot decompress with T64", ErrorCodes::CANNOT_DECOMPRESS);
}

void CompressionCodecT64::useInfoAboutType(DataTypePtr data_type)
{
    if (data_type)
    {
        type_idx = typeIdx(data_type);
        if (type_idx == TypeIndex::Nothing)
            throw Exception("T64 codec is not supported for specified type", ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);
    }
}

UInt8 CompressionCodecT64::getMethodByte() const
{
    return codecId();
}

void registerCodecT64(CompressionCodecFactory & factory)
{
    auto reg_func = [&](const ASTPtr & arguments, DataTypePtr type) -> CompressionCodecPtr
    {
        if (arguments && !arguments->children.empty())
            throw Exception("T64 codec should not have parameters", ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

        auto type_idx = typeIdx(type);
        if (type && type_idx == TypeIndex::Nothing)
            throw Exception("T64 codec is not supported for specified type", ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);
        return std::make_shared<CompressionCodecT64>(type_idx);
    };

    factory.registerCompressionCodecWithType("T64", codecId(), reg_func);
}
}
