#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <base/unaligned.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <IO/WriteHelpers.h>
#include "Common/Exception.h"
#include "base/Decimal_fwd.h"
#include "base/types.h"
#include "config.h"

#include <cstdint>
#include <numeric>

namespace DB
{

template <typename T>
T gcd_func(T a, T b)
{
    while (b != 0)
    {
        T c = a % b;
        a = b;
        b = c;
    }
    if (a == T(-1))
    {
        return -a;
    }
    return a;
}

class CompressionCodecGCD : public ICompressionCodec
{
public:
    explicit CompressionCodecGCD(UInt8 gcd_bytes_size_);

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return false; }
    bool isGenericCompression() const override { return false; }

private:
    const UInt8 gcd_bytes_size;
};


namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
}

UInt32 CompressionCodecGCD::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return uncompressed_size + 2;
}

CompressionCodecGCD::CompressionCodecGCD(UInt8 gcd_bytes_size_)
    : gcd_bytes_size(gcd_bytes_size_)
{
    setCodecDescription("GCD", {std::make_shared<ASTLiteral>(static_cast<UInt64>(gcd_bytes_size))});
}

uint8_t CompressionCodecGCD::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::GCD);
}

void CompressionCodecGCD::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash);
}

namespace
{

template <typename T>
void compressDataForType(const char * source, UInt32 source_size, char * dest)
{
    if (source_size % sizeof(T) != 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot GCD compress, data size {}  is not aligned to {}", source_size, sizeof(T));

    const char * const source_end = source + source_size;

    T gcd{};
    const auto * cur_source = source;
    while (cur_source < source_end)
    {
        if (cur_source == source)
        {
            gcd = unalignedLoad<T>(cur_source);
        }
        else
        {
            gcd = gcd_func<T>(gcd, unalignedLoad<T>(cur_source));
        }
    }

    unalignedStore<T>(dest, gcd);
    dest += sizeof(T);

    cur_source = source;
    while (cur_source < source_end)
    {
        unalignedStore<T>(dest, unalignedLoad<T>(cur_source) / gcd);
        cur_source += sizeof(T);
        dest += sizeof(T);
    }
}

template <typename T>
void decompressDataForType(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    const char * const output_end = dest + output_size;

    if (source_size % sizeof(T) != 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot GCD decompress, data size {}  is not aligned to {}", source_size, sizeof(T));

    if (source_size < sizeof(T))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot GCD decompress, data size {} is less than {}", source_size, sizeof(T));

    const char * const source_end = source + source_size;
    const T gcd = unalignedLoad<T>(source);
    source += sizeof(T);
    while (source < source_end)
    {
        if (dest + sizeof(T) > output_end) [[unlikely]]
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress the data");
        unalignedStore<T>(dest, unalignedLoad<T>(source) * gcd);

        source += sizeof(T);
        dest += sizeof(T);
    }
}

}

UInt32 CompressionCodecGCD::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt8 bytes_to_skip = source_size % gcd_bytes_size;
    dest[0] = gcd_bytes_size;
    dest[1] = bytes_to_skip; /// unused (backward compatibility)
    memcpy(&dest[2], source, bytes_to_skip);
    size_t start_pos = 2 + bytes_to_skip;
    switch (gcd_bytes_size)
    {
    case 1:
        compressDataForType<UInt8>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 2:
        compressDataForType<UInt16>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 4:
        compressDataForType<UInt32>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 8:
        compressDataForType<UInt64>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 16:
        compressDataForType<UInt128>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 32:
        compressDataForType<UInt256>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    }
    return 1 + 1 + source_size;
}

void CompressionCodecGCD::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < 2)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress. File has wrong header");

    if (uncompressed_size == 0)
        return;

    UInt8 bytes_size = source[0];

    if (!(bytes_size == 1 || bytes_size == 2 || bytes_size == 4 || bytes_size == 8 || bytes_size == 16 || bytes_size == 32))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress. File has wrong header");

    UInt8 bytes_to_skip = uncompressed_size % bytes_size;
    UInt32 output_size = uncompressed_size - bytes_to_skip;

    if (static_cast<UInt32>(2 + bytes_to_skip) > source_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress. File has wrong header");

    memcpy(dest, &source[2], bytes_to_skip);
    UInt32 source_size_no_header = source_size - bytes_to_skip - 2;
    switch (bytes_size)
    {
    case 1:
        decompressDataForType<UInt8>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
        break;
    case 2:
        decompressDataForType<UInt16>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
        break;
    case 4:
        decompressDataForType<UInt32>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
        break;
    case 8:
        decompressDataForType<UInt64>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
        break;
    case 16:
        decompressDataForType<UInt128>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
        break;
    case 32:
        decompressDataForType<UInt256>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
        break;
    }
}

namespace
{

UInt8 getGCDBytesSize(const IDataType * column_type)
{
    if (!column_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec Delta is not applicable for {} because the data type is not of fixed size",
            column_type->getName());

    size_t max_size = column_type->getSizeOfValueInMemory();
    if (max_size == 1 || max_size == 2 || max_size == 4 || max_size == 8 || max_size == 16 || max_size == 32)
        return static_cast<UInt8>(max_size);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec Delta is only applicable for data types of size 1, 2, 4, 8, 16, 32 bytes. Given type {}",
            column_type->getName());
}

}

void registerCodecGCD(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::GCD);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        /// Default bytes size is 1.
        UInt8 gcd_bytes_size = 1;
        if (column_type)
        {
            gcd_bytes_size = getGCDBytesSize(column_type);
        }

        if (arguments && arguments->children.size() > 1)
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "GCD codec must have 1 parameter, given {}", arguments->children.size());
        return std::make_shared<CompressionCodecGCD>(gcd_bytes_size);
    };
    factory.registerCompressionCodecWithType("GCD", method_code, codec_builder);
}

CompressionCodecPtr getCompressionCodecGCD(UInt8 gcd_bytes_size)
{
    return std::make_shared<CompressionCodecGCD>(gcd_bytes_size);
}

}
