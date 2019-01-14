#include <Compression/CompressionCodecDelta.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>
#include <cstdlib>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
extern const int ILLEGAL_CODEC_PARAMETER;
}

CompressionCodecDelta::CompressionCodecDelta(UInt8 delta_bytes_size_)
    : delta_bytes_size(delta_bytes_size_)
{
}

UInt8 CompressionCodecDelta::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::Delta);
}

String CompressionCodecDelta::getCodecDesc() const
{
    return "Delta(" + toString(delta_bytes_size) + ")";
}

namespace
{

template <typename T>
void compressDataForType(const char * source, UInt32 source_size, char * dest)
{
    const auto * source_with_type = reinterpret_cast<const T *>(source);
    auto * dest_with_type = reinterpret_cast<T *>(dest);

    if (source_size > 0)
        dest_with_type[0] = source_with_type[0];

    for (size_t dest_index = 1, dest_end = source_size / sizeof(T); dest_index < dest_end; ++dest_index)
        dest_with_type[dest_index] = source_with_type[dest_index] - source_with_type[dest_index - 1];
}

template <typename T>
void decompressDataForType(const char * source, UInt32 source_size, char * dest)
{
    const auto * source_with_type = reinterpret_cast<const T *>(source);
    auto * dest_with_type = reinterpret_cast<T *>(dest);

    if (source_size > 0)
        dest_with_type[0] = source_with_type[0];

    for (size_t dest_index = 1, dest_end = source_size / sizeof(T); dest_index < dest_end; ++dest_index)
        dest_with_type[dest_index] = source_with_type[dest_index] + dest_with_type[dest_index - 1];
}

}

UInt32 CompressionCodecDelta::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt8 bytes_to_skip = source_size % delta_bytes_size;
    dest[0] = delta_bytes_size;
    dest[1] = bytes_to_skip;
    memcpy(&dest[2], source, bytes_to_skip);
    size_t start_pos = 2 + bytes_to_skip;
    switch (delta_bytes_size)
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
    }
    return 1 + 1 + source_size;
}

void CompressionCodecDelta::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 /* uncompressed_size */) const
{
    UInt8 bytes_size = source[0];
    UInt8 bytes_to_skip = source[1];

    memcpy(dest, &source[2], bytes_to_skip);
    switch (bytes_size)
    {
    case 1:
        decompressDataForType<UInt8>(&source[2 + bytes_to_skip], source_size - bytes_to_skip, &dest[bytes_to_skip]);
        break;
    case 2:
        decompressDataForType<UInt16>(&source[2 + bytes_to_skip], source_size - bytes_to_skip, &dest[bytes_to_skip]);
        break;
    case 4:
        decompressDataForType<UInt32>(&source[2 + bytes_to_skip], source_size - bytes_to_skip, &dest[bytes_to_skip]);
        break;
    case 8:
        decompressDataForType<UInt64>(&source[2 + bytes_to_skip], source_size - bytes_to_skip, &dest[bytes_to_skip]);
        break;
    }
}

void registerCodecDelta(CompressionCodecFactory & factory)
{
    UInt8 method_code = UInt8(CompressionMethodByte::Delta);
    factory.registerCompressionCodec("Delta", method_code, [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        UInt8 delta_bytes_size = CompressionCodecDelta::DEFAULT_BYTES_SIZE;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 1)
                throw Exception("Delta codec must have 1 parameter, given " + std::to_string(arguments->children.size()), ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

            const auto children = arguments->children;
            const ASTLiteral * literal = static_cast<const ASTLiteral *>(children[0].get());
            size_t user_bytes_size = literal->value.safeGet<UInt64>();
            if (user_bytes_size != 1 && user_bytes_size != 2 && user_bytes_size != 4 && user_bytes_size != 8)
                throw Exception("Delta value for delta codec can be 1, 2, 4 or 8, given " + toString(user_bytes_size), ErrorCodes::ILLEGAL_CODEC_PARAMETER);
            delta_bytes_size = static_cast<UInt8>(user_bytes_size);
        }

        return std::make_shared<CompressionCodecDelta>(delta_bytes_size);
    });
}
}
