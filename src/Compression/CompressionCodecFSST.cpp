#include <Compression/CompressionCodecFSST.h>

#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/registerCompressionCodecs.h>
#include <Parsers/IAST.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <IO/VarInt.h>
#include <base/unaligned.h>

#include <cstring>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
}

uint8_t CompressionCodecFSST::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::FSST);
}

ASTPtr CompressionCodecFSST::getCodecDescription() const
{
    return makeCodecDescription("FSST");
}

void CompressionCodecFSST::updateHash(SipHash & hash) const
{
    getCodecDescription()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecFSST::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    /// The data is encoded into `dest` before it is known whether it is smaller than the original.
    /// The index has at most one boundary per byte, and every difference of the positions of the codes
    /// takes one byte, plus one byte per 128 codes.
    const UInt64 codes_size = FSST::maxEncodedSize(uncompressed_size);
    const UInt64 max_index_size = 10 + uncompressed_size + codes_size / 128 + 1;
    return static_cast<UInt32>(1 + FSST::MAX_SERIALIZED_SYMBOL_TABLE_SIZE + sizeof(UInt32) + codes_size + max_index_size);
}

UInt32 CompressionCodecFSST::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    return compressImpl(source, source_size, dest, nullptr);
}

UInt32 CompressionCodecFSST::doCompressDataWithValueBoundaries(
    const char * source, UInt32 source_size, char * dest, std::span<const UInt32> value_boundaries) const
{
    return compressImpl(source, source_size, dest, &value_boundaries);
}

UInt32 CompressionCodecFSST::compressImpl(
    const char * source, UInt32 source_size, char * dest, const std::span<const UInt32> * value_boundaries) const
{
    if (source_size)
    {
        FSST::SymbolTable table = FSST::SymbolTable::build(source, source_size, value_boundaries);
        const size_t table_size = table.serializedSize();
        table.serialize(dest + 1);

        char * pos = dest + 1 + table_size;
        if (value_boundaries)
        {
            char * codes = pos + sizeof(UInt32);
            const size_t codes_size = FSST::encode(table, source, source_size, reinterpret_cast<UInt8 *>(codes), value_boundaries);
            unalignedStoreLittleEndian<UInt32>(pos, static_cast<UInt32>(codes_size));

            VectorWithMemoryTracking<UInt32> positions;
            positions.reserve(value_boundaries->size());
            FSST::findBoundaryCodePositions(table, reinterpret_cast<const UInt8 *>(codes), codes_size, *value_boundaries, positions);

            pos = writeVarUInt(positions.size(), codes + codes_size);
            UInt32 prev = 0;
            for (UInt32 position : positions)
            {
                pos = writeVarUInt(position - prev, pos);
                prev = position;
            }
        }
        else
        {
            pos += FSST::encode(table, source, source_size, reinterpret_cast<UInt8 *>(pos));
        }

        const size_t compressed_size = pos - dest;
        if (compressed_size < 1 + static_cast<size_t>(source_size))
        {
            dest[0] = static_cast<char>(value_boundaries ? Format::ENCODED_ALIGNED : Format::ENCODED);
            return static_cast<UInt32>(compressed_size);
        }
    }

    dest[0] = static_cast<char>(Format::STORED);
    memcpy(dest + 1, source, source_size);
    return 1 + source_size;
}

bool CompressionCodecFSST::parseBlock(
    const char * source, size_t source_size, size_t decompressed_size, EncodedBlock & block, int error_code, bool read_index)
{
    if (source_size < 1)
        throw Exception(error_code, "Cannot decompress FSST-encoded data: the data is too short");

    const auto format = static_cast<Format>(source[0]);
    if (format == Format::STORED)
    {
        if (source_size - 1 != decompressed_size)
            throw Exception(error_code,
                "Cannot decompress FSST-encoded data: the size of the stored data ({}) does not match the decompressed size ({})",
                source_size - 1, decompressed_size);
        return false;
    }

    if (format != Format::ENCODED && format != Format::ENCODED_ALIGNED)
        throw Exception(error_code, "Cannot decompress FSST-encoded data: unknown format {}", static_cast<UInt32>(format));
    block.aligned = format == Format::ENCODED_ALIGNED;

    const size_t table_size = block.table.deserialize(source + 1, source_size - 1, error_code);
    const char * pos = source + 1 + table_size;
    const char * const end = source + source_size;
    block.error_code = error_code;
    block.boundary_code_positions.clear();

    if (!block.aligned)
    {
        block.codes = reinterpret_cast<const UInt8 *>(pos);
        block.codes_size = end - pos;
        return true;
    }

    if (end - pos < static_cast<ptrdiff_t>(sizeof(UInt32)))
        throw Exception(error_code, "Cannot decompress FSST-encoded data: the data is too short");
    const size_t codes_size = unalignedLoadLittleEndian<UInt32>(pos);
    pos += sizeof(UInt32);
    if (static_cast<size_t>(end - pos) < codes_size)
        throw Exception(error_code, "Cannot decompress FSST-encoded data: the size of the codes ({}) is larger than the data", codes_size);
    block.codes = reinterpret_cast<const UInt8 *>(pos);
    block.codes_size = codes_size;

    if (read_index)
    {
        pos += codes_size;
        UInt64 num_boundaries = 0;
        pos = readVarUInt(num_boundaries, pos, end - pos);
        /// Every boundary takes at least one byte.
        if (num_boundaries > static_cast<size_t>(end - pos))
            throw Exception(error_code, "Cannot decompress FSST-encoded data: the index of the values is too large ({})", num_boundaries);

        block.boundary_code_positions.reserve(num_boundaries);
        UInt64 position = 0;
        for (size_t i = 0; i < num_boundaries; ++i)
        {
            UInt64 difference = 0;
            pos = readVarUInt(difference, pos, end - pos);
            position += difference;
            /// The positions of the boundaries increase, because every value between them has at least one code.
            if (difference == 0 || position > codes_size)
                throw Exception(error_code, "Cannot decompress FSST-encoded data: malformed index of the values");
            block.boundary_code_positions.push_back(static_cast<UInt32>(position));
        }
    }

    return true;
}

UInt32 CompressionCodecFSST::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    EncodedBlock block;
    if (!parseBlock(source, source_size, uncompressed_size, block, decompression_error_code, /*read_index=*/ false))
    {
        memcpy(dest, source + 1, uncompressed_size);
        return uncompressed_size;
    }

    FSST::decodeBlock(block.table, block.codes, block.codes_size, dest, uncompressed_size, block.error_code);
    return uncompressed_size;
}

void registerCodecFSST(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::FSST);
    factory.registerCompressionCodec("FSST", method_code, [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        if (arguments && !arguments->children.empty())
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "FSST codec must have no parameters, given {}", arguments->children.size());
        return std::make_shared<CompressionCodecFSST>();
    });
}

}
