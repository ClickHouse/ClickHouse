#include <Storages/MergeTree/MergeTreeCompressedBlockReader.h>

#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeCompressedBlockReader::MergeTreeCompressedBlockReader(
    const IMergeTreeDataPart & part, const NameAndTypePair & column, const ReadSettings & read_settings)
    : CompressedReadBufferBase(nullptr)
    , file(openColumnFile(part, column, read_settings))
{
    compressed_in = file.get();
}

std::unique_ptr<ReadBufferFromFileBase> MergeTreeCompressedBlockReader::openColumnFile(
    const IMergeTreeDataPart & part, const NameAndTypePair & column, const ReadSettings & read_settings)
{
    if (part.getType() != MergeTreeDataPartType::Wide)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Part {} is not wide, so column {} does not have a file of its own",
            part.name,
            column.name);

    const auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
        column, {}, ".bin", part.checksums, part.storage.getSettings());

    if (!stream_name)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Column {} of part {} has no single stream", column.name, part.name);

    return part.getDataPartStorage().readFile(*stream_name + ".bin", read_settings, /*read_hint=*/std::nullopt);
}

std::optional<MergeTreeCompressedBlockReader::Block> MergeTreeCompressedBlockReader::next()
{
    size_t decompressed_bytes = 0;
    size_t compressed_with_header = 0;

    if (readCompressedData(decompressed_bytes, compressed_with_header, /*always_copy=*/false) == 0)
        return {};

    constexpr UInt8 header_size = ICompressionCodec::getHeaderSize();
    if (compressed_with_header < header_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "A compressed block of {} bytes is shorter than its header", compressed_with_header);

    /// `compressed_buffer` starts at the header, whose first byte is the method - the same place
    /// `decompressTo` reads it from.
    const UInt8 block_method = static_cast<UInt8>(compressed_buffer[0]);
    if (method_byte && *method_byte != block_method)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A column holds blocks of method {:#x} and {:#x}, which this reader does not mix",
            static_cast<UInt16>(*method_byte),
            static_cast<UInt16>(block_method));
    method_byte = block_method;

    return Block{
        .payload = compressed_buffer + header_size,
        .compressed_bytes = compressed_with_header - header_size,
        .decompressed_bytes = decompressed_bytes,
    };
}

}
