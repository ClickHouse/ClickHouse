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
    chassert(part.getType() == MergeTreeDataPartType::Wide);

    const auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
        column, {}, ".bin", part.checksums, part.storage.getSettings());

    return part.getDataPartStorage().readFile(*stream_name + ".bin", read_settings, /*read_hint=*/std::nullopt);
}

std::optional<MergeTreeCompressedBlockReader::Block> MergeTreeCompressedBlockReader::next()
{
    size_t decompressed_bytes = 0;
    size_t compressed_with_header = 0;

    if (readCompressedData(decompressed_bytes, compressed_with_header, /*always_copy=*/false) == 0)
        return {};

    constexpr UInt8 header_size = ICompressionCodec::getHeaderSize();

    const UInt8 block_method = static_cast<UInt8>(compressed_buffer[0]);
    method_byte = block_method;

    return Block{
        .payload = compressed_buffer + header_size,
        .compressed_bytes = compressed_with_header - header_size,
        .decompressed_bytes = decompressed_bytes,
    };
}

}
