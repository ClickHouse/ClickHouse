#include <Storages/MergeTree/MergeTreeCompressedBlockReader.h>

#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool use_streaming_marks_compression;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeCompressedBlockReader::MergeTreeCompressedBlockReader(
    const IMergeTreeDataPart & part,
    const NameAndTypePair & column,
    const ReadSettings & read_settings,
    const ISerialization::SubstreamPath & substream_path)
    : CompressedReadBufferBase(nullptr)
    , file(openColumnFile(part, column, read_settings, substream_path))
{
    compressed_in = file.get();
}

std::unique_ptr<ReadBufferFromFileBase> MergeTreeCompressedBlockReader::openColumnFile(
    const IMergeTreeDataPart & part,
    const NameAndTypePair & column,
    const ReadSettings & read_settings,
    const ISerialization::SubstreamPath & substream_path)
{
    chassert(part.getType() == MergeTreeDataPartType::Wide);

    const auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
        column, substream_path, ".bin", part.checksums, part.storage.getSettings());
    if (!stream_name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} has no file for a stream of column {}", part.name, column.name);

    return part.getDataPartStorage().readFile(*stream_name + ".bin", read_settings, /*read_hint=*/std::nullopt);
}

MergeTreeMarksGetterPtr MergeTreeCompressedBlockReader::loadMarks(
    const IMergeTreeDataPart & part,
    const NameAndTypePair & column,
    const ReadSettings & read_settings,
    const ISerialization::SubstreamPath & substream_path)
{
    chassert(part.getType() == MergeTreeDataPartType::Wide);

    const auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
        column, substream_path, ".bin", part.checksums, part.storage.getSettings());
    if (!stream_name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} has no file for a stream of column {}", part.name, column.name);

    const auto context = part.storage.getContext();
    const auto & index_granularity_info = part.index_granularity_info;
    MergeTreeMarksLoader loader(
        std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part.shared_from_this(), std::make_shared<AlterConversions>()),
        context->getMarkCache().get(),
        index_granularity_info.getMarksFilePath(*stream_name),
        part.getMarksCount(),
        index_granularity_info,
        /*save_marks_in_cache=*/ true,
        read_settings,
        /*load_marks_threadpool=*/ nullptr,
        /*num_columns_in_mark=*/ 1,
        context->getSettingsRef()[Setting::use_streaming_marks_compression]);
    return loader.loadMarks();
}

void MergeTreeCompressedBlockReader::seekToBlock(size_t offset_in_compressed_file)
{
    file->seek(offset_in_compressed_file, SEEK_SET);
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
