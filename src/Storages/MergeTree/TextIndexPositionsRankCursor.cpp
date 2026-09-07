#include <Storages/MergeTree/TextIndexPositionsRankCursor.h>

#include <Storages/MergeTree/MergeTreeReaderStream.h>

#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

TextIndexPositionsRankCursor::TextIndexPositionsRankCursor(
    MergeTreeReaderStream & stream_, const TokenPostingsInfo & info_, UInt64 expected_num_docs)
    : stream(&stream_), info(&info_)
{
    const size_t file_size = stream->getFileSize();
    if ((info->position_bytes == 0) || (info->position_offset > file_size)
        || (info->position_bytes > file_size - info->position_offset))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: blob of {} bytes at offset {} is outside the {}-byte stream",
            info->position_bytes, info->position_offset, file_size);

    stream->seekToMark({info->position_offset, 0});
    directory = TextIndexBlockedPositionsCodec::readDirectory(
        *stream->getDataBuffer(), info->position_offset, expected_num_docs, info->position_bytes);
}

std::span<const UInt32> TextIndexPositionsRankCursor::seek(UInt64 rank)
{
    const size_t block_idx = static_cast<size_t>(rank / TextIndexBlockedPositionsCodec::BLOCK_DOCS);
    const size_t local_rank = static_cast<size_t>(rank % TextIndexBlockedPositionsCodec::BLOCK_DOCS);

    if (block_idx != current_block)
    {
        if (block_idx >= directory.numBlocks())
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupt text index positions: rank {} is outside the token's {} blocks", rank, directory.numBlocks());

        const size_t docs = directory.docsInBlock(block_idx);
        all_local_ranks.resize(docs);
        std::iota(all_local_ranks.begin(), all_local_ranks.end(), UInt32{0});

        block_offsets.clear();
        block_positions.clear();
        block_offsets.push_back(0);

        stream->seekToMark({directory.block_offsets[block_idx], 0});
        TextIndexBlockedPositionsCodec::decodeBlock(
            *stream->getDataBuffer(), directory, block_idx,
            std::span<const UInt32>(all_local_ranks.data(), all_local_ranks.size()),
            block_offsets, block_positions, scratch);

        current_block = block_idx;
    }

    if (local_rank + 1 >= block_offsets.size())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: rank {} is outside its block", rank);

    const UInt32 begin = block_offsets[local_rank];
    const UInt32 end = block_offsets[local_rank + 1];
    return std::span<const UInt32>(block_positions.data() + begin, end - begin);
}

}
