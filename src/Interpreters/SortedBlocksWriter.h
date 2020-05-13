#pragma once

#include <mutex>
#include <condition_variable>

#include <Common/filesystemHelpers.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataStreams/SizeLimits.h>
#include <DataStreams/IBlockStream_fwd.h>

namespace DB
{

class TableJoin;
class MergeJoinCursor;
struct MergeJoinEqualRange;

class VolumeJBOD;
using VolumeJBODPtr = std::shared_ptr<VolumeJBOD>;

struct SortedBlocksWriter
{
    using TmpFilePtr = std::unique_ptr<TemporaryFile>;
    using SortedFiles = std::vector<TmpFilePtr>;

    struct Blocks
    {
        BlocksList blocks;
        size_t row_count = 0;
        size_t bytes = 0;

        bool empty() const { return blocks.empty(); }

        void insert(Block && block)
        {
            countBlockSize(block);
            blocks.emplace_back(std::move(block));
        }

        void countBlockSize(const Block & block)
        {
            row_count += block.rows();
            bytes += block.bytes();
        }

        void clear()
        {
            blocks.clear();
            row_count = 0;
            bytes = 0;
        }
    };

    static constexpr const size_t num_streams = 2;

    std::mutex insert_mutex;
    std::condition_variable flush_condvar;
    const SizeLimits & size_limits;
    VolumeJBODPtr volume;
    const Block & sample_block;
    const SortDescription & sort_description;
    Blocks & inserted_blocks;
    const size_t rows_in_block;
    const size_t num_files_for_merge;
    const String & codec;
    SortedFiles sorted_files;
    size_t row_count_in_flush = 0;
    size_t bytes_in_flush = 0;
    size_t flush_number = 0;
    size_t flush_inflight = 0;

    SortedBlocksWriter(const SizeLimits & size_limits_, VolumeJBODPtr volume_, const Block & sample_block_, const SortDescription & description,
                       Blocks & blocks, size_t rows_in_block_, size_t num_files_to_merge_, const String & codec_)
        : size_limits(size_limits_)
        , volume(volume_)
        , sample_block(sample_block_)
        , sort_description(description)
        , inserted_blocks(blocks)
        , rows_in_block(rows_in_block_)
        , num_files_for_merge(num_files_to_merge_)
        , codec(codec_)
    {
        sorted_files.emplace_back(flush(inserted_blocks.blocks));
        inserted_blocks.clear();
    }

    String getPath() const;
    BlockInputStreamPtr streamFromFile(const TmpFilePtr & file) const;

    void insert(Block && block);
    TmpFilePtr flush(const BlocksList & blocks) const;
    SortedFiles finishMerge(std::function<void(const Block &)> callback = [](const Block &){});
};

}
