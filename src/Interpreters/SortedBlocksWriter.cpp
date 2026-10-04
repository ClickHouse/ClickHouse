#include <Core/SortCursor.h>
#include <Interpreters/SortedBlocksWriter.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Sources/ExternalMergeSource.h>
#include <Processors/Merges/MergingSortedTransform.h>


namespace ProfileEvents
{
    extern const Event ExternalJoinMerge;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

TemporaryBlockStreamHolder flushBlockToFile(const TemporaryDataOnDiskScopePtr & tmp_data, const Block & block)
{
    TemporaryBlockStreamHolder stream_holder(std::make_shared<const Block>(block.cloneEmpty()), tmp_data);
    stream_holder->write(block);
    stream_holder.finishWriting();
    return stream_holder;
}


TemporaryBlockStreamHolder flushToFile(const TemporaryDataOnDiskScopePtr & tmp_data, const Block & header, QueryPipelineBuilder pipeline)
{
    TemporaryBlockStreamHolder stream_holder(std::make_shared<const Block>(header), tmp_data);

    auto exec_pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline));
    PullingPipelineExecutor executor(exec_pipeline);

    Block block;
    while (executor.pull(block))
        stream_holder->write(block);

    stream_holder.finishWriting();
    return stream_holder;
}

SortedBlocksWriter::SortedFiles flushToManyFiles(const TemporaryDataOnDiskScopePtr & tmp_data, QueryPipelineBuilder builder,
                                                 std::function<void(const Block &)> callback)
{
    SortedBlocksWriter::SortedFiles files;
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    PullingPipelineExecutor executor(pipeline);

    Block block;
    while (executor.pull(block))
    {
        if (!block.rows())
            continue;
        callback(block);
        files.push_back(flushBlockToFile(tmp_data, block));
    }

    return files;
}

}


void SortedBlocksWriter::insert(Block && block)
{
    bool can_insert_more = false;
    bool has_data_to_flush = false;

    BlocksList current_blocks;
    size_t row_count = 0;
    size_t bytes = 0;
    size_t flush_no = 0;

    if (!block.rows())
        return;

    {
        std::lock_guard lock{insert_mutex};

        /// Add the block and update the buffered size while holding the insertion lock.
        inserted_blocks.insert(std::move(block));

        size_t total_row_count = inserted_blocks.row_count + row_count_in_flush;
        size_t total_bytes = inserted_blocks.bytes + bytes_in_flush;

        can_insert_more = size_limits.softCheck(total_row_count, total_bytes);
        has_data_to_flush = !size_limits.softCheck(inserted_blocks.row_count * num_streams, inserted_blocks.bytes * num_streams);

        if (has_data_to_flush)
        {
            ++flush_inflight;
            current_blocks.swap(inserted_blocks.blocks);
            row_count_in_flush = total_row_count;
            bytes_in_flush = total_bytes;

            row_count = inserted_blocks.row_count;
            bytes = inserted_blocks.bytes;
            inserted_blocks.clear();
        }
        else if (can_insert_more)
            flush_no = flush_number;
    }

    if (has_data_to_flush)
    {

        /// Write the selected blocks without holding the insertion lock so other streams can insert.
        auto flushed = flush(current_blocks);
        current_blocks.clear();

        std::lock_guard lock{insert_mutex};

        sorted_files.emplace_back(std::move(flushed));
        row_count_in_flush -= row_count;
        bytes_in_flush -= bytes;

        /// Advance `flush_number` for waiting inserts and reduce `flush_inflight` for the waiting merge.
        ++flush_number;
        --flush_inflight;
        flush_condvar.notify_all();
    }
    else if (!can_insert_more)
    {

        /// Wait for a flush to release memory when buffered and in-flight blocks exceed the size limit.
        std::unique_lock lock{insert_mutex};
        flush_condvar.wait(lock, [&]{ return flush_no < flush_number; });
    }
}

TemporaryBlockStreamHolder SortedBlocksWriter::flush(const BlocksList & blocks) const
{
    Pipes pipes;
    pipes.reserve(blocks.size());
    for (const auto & block : blocks)
        if (auto num_rows = block.rows())
            pipes.emplace_back(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(block.cloneEmpty()), Chunk(block.getColumns(), num_rows)));

    if (pipes.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty block");

    QueryPipelineBuilder pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes)));

    if (pipeline.getNumStreams() > 1)
    {
        auto transform = std::make_shared<MergingSortedTransform>(
            pipeline.getSharedHeader(),
            pipeline.getNumStreams(),
            sort_description,
            rows_in_block,
            /*max_block_size_bytes=*/0,
            /*max_dynamic_subcolumns=*/std::nullopt,
            SortingQueueStrategy::Default);

        pipeline.addTransform(std::move(transform));
    }

    return flushToFile(tmp_data, sample_block, std::move(pipeline));
}

SortedBlocksWriter::SortedFiles SortedBlocksWriter::finishMerge(std::function<void(const Block &)> callback)
{
    SortedFiles files;
    BlocksList blocks;

    /// Wait for in-flight flushes to finish before constructing the file merge.
    {
        std::unique_lock lock{insert_mutex};

        files.swap(sorted_files);
        blocks.swap(inserted_blocks.blocks);
        inserted_blocks.clear();

        flush_condvar.wait(lock, [&]{ return !flush_inflight; });
    }

    /// Write any buffered blocks so every merge input is a completed temporary file.
    if (!blocks.empty())
        files.emplace_back(flush(blocks));

    auto header = std::make_shared<const Block>(sample_block);
    auto merge = [header, description = sort_description, block_size = rows_in_block]
        (const SharedHeaders & headers) -> ProcessorPtr
    {
        return std::make_shared<MergingSortedTransform>(
            header, headers.size(), description, block_size, /*max_block_size_bytes=*/ 0,
            /*max_dynamic_subcolumns=*/ std::nullopt, SortingQueueStrategy::Default);
    };
    auto final_merge = [merge](const SharedHeaders & headers)
    {
        if (headers.size() > 1)
            ProfileEvents::increment(ProfileEvents::ExternalJoinMerge);
        return merge(headers);
    };
    ExternalMergeSource::Runs runs;
    for (auto & file : files)
        runs.emplace_back(std::move(file));
    std::vector<ExternalMergeSource::Group> groups;
    groups.emplace_back(std::move(runs), merge);
    auto source = std::make_shared<ExternalMergeSource>(
        header, std::move(groups), /*tail_=*/ nullptr, std::move(final_merge), num_files_for_merge, tmp_data,
        /*min_free_disk_space=*/ 0, getLogger("SortedBlocksWriter"));
    QueryPipelineBuilder pipeline;
    pipeline.init(Pipe(std::move(source)));
    return flushToManyFiles(tmp_data, std::move(pipeline), std::move(callback));
}

Block SortedBlocksBuffer::exchange(Block && block)
{
    static constexpr const double reserve_coefficient = 1.2;

    Blocks out_blocks;
    Block empty_out = block.cloneEmpty();

    {
        std::lock_guard lock(mutex);

        if (!block.empty())
        {
            current_bytes += block.bytes();
            buffer.emplace_back(std::move(block));

            /// Saved. Return empty block with same structure.
            if (current_bytes < max_bytes)
                return empty_out;
        }

        /// Not saved. Return buffered.
        out_blocks.swap(buffer);
        buffer.reserve(static_cast<size_t>(static_cast<double>(out_blocks.size()) * reserve_coefficient));
        current_bytes = 0;
    }

    if (size_t size = out_blocks.size())
    {
        if (size == 1)
            return out_blocks[0];
        return mergeBlocks(std::move(out_blocks));
    }

    return {};
}

Block SortedBlocksBuffer::mergeBlocks(Blocks && blocks) const
{
    size_t num_rows = 0;

    { /// Merge sort blocks
        Pipes pipes;
        pipes.reserve(blocks.size());

        for (auto & block : blocks)
        {
            num_rows += block.rows();
            Chunk chunk(block.getColumns(), block.rows());
            pipes.emplace_back(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(block.cloneEmpty()), std::move(chunk)));
        }

        Blocks tmp_blocks;

        QueryPipelineBuilder builder;
        builder.init(Pipe::unitePipes(std::move(pipes)));

        if (builder.getNumStreams() > 1)
        {
            auto transform = std::make_shared<MergingSortedTransform>(
                builder.getSharedHeader(),
                builder.getNumStreams(),
                sort_description,
                num_rows,
                /*max_block_size_bytes=*/0,
                /*max_dynamic_subcolumns=*/std::nullopt,
                SortingQueueStrategy::Default);

            builder.addTransform(std::move(transform));
        }

        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
        PullingPipelineExecutor executor(pipeline);
        Block block;
        while (executor.pull(block))
            tmp_blocks.emplace_back(block);

        blocks.swap(tmp_blocks);
    }

    return concatenateBlocks(blocks);
}

}
