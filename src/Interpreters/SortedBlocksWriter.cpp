#include <Core/SortCursor.h>
#include <Interpreters/SortedBlocksWriter.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/TemporaryFileStream.h>
#include <Disks/StoragePolicy.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_ENOUGH_SPACE;
}

namespace
{

std::unique_ptr<TemporaryFile> flushToFile(const String & tmp_path, const Block & header, IBlockInputStream & stream, const String & codec)
{
    auto tmp_file = createTemporaryFile(tmp_path);

    std::atomic<bool> is_cancelled{false};
    TemporaryFileStream::write(tmp_file->path(), header, stream, &is_cancelled, codec);
    if (is_cancelled)
        throw Exception("Cannot flush MergeJoin data on disk. No space at " + tmp_path, ErrorCodes::NOT_ENOUGH_SPACE);

    return tmp_file;
}

SortedBlocksWriter::SortedFiles flushToManyFiles(const String & tmp_path, const Block & header, IBlockInputStream & stream,
                                                 const String & codec, std::function<void(const Block &)> callback = [](const Block &){})
{
    std::vector<std::unique_ptr<TemporaryFile>> files;

    while (Block block = stream.read())
    {
        if (!block.rows())
            continue;

        callback(block);

        OneBlockInputStream block_stream(block);
        auto tmp_file = flushToFile(tmp_path, header, block_stream, codec);
        files.emplace_back(std::move(tmp_file));
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

    {
        std::lock_guard lock{insert_mutex};

        /// insert bock into BlocksList undef lock
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
        /// flush new blocks without lock
        auto flushed = flush(current_blocks);
        current_blocks.clear();

        std::lock_guard lock{insert_mutex};

        sorted_files.emplace_back(std::move(flushed));
        row_count_in_flush -= row_count;
        bytes_in_flush -= bytes;

        /// notify another insert (flush_number) and merge (flush_inflight)
        ++flush_number;
        --flush_inflight;
        flush_condvar.notify_all();
    }
    else if (!can_insert_more)
    {
        /// wakeup insert blocked by out of limit
        std::unique_lock lock{insert_mutex};
        flush_condvar.wait(lock, [&]{ return flush_no < flush_number; });
    }
}

SortedBlocksWriter::TmpFilePtr SortedBlocksWriter::flush(const BlocksList & blocks) const
{
    const std::string path = getPath();

    if (blocks.empty())
        return {};

    if (blocks.size() == 1)
    {
        OneBlockInputStream sorted_input(blocks.front());
        return flushToFile(path, sample_block, sorted_input, codec);
    }

    BlockInputStreams inputs;
    inputs.reserve(blocks.size());
    for (const auto & block : blocks)
        if (block.rows())
            inputs.push_back(std::make_shared<OneBlockInputStream>(block));

    MergingSortedBlockInputStream sorted_input(inputs, sort_description, rows_in_block);
    return flushToFile(path, sample_block, sorted_input, codec);
}

SortedBlocksWriter::SortedFiles SortedBlocksWriter::finishMerge(std::function<void(const Block &)> callback)
{
    /// wait other flushes if any
    {
        std::unique_lock lock{insert_mutex};
        flush_condvar.wait(lock, [&]{ return !flush_inflight; });
    }

    /// flush not flushed
    if (!inserted_blocks.empty())
        sorted_files.emplace_back(flush(inserted_blocks.blocks));
    inserted_blocks.clear();

    BlockInputStreams inputs;
    inputs.reserve(num_files_for_merge);

    /// Merge by parts to save memory. It's possible to exchange disk I/O and memory by num_files_for_merge.
    {
        SortedFiles new_files;
        new_files.reserve(sorted_files.size() / num_files_for_merge + 1);

        while (sorted_files.size() > num_files_for_merge)
        {
            for (const auto & file : sorted_files)
            {
                inputs.emplace_back(streamFromFile(file));

                if (inputs.size() == num_files_for_merge || &file == &sorted_files.back())
                {
                    MergingSortedBlockInputStream sorted_input(inputs, sort_description, rows_in_block);
                    new_files.emplace_back(flushToFile(getPath(), sample_block, sorted_input, codec));
                    inputs.clear();
                }
            }

            sorted_files.clear();
            sorted_files.swap(new_files);
        }

        for (const auto & file : sorted_files)
            inputs.emplace_back(streamFromFile(file));
    }

    MergingSortedBlockInputStream sorted_input(inputs, sort_description, rows_in_block);

    SortedFiles out = flushToManyFiles(getPath(), sample_block, sorted_input, codec, callback);
    sorted_files.clear();
    return out; /// There're also inserted_blocks counters as indirect output
}

BlockInputStreamPtr SortedBlocksWriter::streamFromFile(const TmpFilePtr & file) const
{
    return std::make_shared<TemporaryFileLazyInputStream>(file->path(), sample_block);
}

String SortedBlocksWriter::getPath() const
{
    return volume->getNextDisk()->getPath();
}

}
