#include <Storages/MergeTree/MergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeThreadBlockInputStream.h>


namespace DB
{


MergeTreeThreadBlockInputStream::MergeTreeThreadBlockInputStream(
    const size_t thread,
    const MergeTreeReadPoolPtr & pool,
    const size_t min_marks_to_read_,
    const size_t max_block_size_rows,
    size_t preferred_block_size_bytes,
    size_t preferred_max_column_in_block_size_bytes,
    MergeTreeData & storage,
    const bool use_uncompressed_cache,
    const PrewhereInfoPtr & prewhere_info,
    const Settings & settings,
    const Names & virt_column_names)
    :
    MergeTreeBaseBlockInputStream{storage, prewhere_info, max_block_size_rows,
        preferred_block_size_bytes, preferred_max_column_in_block_size_bytes, settings.min_bytes_to_use_direct_io,
        settings.max_read_buffer_size, use_uncompressed_cache, true, virt_column_names},
    thread{thread},
    pool{pool}
{
    /// round min_marks_to_read up to nearest multiple of block_size expressed in marks
    if (max_block_size_rows)
    {
        min_marks_to_read = (min_marks_to_read_ * storage.index_granularity + max_block_size_rows - 1)
                            / max_block_size_rows * max_block_size_rows / storage.index_granularity;
    }
    else
        min_marks_to_read = min_marks_to_read_;

    ordered_names = getHeader().getNames();
}


Block MergeTreeThreadBlockInputStream::getHeader() const
{
    auto res = pool->getHeader();
    injectVirtualColumns(res);
    executePrewhereActions(res, prewhere_info);
    return res;
}


/// Requests read task from MergeTreeReadPool and signals whether it got one
bool MergeTreeThreadBlockInputStream::getNewTask()
{
    task = pool->getTask(min_marks_to_read, thread, ordered_names);

    if (!task)
    {
        /** Close the files (before destroying the object).
          * When many sources are created, but simultaneously reading only a few of them,
          * buffers don't waste memory.
          */
        reader.reset();
        pre_reader.reset();
        return false;
    }

    const std::string path = task->data_part->getFullPath();

    /// Allows pool to reduce number of threads in case of too slow reads.
    auto profile_callback = [this](ReadBufferFromFileBase::ProfileInfo info) { pool->profileFeedback(info); };

    if (!reader)
    {
        if (use_uncompressed_cache)
            owned_uncompressed_cache = storage.context.getUncompressedCache();

        owned_mark_cache = storage.context.getMarkCache();

        reader = std::make_unique<MergeTreeReader>(
            path, task->data_part, task->columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), save_marks_in_cache,
            storage, task->mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size, MergeTreeReader::ValueSizeMap{}, profile_callback);

        if (prewhere_info)
            pre_reader = std::make_unique<MergeTreeReader>(
                path, task->data_part, task->pre_columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), save_marks_in_cache,
                storage, task->mark_ranges, min_bytes_to_use_direct_io,
                max_read_buffer_size, MergeTreeReader::ValueSizeMap{}, profile_callback);
    }
    else
    {
        /// retain avg_value_size_hints
        reader = std::make_unique<MergeTreeReader>(
            path, task->data_part, task->columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), save_marks_in_cache,
            storage, task->mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size,
            reader->getAvgValueSizeHints(), profile_callback);

        if (prewhere_info)
            pre_reader = std::make_unique<MergeTreeReader>(
                path, task->data_part, task->pre_columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), save_marks_in_cache,
                storage, task->mark_ranges, min_bytes_to_use_direct_io,
                max_read_buffer_size, pre_reader->getAvgValueSizeHints(), profile_callback);
    }

    return true;
}


MergeTreeThreadBlockInputStream::~MergeTreeThreadBlockInputStream() = default;

}
