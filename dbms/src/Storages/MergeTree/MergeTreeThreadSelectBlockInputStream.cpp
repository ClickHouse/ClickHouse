#include <Storages/MergeTree/MergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeThreadSelectBlockInputStream.h>


namespace DB
{


MergeTreeThreadSelectBlockInputStream::MergeTreeThreadSelectBlockInputStream(
    const size_t thread,
    const MergeTreeReadPoolPtr & pool,
    const size_t min_marks_to_read_,
    const UInt64 max_block_size_rows_,
    size_t preferred_block_size_bytes_,
    size_t preferred_max_column_in_block_size_bytes_,
    const MergeTreeData & storage_,
    const bool use_uncompressed_cache_,
    const PrewhereInfoPtr & prewhere_info_,
    const Settings & settings,
    const Names & virt_column_names_)
    :
    MergeTreeBaseSelectBlockInputStream{storage_, prewhere_info_, max_block_size_rows_,
        preferred_block_size_bytes_, preferred_max_column_in_block_size_bytes_, settings.min_bytes_to_use_direct_io,
        settings.max_read_buffer_size, use_uncompressed_cache_, true, virt_column_names_},
    thread{thread},
    pool{pool}
{
    /// round min_marks_to_read up to nearest multiple of block_size expressed in marks
    /// If granularity is adaptive it doesn't make sense
    /// Maybe it will make sence to add settings `max_block_size_bytes`
    if (max_block_size_rows && !storage.index_granularity_info.is_adaptive)
    {
        size_t fixed_index_granularity = storage.index_granularity_info.fixed_index_granularity;
        min_marks_to_read = (min_marks_to_read_ * fixed_index_granularity + max_block_size_rows - 1)
            / max_block_size_rows * max_block_size_rows / fixed_index_granularity;
    }
    else
        min_marks_to_read = min_marks_to_read_;

    ordered_names = getHeader().getNames();
}


Block MergeTreeThreadSelectBlockInputStream::getHeader() const
{
    auto res = pool->getHeader();
    executePrewhereActions(res, prewhere_info);
    injectVirtualColumns(res);
    return res;
}


/// Requests read task from MergeTreeReadPool and signals whether it got one
bool MergeTreeThreadSelectBlockInputStream::getNewTask()
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
        auto rest_mark_ranges = pool->getRestMarks(path, task->mark_ranges[0]);

        if (use_uncompressed_cache)
            owned_uncompressed_cache = storage.global_context.getUncompressedCache();
        owned_mark_cache = storage.global_context.getMarkCache();

        reader = std::make_unique<MergeTreeReader>(
            path, task->data_part, task->columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), save_marks_in_cache,
            storage, rest_mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size, MergeTreeReader::ValueSizeMap{}, profile_callback);

        if (prewhere_info)
            pre_reader = std::make_unique<MergeTreeReader>(
                path, task->data_part, task->pre_columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), save_marks_in_cache,
                storage, rest_mark_ranges, min_bytes_to_use_direct_io,
                max_read_buffer_size, MergeTreeReader::ValueSizeMap{}, profile_callback);
    }
    else
    {
        /// in other case we can reuse readers, anyway they will be "seeked" to required mark
        if (path != last_readed_part_path)
        {
            auto rest_mark_ranges = pool->getRestMarks(path, task->mark_ranges[0]);
            /// retain avg_value_size_hints
            reader = std::make_unique<MergeTreeReader>(
                path, task->data_part, task->columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), save_marks_in_cache,
                storage, rest_mark_ranges, min_bytes_to_use_direct_io, max_read_buffer_size,
                reader->getAvgValueSizeHints(), profile_callback);

            if (prewhere_info)
                pre_reader = std::make_unique<MergeTreeReader>(
                    path, task->data_part, task->pre_columns, owned_uncompressed_cache.get(), owned_mark_cache.get(), save_marks_in_cache,
                    storage, rest_mark_ranges, min_bytes_to_use_direct_io,
                    max_read_buffer_size, pre_reader->getAvgValueSizeHints(), profile_callback);
        }
    }
    last_readed_part_path = path;

    return true;
}


MergeTreeThreadSelectBlockInputStream::~MergeTreeThreadSelectBlockInputStream() = default;

}
