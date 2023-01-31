#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeThreadSelectProcessor.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeThreadSelectProcessor::MergeTreeThreadSelectProcessor(
    size_t thread_,
    const MergeTreeReadPoolPtr & pool_,
    size_t min_marks_to_read_,
    UInt64 max_block_size_rows_,
    size_t preferred_block_size_bytes_,
    size_t preferred_max_column_in_block_size_bytes_,
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    bool use_uncompressed_cache_,
    const PrewhereInfoPtr & prewhere_info_,
    ExpressionActionsSettings actions_settings,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & virt_column_names_,
    std::optional<ParallelReadingExtension> extension_)
    :
    MergeTreeBaseSelectProcessor{
        pool_->getHeader(), storage_, storage_snapshot_, prewhere_info_, std::move(actions_settings), max_block_size_rows_,
        preferred_block_size_bytes_, preferred_max_column_in_block_size_bytes_,
        reader_settings_, use_uncompressed_cache_, virt_column_names_, extension_},
    thread{thread_},
    pool{pool_}
{
    /// round min_marks_to_read up to nearest multiple of block_size expressed in marks
    /// If granularity is adaptive it doesn't make sense
    /// Maybe it will make sense to add settings `max_block_size_bytes`
    if (max_block_size_rows && !storage.canUseAdaptiveGranularity())
    {
        size_t fixed_index_granularity = storage.getSettings()->index_granularity;
        min_marks_to_read = (min_marks_to_read_ * fixed_index_granularity + max_block_size_rows - 1)
            / max_block_size_rows * max_block_size_rows / fixed_index_granularity;
    }
    else if (extension.has_value())
    {
        /// Parallel reading from replicas is enabled.
        /// We try to estimate the average number of bytes in a granule
        /// to make one request over the network per one gigabyte of data
        /// Actually we will ask MergeTreeReadPool to provide us heavier tasks to read
        /// because the most part of each task will be postponed
        /// (due to using consistent hash for better cache affinity)
        const size_t amount_of_read_bytes_per_one_request = 1024 * 1024 * 1024; // 1GiB
        /// In case of reading from compact parts (for which we can't estimate the average size of marks)
        /// we will use this value
        const size_t empirical_size_of_mark = 1024 * 1024 * 10; // 10 MiB

        if (extension->colums_to_read.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A set of column to read is empty. It is a bug");

        size_t sum_average_marks_size = 0;
        auto column_sizes = storage.getColumnSizes();
        for (const auto & name : extension->colums_to_read)
        {
            auto it = column_sizes.find(name);
            if (it == column_sizes.end())
                continue;
            auto size = it->second;

            if (size.data_compressed == 0 || size.data_uncompressed == 0 || size.marks == 0)
                continue;

            sum_average_marks_size += size.data_uncompressed / size.marks;
        }

        if (sum_average_marks_size == 0)
            sum_average_marks_size = empirical_size_of_mark * extension->colums_to_read.size();

        min_marks_to_read = extension->count_participating_replicas * amount_of_read_bytes_per_one_request / sum_average_marks_size;
    }
    else
    {
        min_marks_to_read = min_marks_to_read_;
    }


    ordered_names = getPort().getHeader().getNames();
}

/// Requests read task from MergeTreeReadPool and signals whether it got one
bool MergeTreeThreadSelectProcessor::getNewTaskImpl()
{
    task = pool->getTask(min_marks_to_read, thread, ordered_names);
    return static_cast<bool>(task);
}


void MergeTreeThreadSelectProcessor::finalizeNewTask()
{
    const std::string part_name = task->data_part->isProjectionPart() ? task->data_part->getParentPart()->name : task->data_part->name;

    /// Allows pool to reduce number of threads in case of too slow reads.
    auto profile_callback = [this](ReadBufferFromFileBase::ProfileInfo info_) { pool->profileFeedback(info_); };
    const auto & metadata_snapshot = storage_snapshot->metadata;

    IMergeTreeReader::ValueSizeMap value_size_map;

    if (!reader)
    {
        if (use_uncompressed_cache)
            owned_uncompressed_cache = storage.getContext()->getUncompressedCache();
        owned_mark_cache = storage.getContext()->getMarkCache();
    }
    else if (part_name != last_readed_part_name)
    {
        value_size_map = reader->getAvgValueSizeHints();
    }

    const bool init_new_readers = !reader || part_name != last_readed_part_name;
    if (init_new_readers)
    {
        initializeMergeTreeReadersForPart(task->data_part, task->task_columns, metadata_snapshot,
            task->mark_ranges, value_size_map, profile_callback);
    }

    last_readed_part_name = part_name;
}


void MergeTreeThreadSelectProcessor::finish()
{
    reader.reset();
    pre_reader_for_step.clear();
}


MergeTreeThreadSelectProcessor::~MergeTreeThreadSelectProcessor() = default;

}
