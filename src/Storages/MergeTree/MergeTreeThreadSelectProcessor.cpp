#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeThreadSelectProcessor.h>
#include <Interpreters/Context.h>

#include <consistent_hashing.h>

namespace DB
{


MergeTreeThreadSelectProcessor::MergeTreeThreadSelectProcessor(
    const size_t thread_,
    const MergeTreeReadPoolPtr & pool_,
    const size_t min_marks_to_read_,
    const UInt64 max_block_size_rows_,
    size_t preferred_block_size_bytes_,
    size_t preferred_max_column_in_block_size_bytes_,
    const MergeTreeData & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const bool use_uncompressed_cache_,
    const PrewhereInfoPtr & prewhere_info_,
    ExpressionActionsSettings actions_settings,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & virt_column_names_,
    std::optional<ParallelReadingExtension> extension_)
    :
    MergeTreeBaseSelectProcessor{
        pool_->getHeader(), storage_, metadata_snapshot_, prewhere_info_, std::move(actions_settings), max_block_size_rows_,
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
            sum_average_marks_size = 8UL * 1024 * 1024 * 10; // 10Mib

        min_marks_to_read = extension->count_participating_replicas * (8UL * 1024 * 1024 * 1024) / sum_average_marks_size;
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


void MergeTreeThreadSelectProcessor::processNewTask()
{
    const size_t max_batch_size = estimateMaxBatchSizeForHugeRanges();

    auto predicate = [this](MarkRange range)
    {
        auto what = fmt::format("{}_{}_{}", task->data_part->info.getPartName(), range.begin, range.end);
        auto hash = CityHash_v1_0_2::CityHash64(what.data(), what.size());
        auto consistent_hash = ConsistentHashing(hash, extension->count_participating_replicas);
        return consistent_hash == extension->number_of_current_replica;
    };

    // LOG_TRACE(log, "Using max batch size to perform a request equals {} marks", max_batch_size);

    size_t current_batch_size = 0;

    buffered_ranges.emplace_back();

    MarkRanges delayed_ranges;

    for (const auto & range : task->mark_ranges)
    {
        auto expand_if_needed = [&]
        {
            if (current_batch_size > max_batch_size)
            {
                buffered_ranges.emplace_back();
                current_batch_size = 0;
            }

        };

        expand_if_needed();

        if (range.end - range.begin < max_batch_size)
        {
            if (predicate(range))
            {
                buffered_ranges.back().push_back(range);
                current_batch_size += range.end - range.begin;
            }
            else
            {
                delayed_ranges.emplace_back(range);
            }
            continue;
        }

        auto current_begin = range.begin;
        auto current_end = range.begin + max_batch_size;

        while (current_end < range.end)
        {
            auto current_range = MarkRange{current_begin, current_end};
            if (predicate(current_range))
            {
                buffered_ranges.back().push_back(current_range);
                current_batch_size += current_end - current_begin;
            }
            else
            {
                delayed_ranges.emplace_back(current_range);
            }

            current_begin = current_end;
            current_end = current_end + max_batch_size;

            expand_if_needed();
        }

        if (range.end - current_begin > 0)
        {
            auto current_range = MarkRange{current_begin, range.end};
            if (predicate(current_range))
            {
                buffered_ranges.back().push_back(current_range);
                current_batch_size += range.end - current_begin;

                /// Do not need to update current_begin and current_end

                expand_if_needed();
            }
            else
            {
                delayed_ranges.emplace_back(current_range);
            }
        }
    }

    if (buffered_ranges.back().empty())
        buffered_ranges.pop_back();

    if (!delayed_ranges.empty())
    {
        auto delayed_task = std::make_unique<MergeTreeReadTask>(*task); // Create a copy
        delayed_task->mark_ranges = std::move(delayed_ranges);
        delayed_tasks.emplace_back(std::move(delayed_task));
    }
}


void MergeTreeThreadSelectProcessor::finalizeNewTask()
{
    const std::string part_name = task->data_part->isProjectionPart() ? task->data_part->getParentPart()->name : task->data_part->name;

    /// Allows pool to reduce number of threads in case of too slow reads.
    auto profile_callback = [this](ReadBufferFromFileBase::ProfileInfo info_) { pool->profileFeedback(info_); };

    if (!reader)
    {
        if (use_uncompressed_cache)
            owned_uncompressed_cache = storage.getContext()->getUncompressedCache();
        owned_mark_cache = storage.getContext()->getMarkCache();

        reader = task->data_part->getReader(task->columns, metadata_snapshot, task->mark_ranges,
            owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings,
            IMergeTreeReader::ValueSizeMap{}, profile_callback);

        if (prewhere_info)
            pre_reader = task->data_part->getReader(task->pre_columns, metadata_snapshot, task->mark_ranges,
                owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings,
                IMergeTreeReader::ValueSizeMap{}, profile_callback);
    }
    else
    {
        /// in other case we can reuse readers, anyway they will be "seeked" to required mark
        if (part_name != last_readed_part_name)
        {
            /// retain avg_value_size_hints
            reader = task->data_part->getReader(task->columns, metadata_snapshot, task->mark_ranges,
                owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings,
                reader->getAvgValueSizeHints(), profile_callback);

            if (prewhere_info)
                pre_reader = task->data_part->getReader(task->pre_columns, metadata_snapshot, task->mark_ranges,
                owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings,
                reader->getAvgValueSizeHints(), profile_callback);
        }
    }

    last_readed_part_name = part_name;
}


void MergeTreeThreadSelectProcessor::finish()
{
    reader.reset();
    pre_reader.reset();
}


MergeTreeThreadSelectProcessor::~MergeTreeThreadSelectProcessor() = default;

}
