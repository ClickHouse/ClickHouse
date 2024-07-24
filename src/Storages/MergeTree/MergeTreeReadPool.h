#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Storages/MergeTree/IMergeTreeReadPool.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Interpreters/Context_fwd.h>

#include <mutex>


namespace DB
{

/** Provides read tasks for MergeTreeThreadSelectProcessor`s in fine-grained batches, allowing for more
 *  uniform distribution of work amongst multiple threads. All parts and their ranges are divided into `threads`
 *  workloads with at most `sum_marks / threads` marks. Then, threads are performing reads from these workloads
 *  in "sequential" manner, requesting work in small batches. As soon as some thread has exhausted
 *  it's workload, it either is signaled that no more work is available (`do_not_steal_tasks == false`) or
 *  continues taking small batches from other threads' workloads (`do_not_steal_tasks == true`).
 */
class MergeTreeReadPool : public IMergeTreeReadPool
{
public:
    struct BackoffSettings;

    MergeTreeReadPool(
        size_t threads_,
        size_t sum_marks_,
        size_t min_marks_for_concurrent_read_,
        RangesInDataParts && parts_,
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const Names & virtual_column_names_,
        ContextPtr context_,
        bool do_not_steal_tasks_ = false);

    ~MergeTreeReadPool() override = default;

    MergeTreeReadTaskPtr getTask(size_t thread) override;

    /** Each worker could call this method and pass information about read performance.
      * If read performance is too low, pool could decide to lower number of threads: do not assign more tasks to several threads.
      * This allows to overcome excessive load to disk subsystem, when reads are not from page cache.
      */
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo info) override;

    Block getHeader() const override;

    /** Pull could dynamically lower (backoff) number of threads, if read operation are too slow.
      * Settings for that backoff.
      */
    struct BackoffSettings
    {
        /// Pay attention only to reads, that took at least this amount of time. If set to 0 - means backoff is disabled.
        size_t min_read_latency_ms = 1000;
        /// Count events, when read throughput is less than specified bytes per second.
        size_t max_throughput = 1048576;
        /// Do not pay attention to event, if not enough time passed since previous event.
        size_t min_interval_between_events_ms = 1000;
        /// Number of events to do backoff - to lower number of threads in pool.
        size_t min_events = 2;
        /// Try keeping the minimal number of threads in pool.
        size_t min_concurrency = 1;

        /// Constants above is just an example.
        explicit BackoffSettings(const Settings & settings)
            : min_read_latency_ms(settings.read_backoff_min_latency_ms.totalMilliseconds()),
            max_throughput(settings.read_backoff_max_throughput),
            min_interval_between_events_ms(settings.read_backoff_min_interval_between_events_ms.totalMilliseconds()),
            min_events(settings.read_backoff_min_events),
            min_concurrency(settings.read_backoff_min_concurrency) {}

        BackoffSettings() : min_read_latency_ms(0) {}
    };

    struct PerPartParams
    {
        MergeTreeReadTaskColumns task_columns;
        NameSet column_name_set;
        MergeTreeBlockSizePredictorPtr size_predictor;
        RangesInDataPart data_part;
    };

    static std::vector<size_t> fillPerPartInfo(
        const RangesInDataParts & parts,
        const StorageSnapshotPtr & storage_snapshot,
        std::vector<bool> & is_part_on_remote_disk,
        bool & predict_block_size_bytes,
        const Names & column_names,
        const Names & virtual_column_names,
        const PrewhereInfoPtr & prewhere_info,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        std::vector<MergeTreeReadPool::PerPartParams> & per_part_params);

private:
    void fillPerThreadInfo(
        size_t threads, size_t sum_marks, std::vector<size_t> per_part_sum_marks,
        const RangesInDataParts & parts);

    /// Initialized in constructor
    StorageSnapshotPtr storage_snapshot;
    const Names column_names;
    const Names virtual_column_names;
    size_t min_marks_for_concurrent_read{0};
    PrewhereInfoPtr prewhere_info;
    ExpressionActionsSettings actions_settings;
    MergeTreeReaderSettings reader_settings;
    RangesInDataParts parts_ranges;
    bool predict_block_size_bytes;
    bool do_not_steal_tasks;
    bool merge_tree_use_const_size_tasks_for_remote_reading = false;

    std::vector<PerPartParams> per_part_params;
    std::vector<bool> is_part_on_remote_disk;

    BackoffSettings backoff_settings;

    mutable std::mutex mutex;
    /// State to track numbers of slow reads.
    struct BackoffState
    {
        size_t current_threads;
        Stopwatch time_since_prev_event {CLOCK_MONOTONIC_COARSE};
        size_t num_events = 0;

        explicit BackoffState(size_t threads) : current_threads(threads) {}
    };
    BackoffState backoff_state;

    struct Part
    {
        MergeTreeData::DataPartPtr data_part;
        size_t part_index_in_query;
    };

    std::vector<Part> parts_with_idx;

    struct ThreadTask
    {
        struct PartIndexAndRange
        {
            size_t part_idx;
            MarkRanges ranges;
        };

        std::vector<PartIndexAndRange> parts_and_ranges;
        std::vector<size_t> sum_marks_in_parts;
    };

    std::vector<ThreadTask> threads_tasks;
    std::set<size_t> remaining_thread_tasks;
    Poco::Logger * log = &Poco::Logger::get("MergeTreeReadPool");

};

class MergeTreeReadPoolParallelReplicas : public IMergeTreeReadPool
{
public:
    MergeTreeReadPoolParallelReplicas(
        StorageSnapshotPtr storage_snapshot_,
        size_t threads_,
        ParallelReadingExtension extension_,
        const RangesInDataParts & parts_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const Names & virtual_column_names_,
        size_t min_marks_for_concurrent_read_)
        : extension(extension_)
        , threads(threads_)
        , prewhere_info(prewhere_info_)
        , actions_settings(actions_settings_)
        , reader_settings(reader_settings_)
        , storage_snapshot(storage_snapshot_)
        , min_marks_for_concurrent_read(min_marks_for_concurrent_read_)
        , column_names(column_names_)
        , virtual_column_names(virtual_column_names_)
        , parts_ranges(std::move(parts_))
    {
        MergeTreeReadPool::fillPerPartInfo(
            parts_ranges, storage_snapshot, is_part_on_remote_disk,
            predict_block_size_bytes, column_names, virtual_column_names, prewhere_info,
            actions_settings, reader_settings, per_part_params);

        extension.all_callback(InitialAllRangesAnnouncement(
            CoordinationMode::Default,
            parts_ranges.getDescriptions(),
            extension.number_of_current_replica
        ));
    }

    ~MergeTreeReadPoolParallelReplicas() override;

    Block getHeader() const override;

    MergeTreeReadTaskPtr getTask(size_t thread) override;

    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}

private:
    ParallelReadingExtension extension;

    RangesInDataPartsDescription buffered_ranges;
    size_t threads;
    bool no_more_tasks_available{false};
    Poco::Logger * log = &Poco::Logger::get("MergeTreeReadPoolParallelReplicas");

    std::mutex mutex;

    PrewhereInfoPtr prewhere_info;
    ExpressionActionsSettings actions_settings;
    MergeTreeReaderSettings reader_settings;
    StorageSnapshotPtr storage_snapshot;
    size_t min_marks_for_concurrent_read;
    const Names column_names;
    const Names virtual_column_names;
    RangesInDataParts parts_ranges;

    bool predict_block_size_bytes = false;
    std::vector<bool> is_part_on_remote_disk;
    std::vector<MergeTreeReadPool::PerPartParams> per_part_params;
};

using MergeTreeReadPoolParallelReplicasPtr = std::shared_ptr<MergeTreeReadPoolParallelReplicas>;


class MergeTreeInOrderReadPoolParallelReplicas : private boost::noncopyable
{
public:
    MergeTreeInOrderReadPoolParallelReplicas(
        RangesInDataParts parts_,
        ParallelReadingExtension extension_,
        CoordinationMode mode_,
        size_t min_marks_for_concurrent_read_)
        : parts_ranges(parts_)
        , extension(extension_)
        , mode(mode_)
        , min_marks_for_concurrent_read(min_marks_for_concurrent_read_)
    {
        for (const auto & part : parts_ranges)
            request.push_back({part.data_part->info, MarkRanges{}});

        for (const auto & part : parts_ranges)
            buffered_tasks.push_back({part.data_part->info, MarkRanges{}});

        extension.all_callback(InitialAllRangesAnnouncement(
            mode,
            parts_ranges.getDescriptions(),
            extension.number_of_current_replica
        ));
    }

    MarkRanges getNewTask(RangesInDataPartDescription description);


    RangesInDataParts parts_ranges;
    ParallelReadingExtension extension;
    CoordinationMode mode;
    size_t min_marks_for_concurrent_read{0};

    bool no_more_tasks{false};
    RangesInDataPartsDescription request;
    RangesInDataPartsDescription buffered_tasks;

    std::mutex mutex;
};

using MergeTreeInOrderReadPoolParallelReplicasPtr = std::shared_ptr<MergeTreeInOrderReadPoolParallelReplicas>;

}
