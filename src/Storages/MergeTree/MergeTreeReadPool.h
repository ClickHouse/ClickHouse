#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Storages/SelectQueryInfo.h>

#include <mutex>


namespace DB
{

using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;


class IMergeTreeReadPool
{
public:
    IMergeTreeReadPool(
        StorageSnapshotPtr storage_snapshot_,
        Names column_names_,
        Names virtual_column_names_,
        size_t min_marks_for_concurrent_read_,
        PrewhereInfoPtr prewhere_info_,
        RangesInDataParts parts_ranges_,
        bool predict_block_size_bytes_,
        bool do_not_steal_tasks_)
        : storage_snapshot(storage_snapshot_)
        , column_names(column_names_)
        , virtual_column_names(virtual_column_names_)
        , min_marks_for_concurrent_read(min_marks_for_concurrent_read_)
        , prewhere_info(prewhere_info_)
        , parts_ranges(parts_ranges_)
        , predict_block_size_bytes(predict_block_size_bytes_)
        , do_not_steal_tasks(do_not_steal_tasks_)
    {}

    virtual MergeTreeReadTaskPtr getTask(size_t thread) = 0;
    virtual Block getHeader() const = 0;
    virtual void profileFeedback(ReadBufferFromFileBase::ProfileInfo info) = 0;
    virtual ~IMergeTreeReadPool() = default;

protected:

    std::vector<size_t> fillPerPartInfo(const RangesInDataParts & parts);

    /// Initialized in constructor
    StorageSnapshotPtr storage_snapshot;
    const Names column_names;
    const Names virtual_column_names;
    size_t min_marks_for_concurrent_read{0};
    PrewhereInfoPtr prewhere_info;
    RangesInDataParts parts_ranges;
    bool predict_block_size_bytes;
    bool do_not_steal_tasks;

    struct PerPartParams
    {
        MergeTreeReadTaskColumns task_columns;
        NameSet column_name_set;
        MergeTreeBlockSizePredictorPtr size_predictor;
        RangesInDataPart data_part;
    };

    std::vector<PerPartParams> per_part_params;
    std::vector<bool> is_part_on_remote_disk;

    mutable std::mutex mutex;
};

using IMergeTreeReadPoolPtr = std::shared_ptr<IMergeTreeReadPool>;

/**   Provides read tasks for MergeTreeThreadSelectProcessor`s in fine-grained batches, allowing for more
 *    uniform distribution of work amongst multiple threads. All parts and their ranges are divided into `threads`
 *    workloads with at most `sum_marks / threads` marks. Then, threads are performing reads from these workloads
 *    in "sequential" manner, requesting work in small batches. As soon as some thread has exhausted
 *    it's workload, it either is signaled that no more work is available (`do_not_steal_tasks == false`) or
 *    continues taking small batches from other threads' workloads (`do_not_steal_tasks == true`).
 */
class MergeTreeReadPool final: public IMergeTreeReadPool, private boost::noncopyable
{
public:
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
            min_concurrency(settings.read_backoff_min_concurrency)
        {
        }

        BackoffSettings() : min_read_latency_ms(0) {}
    };

    BackoffSettings backoff_settings;

private:
    /** State to track numbers of slow reads.
      */
    struct BackoffState
    {
        size_t current_threads;
        Stopwatch time_since_prev_event {CLOCK_MONOTONIC_COARSE};
        size_t num_events = 0;

        explicit BackoffState(size_t threads) : current_threads(threads) {}
    };

    BackoffState backoff_state;

public:
    MergeTreeReadPool(
        size_t threads_,
        size_t sum_marks_,
        size_t min_marks_for_concurrent_read_,
        RangesInDataParts && parts_,
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const Names & column_names_,
        const Names & virtual_column_names_,
        const BackoffSettings & backoff_settings_,
        size_t preferred_block_size_bytes_,
        bool do_not_steal_tasks_ = false);

    ~MergeTreeReadPool() override = default;
    MergeTreeReadTaskPtr getTask(size_t thread) override;

    /** Each worker could call this method and pass information about read performance.
      * If read performance is too low, pool could decide to lower number of threads: do not assign more tasks to several threads.
      * This allows to overcome excessive load to disk subsystem, when reads are not from page cache.
      */
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo info) override;

    Block getHeader() const override;

private:

    void fillPerThreadInfo(
        size_t threads, size_t sum_marks, std::vector<size_t> per_part_sum_marks,
        const RangesInDataParts & parts);

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

using MergeTreeReadPoolPtr = std::shared_ptr<MergeTreeReadPool>;

class MergeTreeReadPoolParallelReplicas : public IMergeTreeReadPool, private boost::noncopyable
{
public:

    MergeTreeReadPoolParallelReplicas(
        StorageSnapshotPtr storage_snapshot_,
        size_t threads_,
        ParallelReadingExtension extension_,
        const RangesInDataParts & parts_,
        const PrewhereInfoPtr & prewhere_info_,
        const Names & column_names_,
        const Names & virtual_column_names_,
        size_t min_marks_for_concurrent_read_
    )
    : IMergeTreeReadPool(
        storage_snapshot_,
        column_names_,
        virtual_column_names_,
        min_marks_for_concurrent_read_,
        prewhere_info_,
        parts_,
        /*predict_block_size*/false,
        /*do_not_steal_tasks*/false)
    , extension(extension_)
    , threads(threads_)
    {
        fillPerPartInfo(parts_ranges);

        extension.all_callback({
            .description = parts_ranges.getDescriptions(),
            .replica_num = extension.number_of_current_replica
        });
    }

    ~MergeTreeReadPoolParallelReplicas() override;

    MergeTreeReadTaskPtr getTask(size_t thread) override;
    Block getHeader() const override;
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}

private:
    ParallelReadingExtension extension;

    RangesInDataPartsDescription buffered_ranges;
    size_t threads;
    bool no_more_tasks_available{false};
    Poco::Logger * log = &Poco::Logger::get("MergeTreeReadPoolParallelReplicas");
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

        extension.all_callback({
            .description = parts_ranges.getDescriptions(),
            .replica_num = extension.number_of_current_replica
        });
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
