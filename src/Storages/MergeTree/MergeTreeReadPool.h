#pragma once
#include <Storages/MergeTree/MergeTreeReadPoolBase.h>
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <mutex>


namespace DB
{

/** Provides read tasks for MergeTreeThreadSelectAlgorithm in fine-grained batches, allowing for more
 *  uniform distribution of work amongst multiple threads. All parts and their ranges are divided into `threads`
 *  workloads with at most `sum_marks / threads` marks. Then, threads are performing reads from these workloads
 *  in "sequential" manner, requesting work in small batches. As soon as some thread has exhausted
 *  it's workload, it either is signaled that no more work is available (`do_not_steal_tasks == false`) or
 *  continues taking small batches from other threads' workloads (`do_not_steal_tasks == true`).
 */
class MergeTreeReadPool : public MergeTreeReadPoolBase
{
public:
    struct BackoffSettings;

    MergeTreeReadPool(
        RangesInDataParts && parts_,
        MutationsSnapshotPtr mutations_snapshot_,
        VirtualFields shared_virtual_fields_,
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const PoolSettings & settings_,
        const MergeTreeReadTask::BlockSizeParams & params_,
        const ContextPtr & context_);

    ~MergeTreeReadPool() override = default;

    String getName() const override { return "ReadPool"; }
    bool preservesOrderOfRanges() const override { return false; }
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;

    /** Each worker could call this method and pass information about read performance.
      * If read performance is too low, pool could decide to lower number of threads: do not assign more tasks to several threads.
      * This allows to overcome excessive load to disk subsystem, when reads are not from page cache.
      */
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo info) override;

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
        explicit BackoffSettings(const Settings & settings);

        BackoffSettings() : min_read_latency_ms(0) {}
    };

private:
    void fillPerThreadInfo(size_t threads, size_t sum_marks);

    mutable std::mutex mutex;

    /// State to track numbers of slow reads.
    struct BackoffState
    {
        size_t current_threads;
        Stopwatch time_since_prev_event {CLOCK_MONOTONIC_COARSE};
        size_t num_events = 0;

        explicit BackoffState(size_t threads) : current_threads(threads) {}
    };

    const BackoffSettings backoff_settings;
    BackoffState backoff_state;

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

    LoggerPtr log = getLogger("MergeTreeReadPool");
};

}
