#pragma once

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <Common/CacheLine.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/NonblockingBoundedQueue.h>
#include <Common/SharedMutex.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperStorage.h>
#include <Coordination/KeeperStorage_fwd.h>
#include <Interpreters/Context.h>

#include <Generator.h>
#include <Runner.h>

/// In-process benchmark for `KeeperStorage` alone (no network, no raft, no state machine).
///
/// Threading model mirrors a simplified Keeper commit path:
///   - N generator threads produce requests using the same `Generator` as the regular keeper-bench mode.
///   - 1 "preprocess" thread calls `KeeperStorage::preprocessRequest` for every write
///     (stands in for the path that, in real keeper, runs under `raft_server::lock_`).
///   - 1 "commit" thread calls `processRequest` for writes (the commit thread in real keeper)
///     and periodically drains the read queue with `processLocalRequests`.
///
/// All inter-thread communication uses `NonblockingBoundedQueue` with 100us polling —
/// no condition variables.
class StorageRunner
{
public:
    StorageRunner(
        const std::string & config_path,
        std::optional<size_t> concurrency,
        std::optional<double> max_time,
        std::optional<double> report_delay,
        std::optional<size_t> max_iterations,
        std::optional<bool> continue_on_error);

    void runBenchmark();

    ~StorageRunner();

private:
    using Storage = DB::KeeperMemoryStorage;

    struct QueueItem
    {
        Coordination::ZooKeeperRequestPtr request;
        int64_t session_id = 0;
        int64_t zxid = 0;
        Coordination::OpNum op_num = Coordination::OpNum::Error;
        bool is_write = false;
        std::function<void(const Coordination::Response *)> callback;
    };

    static_assert(std::is_nothrow_move_assignable_v<QueueItem>);

    /// One counter per op-num, limited to a small contiguous range.
    static constexpr size_t NUM_OP_SLOTS = 32;
    static size_t opIndex(Coordination::OpNum op_num);

    struct PerOpStats
    {
        std::atomic<uint64_t> count{0};
        /// Time spent in `processRequest` (writes) or in `processLocalRequests` attributed to this request (reads).
        std::atomic<uint64_t> process_ns{0};
        /// Time spent in `preprocessRequest` for this request. Zero for reads.
        std::atomic<uint64_t> preprocess_ns{0};
        /// For list/list-recursive requests only: total number of child names returned in the response.
        std::atomic<uint64_t> list_entries{0};
    };

    struct PeriodStats
    {
        alignas(DB::CH_CACHE_LINE_SIZE) std::atomic<uint64_t> writes_committed{0};
        alignas(DB::CH_CACHE_LINE_SIZE) std::atomic<uint64_t> reads_committed{0};
        alignas(DB::CH_CACHE_LINE_SIZE) std::atomic<uint64_t> preprocess_busy_ns{0};
        alignas(DB::CH_CACHE_LINE_SIZE) std::atomic<uint64_t> commit_write_busy_ns{0};
        alignas(DB::CH_CACHE_LINE_SIZE) std::atomic<uint64_t> commit_read_busy_ns{0};
        std::array<PerOpStats, NUM_OP_SLOTS> per_op;
    };

    void parseConfig(const Poco::Util::AbstractConfiguration & config);
    void setupStorage();
    void startGenerators();
    void generatorThread(size_t idx);
    void preprocessThread();
    void commitThread();
    void report(double period_seconds, bool snapshot_mode_during_period);

    /// Push with 100us polling.
    template <typename QueueT>
    void pushBlocking(QueueT & queue, QueueItem && item);

    std::string config_path;
    DB::ConfigurationPtr config_ptr;

    size_t concurrency = 4;
    double max_time = 0;
    double report_delay = 1;
    size_t max_iterations = 0;
    bool continue_on_error = false;

    /// Storage-benchmark-specific params (under `storage.*` in config).
    size_t writes_per_read_batch = 10;
    size_t snapshot_toggle_periods = 0;
    size_t preprocess_commit_queue_size = 32;
    size_t input_queue_size = 2048;
    int64_t tick_time_ms = 500;

    DB::KeeperContextPtr keeper_context;

    DB::SharedMutex state_machine_storage_mutex;
    std::unique_ptr<Storage> storage;

    BenchmarkContext benchmark_context;
    std::vector<std::shared_ptr<Generator>> generators;
    std::vector<int64_t> generator_session_ids;
    int64_t setup_session_id = 0;

    std::unique_ptr<NonblockingBoundedQueue<QueueItem>> preprocess_queue;
    std::unique_ptr<NonblockingBoundedQueue<QueueItem>> read_queue;
    std::unique_ptr<NonblockingBoundedQueue<QueueItem>> commit_queue;

    std::atomic<bool> shutdown{false};
    std::atomic<bool> generators_done{false};
    std::atomic<size_t> requests_started{0};

    std::atomic<int64_t> next_zxid{1};

    PeriodStats period_stats;

    std::atomic<bool> snapshot_enabled{false};

    DB::SharedContextHolder shared_context;
    DB::ContextMutablePtr global_context;

    std::vector<std::thread> generator_threads;
    std::unique_ptr<std::thread> preprocess_thread_handle;
    std::unique_ptr<std::thread> commit_thread_handle;

    Stopwatch total_watch;
};
