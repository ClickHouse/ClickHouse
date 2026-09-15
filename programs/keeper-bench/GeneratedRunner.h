#pragma once

#include <Common/CacheLine.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>

#include <Generator.h>
#include <NodesSetup.h>
#include <RunnerCommon.h>
#include <Stats.h>

#include <atomic>
#include <mutex>
#include <optional>
#include <unordered_map>

/// Benchmarks a Keeper cluster with synthetic requests produced by `Generator`
/// according to the `generator` config section.
class GeneratedRunner
{
private:
    struct alignas(DB::CH_CACHE_LINE_SIZE) ThreadState
    {
        size_t thread_idx = 0;
        pcg64 rng;
        Stats thread_info;
    };

public:
    GeneratedRunner(
        std::optional<size_t> concurrency_,
        const std::string & config_path,
        const Strings & hosts_strings_,
        std::optional<double> max_time_,
        std::optional<double> delay_,
        std::optional<bool> continue_on_error_,
        std::optional<size_t> max_iterations_);

    void runBenchmark();

    ~GeneratedRunner();

private:
    void thread(std::vector<std::shared_ptr<Coordination::ZooKeeper>> zookeepers, ThreadState & thread_state);

    void printNumberOfRequestsExecuted(size_t num);

    void createConnections();
    std::vector<std::shared_ptr<Coordination::ZooKeeper>> refreshConnections();

    std::shared_ptr<Stats> mergeThreadInfos();

    size_t concurrency = 1;
    size_t pipeline_depth = 1;

    std::optional<ThreadPool> pool;

    DB::ConfigurationPtr config_ptr;

    double max_time = 0;
    double delay = 1;
    bool continue_on_error = false;
    bool enable_tracing = false;
    size_t max_iterations = 0;

    /// Iteration counter, excluding requests during warmup. This is what max_iterations limits.
    std::atomic<size_t> requests_started = 0;
    std::atomic<bool> shutdown = false;

    double warmup_seconds = 0;
    std::atomic<bool> warmup_complete = false;

    std::shared_ptr<Stats> info;
    BenchmarkOutput output;

    /// Shared by all worker threads; immutable after startup.
    std::shared_ptr<Generator> generator;

    Stopwatch total_watch;
    Stopwatch delay_watch;

    std::vector<ThreadState> threads;

    std::mutex mutex; // for merging and reporting thread stats

    std::mutex connection_mutex;
    ConnectionFactory connection_factory;
    std::vector<std::shared_ptr<Coordination::ZooKeeper>> connections;
    std::unordered_map<size_t, size_t> connections_to_info_map;

    NodesSetup nodes_setup;
};
