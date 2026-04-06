#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <vector>

#include <AggregateFunctions/ReservoirSampler.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

#include <base/JSON.h>

struct Stats
{
    using Sampler = ReservoirSampler<double>;
    /// All StatsCollector access is protected by Stats::mutex
    struct StatsCollector
    {
        size_t requests = 0;
        uint64_t requests_bytes = 0;
        Sampler sampler;

        /// requests/second, bytes/second
        std::pair<double, double> getThroughput(double elapsed_seconds) const;
        double getPercentile(double percent);

        void add(uint64_t microseconds, size_t requests_inc, size_t bytes_inc);
        void clear();
        void merge(const StatsCollector & from);
    };

    std::atomic<size_t> errors{0};

    Stopwatch elapsed;

    StatsCollector read_collector;
    StatsCollector write_collector;

    /// Per-operation-type stats
    std::map<Coordination::OpNum, StatsCollector> op_collectors;

    std::mutex mutex;

    void addRead(uint64_t microseconds, size_t requests_inc, size_t bytes_inc);
    void addWrite(uint64_t microseconds, size_t requests_inc, size_t bytes_inc);
    void addOp(Coordination::OpNum op_num, uint64_t microseconds, size_t requests_inc, size_t bytes_inc);

    /// Adds counters, merges samples, leaves `elapsed` unchanged.
    void merge(Stats & from);

    void clear();

    void report();
    void writeJSON(DB::WriteBuffer & out, int64_t start_timestamp);
};
