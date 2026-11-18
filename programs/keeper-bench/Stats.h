#pragma once

#include <atomic>
#include <mutex>

#include <AggregateFunctions/ReservoirSampler.h>
#include <Common/Stopwatch.h>

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
    std::atomic<size_t> watches_fired{0};

    Stopwatch elapsed;

    StatsCollector read_collector;
    StatsCollector write_collector;

    std::mutex mutex;

    void addRead(uint64_t microseconds, size_t requests_inc, size_t bytes_inc);
    void addWrite(uint64_t microseconds, size_t requests_inc, size_t bytes_inc);

    /// Adds counters, merges samples, leaves `elapsed` unchanged.
    void merge(Stats & from);

    /// Atomically moves collectors and counters into `target` and clears this object.
    /// Holds this->mutex during the operation so no concurrent `add*` calls are lost.
    /// `target` must not be concurrently accessed by other threads.
    void extractInto(Stats & target);

    void clear();

    /// Request counts and error/watch totals are taken from `cumulative`
    /// (showing running totals), while throughput and latency are computed
    /// from this object (showing per-period values). Pass `*this` when
    /// the report itself is the cumulative one (e.g. the final report).
    void report(const Stats & cumulative);
    void writeJSON(DB::WriteBuffer & out, int64_t start_timestamp);
};
