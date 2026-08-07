#pragma once

#include <vector>
#include <atomic>

#include <AggregateFunctions/ReservoirSampler.h>

#include <base/JSON.h>

struct Stats
{
    size_t errors = 0;

    using Sampler = ReservoirSampler<double>;
    struct StatsCollector
    {
        std::atomic<size_t> requests{0};
        uint64_t requests_bytes = 0;
        uint64_t work_time = 0;
        Sampler sampler;

        /// requests/second, bytes/second
        std::pair<double, double> getThroughput(size_t concurrency);
        double getPercentile(double percent);

        void add(uint64_t microseconds, size_t requests_inc, size_t bytes_inc);
        void clear();
    };

    StatsCollector read_collector;
    StatsCollector write_collector;

    void addRead(uint64_t microseconds, size_t requests_inc, size_t bytes_inc);
    void addWrite(uint64_t microseconds, size_t requests_inc, size_t bytes_inc);

    void clear();

    void report(size_t concurrency);
    void writeJSON(DB::WriteBuffer & out, size_t concurrency, int64_t start_timestamp);
};


