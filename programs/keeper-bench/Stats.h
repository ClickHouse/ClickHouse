#pragma once

#include <vector>
#include <map>

#include <AggregateFunctions/ReservoirSampler.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

#include <base/JSON.h>

struct Stats
{
    size_t errors = 0;

    using Sampler = ReservoirSampler<double>;
    /// All StatsCollector access must be protected by Runner::mutex
    struct StatsCollector
    {
        size_t requests = 0;
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

    /// Per-operation-type stats
    std::map<Coordination::OpNum, StatsCollector> op_collectors;

    void addRead(uint64_t microseconds, size_t requests_inc, size_t bytes_inc);
    void addWrite(uint64_t microseconds, size_t requests_inc, size_t bytes_inc);
    void addOp(Coordination::OpNum op_num, uint64_t microseconds, size_t requests_inc, size_t bytes_inc);

    void clear();

    void report(size_t concurrency);
    void writeJSON(DB::WriteBuffer & out, size_t concurrency, int64_t start_timestamp);
};
