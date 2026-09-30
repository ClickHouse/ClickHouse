#pragma once

#include <atomic>
#include <base/types.h>
#include <memory>
#include <cstdint>

namespace DB
{

/// Request statistics for connection or dispatcher
class KeeperConnectionStats
{
public:
    KeeperConnectionStats()
    {
        reset();
    }

    uint64_t getMinLatency() const;
    uint64_t getMaxLatency() const;

    uint64_t getAvgLatency() const;
    uint64_t getLastLatency() const;

    uint64_t getPacketsReceived() const;
    uint64_t getPacketsSent() const;

    void incrementPacketsReceived();
    void incrementPacketsSent();

    void updateLatency(uint64_t latency_ms);
    void reset();

private:
    void resetLatency();
    void resetRequestCounters();

    /// all response with watch response included
    std::atomic_uint64_t packets_sent;
    /// All user requests
    std::atomic_uint64_t packets_received;

    /// For consistent with zookeeper measured by millisecond,
    /// otherwise maybe microsecond is better
    std::atomic_uint64_t total_latency;
    std::atomic_uint64_t max_latency;
    std::atomic_uint64_t min_latency;

    /// last operation latency
    std::atomic_uint64_t last_latency;

    std::atomic_uint64_t count;
};

}
