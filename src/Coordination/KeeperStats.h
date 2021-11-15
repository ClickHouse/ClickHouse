#pragma once

#include<base/types.h>
#include<mutex>
#include<shared_mutex>
#include <memory>

namespace DB
{
class KeeperStats;
using KeeperStatsPtr = std::shared_ptr<KeeperStats>;

/// Request statistics for connection or dispatcher
class KeeperStats
{
public:
    explicit KeeperStats() = default;

    UInt64 getMinLatency() const;
    UInt64 getMaxLatency() const;

    UInt64 getAvgLatency() const;
    UInt64 getLastLatency() const;

    UInt64 getPacketsReceived() const;
    UInt64 getPacketsSent() const;

    void incrementPacketsReceived();
    void incrementPacketsSent();

    void updateLatency(UInt64 latency_ms);
    void reset();

private:
    void inline resetLatency();
    void inline resetRequestCounters();

    mutable std::shared_mutex mutex;

    /// all response with watch response included
    UInt64 packets_sent = 0;
    /// All user requests
    UInt64 packets_received = 0;

    /// For consistent with zookeeper measured by millisecond,
    /// otherwise maybe microsecond is better
    UInt64 total_latency = 0;
    UInt64 max_latency = 0;
    UInt64 min_latency = 0;

    /// last operation latency
    UInt64 last_latency = 0;

    UInt64 count = 0;
};

}
