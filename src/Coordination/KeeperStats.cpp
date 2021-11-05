#include <Coordination/KeeperStats.h>

namespace DB
{
UInt64 KeeperStats::getMinLatency() const
{
    std::shared_lock lock(mutex);
    return min_latency;
}

UInt64 KeeperStats::getMaxLatency() const
{
    std::shared_lock lock(mutex);
    return max_latency;
}

UInt64 KeeperStats::getAvgLatency() const
{
    std::shared_lock lock(mutex);
    if (count != 0)
    {
        return total_latency / count;
    }
    return 0;
}

UInt64 KeeperStats::getLastLatency() const
{
    std::shared_lock lock(mutex);
    return last_latency;
}

UInt64 KeeperStats::getPacketsReceived() const
{
    std::shared_lock lock(mutex);
    return packets_received;
}

UInt64 KeeperStats::getPacketsSent() const
{
    std::shared_lock lock(mutex);
    return packets_sent;
}

void KeeperStats::incrementPacketsReceived()
{
    std::unique_lock lock(mutex);
    packets_received++;
}

void KeeperStats::incrementPacketsSent()
{
    std::unique_lock lock(mutex);
    packets_sent++;
}

void KeeperStats::updateLatency(UInt64 latency_ms)
{
    std::unique_lock lock(mutex);

    last_latency = latency_ms;
    total_latency += (latency_ms);
    count++;

    if (latency_ms < min_latency)
    {
        min_latency = latency_ms;
    }

    if (latency_ms > max_latency)
    {
        max_latency = latency_ms;
    }
}

void KeeperStats::reset()
{
    std::unique_lock lock(mutex);
    resetLatency();
    resetRequestCounters();
}

void KeeperStats::resetLatency()
{
    total_latency = 0;
    count = 0;
    max_latency = 0;
    min_latency = 0;
}

void KeeperStats::resetRequestCounters()
{
    packets_received = 0;
    packets_sent = 0;
}

}
