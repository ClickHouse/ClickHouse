#include <Coordination/KeeperConnectionStats.h>

namespace DB
{

uint64_t KeeperConnectionStats::getMinLatency() const
{
    return min_latency;
}

uint64_t KeeperConnectionStats::getMaxLatency() const
{
    return max_latency;
}

uint64_t KeeperConnectionStats::getAvgLatency() const
{
    if (count != 0)
        return total_latency / count;
    return 0;
}

uint64_t KeeperConnectionStats::getLastLatency() const
{
    return last_latency;
}

uint64_t KeeperConnectionStats::getPacketsReceived() const
{
    return packets_received;
}

uint64_t KeeperConnectionStats::getPacketsSent() const
{
    return packets_sent;
}

void KeeperConnectionStats::incrementPacketsReceived()
{
    packets_received++;
}

void KeeperConnectionStats::incrementPacketsSent()
{
    packets_sent++;
}

void KeeperConnectionStats::updateLatency(uint64_t latency_ms)
{
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

void KeeperConnectionStats::reset()
{
    resetLatency();
    resetRequestCounters();
}

void KeeperConnectionStats::resetLatency()
{
    total_latency = 0;
    count = 0;
    max_latency = 0;
    min_latency = 0;
}

void KeeperConnectionStats::resetRequestCounters()
{
    packets_received = 0;
    packets_sent = 0;
}

}
