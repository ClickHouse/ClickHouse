#include <atomic>
#include <Coordination/KeeperConnectionStats.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event KeeperPacketsSent;
    extern const Event KeeperPacketsReceived;
    extern const Event KeeperRequestTotal;
    extern const Event KeeperLatency;
}

namespace DB
{

uint64_t KeeperConnectionStats::getMinLatency() const
{
    return min_latency.load(std::memory_order_relaxed);
}

uint64_t KeeperConnectionStats::getMaxLatency() const
{
    return max_latency.load(std::memory_order_relaxed);
}

uint64_t KeeperConnectionStats::getAvgLatency() const
{
    auto cnt = count.load(std::memory_order_relaxed);
    if (cnt)
        return total_latency.load(std::memory_order_relaxed) / cnt;
    return 0;
}

uint64_t KeeperConnectionStats::getLastLatency() const
{
    return last_latency.load(std::memory_order_relaxed);
}

uint64_t KeeperConnectionStats::getPacketsReceived() const
{
    return packets_received.load(std::memory_order_relaxed);
}

uint64_t KeeperConnectionStats::getPacketsSent() const
{
    return packets_sent.load(std::memory_order_relaxed);
}

void KeeperConnectionStats::incrementPacketsReceived()
{
    packets_received.fetch_add(1, std::memory_order_relaxed);
    ProfileEvents::increment(ProfileEvents::KeeperPacketsReceived, 1);
}

void KeeperConnectionStats::incrementPacketsSent()
{
    packets_sent.fetch_add(1, std::memory_order_relaxed);
    ProfileEvents::increment(ProfileEvents::KeeperPacketsSent, 1);
}

void KeeperConnectionStats::updateLatency(uint64_t latency_ms)
{
    last_latency.store(latency_ms, std::memory_order_relaxed);
    total_latency.fetch_add(latency_ms, std::memory_order_relaxed);
    ProfileEvents::increment(ProfileEvents::KeeperLatency, latency_ms);
    count.fetch_add(1, std::memory_order_relaxed);
    ProfileEvents::increment(ProfileEvents::KeeperRequestTotal, 1);

    uint64_t prev_val = min_latency.load(std::memory_order_relaxed);
    while (prev_val > latency_ms && !min_latency.compare_exchange_weak(prev_val, latency_ms, std::memory_order_relaxed)) {}

    prev_val = max_latency.load(std::memory_order_relaxed);
    while (prev_val < latency_ms && !max_latency.compare_exchange_weak(prev_val, latency_ms, std::memory_order_relaxed)) {}
}

void KeeperConnectionStats::reset()
{
    resetLatency();
    resetRequestCounters();
}

void KeeperConnectionStats::resetLatency()
{
    total_latency.store(0, std::memory_order_relaxed);
    count.store(0, std::memory_order_relaxed);
    max_latency.store(0, std::memory_order_relaxed);
    min_latency.store(0, std::memory_order_relaxed);
    last_latency.store(0, std::memory_order_relaxed);
}

void KeeperConnectionStats::resetRequestCounters()
{
    packets_received.store(0, std::memory_order_relaxed);
    packets_sent.store(0, std::memory_order_relaxed);
}

}
