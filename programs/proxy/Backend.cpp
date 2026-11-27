#include <Backend.h>

#include <ctime>

namespace DB::Proxy
{

void updateEWMA(std::atomic<double> & average, double sample, double alpha)
{
    double old_value = average.load(std::memory_order_relaxed);
    double new_value = old_value == 0.0 ? sample : old_value * (1.0 - alpha) + sample * alpha;
    average.store(new_value, std::memory_order_relaxed);
}

void RateCounter::hit(UInt64 count)
{
    Int64 now = ::time(nullptr);
    size_t slot = static_cast<size_t>(now) % WINDOW;
    Int64 slot_time = bucket_time[slot].load(std::memory_order_relaxed);
    if (slot_time != now)
    {
        /// A stale bucket from a previous wrap of the window: reset it for the current second.
        buckets[slot].store(0, std::memory_order_relaxed);
        bucket_time[slot].store(now, std::memory_order_relaxed);
    }
    buckets[slot].fetch_add(count, std::memory_order_relaxed);
}

double RateCounter::perSecond() const
{
    Int64 now = ::time(nullptr);
    UInt64 sum = 0;
    /// Skip the current (incomplete) second.
    for (Int64 t = now - static_cast<Int64>(RATE_SECONDS); t < now; ++t)
    {
        size_t slot = static_cast<size_t>(t) % WINDOW;
        if (bucket_time[slot].load(std::memory_order_relaxed) == t)
            sum += buckets[slot].load(std::memory_order_relaxed);
    }
    return static_cast<double>(sum) / RATE_SECONDS;
}

Backend::Backend(BackendConfig config_)
    : cfg(std::move(config_))
{
}

Backend::Health & Backend::healthOf(UInt16 port)
{
    std::lock_guard lock(health_mutex);
    return health[port];
}

bool Backend::isAlive(UInt16 port) const
{
    std::lock_guard lock(health_mutex);
    for (const UInt16 key : {whole_backend, port})
    {
        const auto it = health.find(key);
        if (it != health.end() && !it->second.alive.load(std::memory_order_relaxed))
            return false;
    }
    return true;
}

bool Backend::isAlive() const
{
    std::lock_guard lock(health_mutex);
    for (const auto & [_, entry] : health)
        if (!entry.alive.load(std::memory_order_relaxed))
            return false;
    return true;
}

std::vector<UInt16> Backend::portsMarkedDown() const
{
    std::vector<UInt16> res;
    std::lock_guard lock(health_mutex);
    for (const auto & [port, entry] : health)
        if (!entry.alive.load(std::memory_order_relaxed))
            res.push_back(port);
    return res;
}

void Backend::reportCheckSuccess(UInt16 port, double latency_ms)
{
    Health & entry = healthOf(port);
    entry.consecutive_failures.store(0, std::memory_order_relaxed);
    entry.alive.store(true, std::memory_order_relaxed);
    updateEWMA(check_latency_ms, latency_ms);
}

void Backend::reportCheckFailure(UInt16 port, UInt32 failures_to_mark_down)
{
    Health & entry = healthOf(port);
    if (entry.consecutive_failures.fetch_add(1, std::memory_order_relaxed) + 1 >= failures_to_mark_down)
        entry.alive.store(false, std::memory_order_relaxed);
}

void Backend::reportConnectSuccess(UInt16 port, double latency_ms)
{
    Health & entry = healthOf(port);
    entry.consecutive_failures.store(0, std::memory_order_relaxed);
    entry.alive.store(true, std::memory_order_relaxed);
    updateEWMA(connect_latency_ms, latency_ms);
}

void Backend::reportConnectFailure(UInt16 port, UInt32 failures_to_mark_down)
{
    total_errors.fetch_add(1, std::memory_order_relaxed);
    Health & entry = healthOf(port);
    if (entry.consecutive_failures.fetch_add(1, std::memory_order_relaxed) + 1 >= failures_to_mark_down)
        entry.alive.store(false, std::memory_order_relaxed);
}

void Backend::onConnectionStart()
{
    active_connections.fetch_add(1, std::memory_order_relaxed);
    total_connections.fetch_add(1, std::memory_order_relaxed);
    connection_rate.hit();
}

void Backend::onConnectionEnd()
{
    active_connections.fetch_sub(1, std::memory_order_relaxed);
}

void Backend::setResourceUsage(double cpu_cores, double memory_bytes)
{
    cpu_usage.store(cpu_cores, std::memory_order_relaxed);
    memory_usage.store(memory_bytes, std::memory_order_relaxed);
}

}
