#pragma once

#include <ProxyConfig.h>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace DB::Proxy
{

/// Sliding window counter of events for computing a per-second rate.
class RateCounter
{
public:
    void hit(UInt64 count = 1);

    /// Average rate over the last few complete seconds.
    double perSecond() const;

private:
    static constexpr size_t WINDOW = 16;
    static constexpr size_t RATE_SECONDS = 5;

    mutable std::atomic<Int64> bucket_time[WINDOW] {};
    mutable std::atomic<UInt64> buckets[WINDOW] {};
};

/// A single upstream server together with its health and usage statistics.
/// The statistics are updated from many fibers concurrently and are approximate by design.
class Backend
{
public:
    explicit Backend(BackendConfig config_);

    const BackendConfig & config() const { return cfg; }
    const String & name() const { return cfg.name; }

    /// Health, actively updated by the health checker and passively by connection errors.
    /// A backend serves each protocol on its own port, and one of them can be down while the others work,
    /// so the health is tracked per port of the backend: a failure to connect to `http_port` does not take
    /// the backend out of rotation for the native protocol. The pseudo-port `whole_backend` holds the result
    /// of probing the explicitly configured `health_check_port`, which stands for the backend as a whole.

    static constexpr UInt16 whole_backend = 0;

    /// Whether connections to this port of the backend can be routed: neither the port nor the whole backend is marked down.
    [[nodiscard]] bool isAlive(UInt16 port) const;
    /// Whether nothing of the backend is marked down.
    [[nodiscard]] bool isAlive() const;
    /// The ports that are marked down, for the status page. Contains `whole_backend` if the backend as a whole is.
    [[nodiscard]] std::vector<UInt16> portsMarkedDown() const;

    void reportCheckSuccess(UInt16 port, double latency_ms);
    void reportCheckFailure(UInt16 port, UInt32 failures_to_mark_down);
    void reportConnectSuccess(UInt16 port, double latency_ms);
    void reportConnectFailure(UInt16 port, UInt32 failures_to_mark_down);
    void reportError() { total_errors.fetch_add(1, std::memory_order_relaxed); }

    /// Connection accounting. Used by the least_connections strategy and for draining.

    void onConnectionStart();
    void onConnectionEnd();
    Int64 activeConnections() const { return active_connections.load(std::memory_order_relaxed); }

    void addBytesFromClient(UInt64 bytes) { bytes_from_client.fetch_add(bytes, std::memory_order_relaxed); }
    void addBytesToClient(UInt64 bytes) { bytes_to_client.fetch_add(bytes, std::memory_order_relaxed); }

    /// Resource usage polled from the backend. Negative means unknown.

    void setResourceUsage(double cpu_cores, double memory_bytes);
    double cpuUsage() const { return cpu_usage.load(std::memory_order_relaxed); }
    double memoryUsage() const { return memory_usage.load(std::memory_order_relaxed); }

    double connectLatencyMs() const { return connect_latency_ms.load(std::memory_order_relaxed); }
    double checkLatencyMs() const { return check_latency_ms.load(std::memory_order_relaxed); }
    UInt64 totalConnections() const { return total_connections.load(std::memory_order_relaxed); }
    UInt64 totalErrors() const { return total_errors.load(std::memory_order_relaxed); }
    UInt64 bytesFromClient() const { return bytes_from_client.load(std::memory_order_relaxed); }
    UInt64 bytesToClient() const { return bytes_to_client.load(std::memory_order_relaxed); }
    double connectionsPerSecond() const { return connection_rate.perSecond(); }

private:
    const BackendConfig cfg;

    struct Health
    {
        std::atomic<bool> alive {true};
        std::atomic<UInt32> consecutive_failures {0};
    };

    /// Keyed by the port. An entry is created by the first report for the port and is never removed
    /// (the set of ports is bounded by the configuration), so a reference to it stays valid without the lock.
    mutable std::mutex health_mutex;
    std::map<UInt16, Health> health;

    Health & healthOf(UInt16 port);

    std::atomic<Int64> active_connections {0};
    std::atomic<UInt64> total_connections {0};
    std::atomic<UInt64> total_errors {0};
    std::atomic<UInt64> bytes_from_client {0};
    std::atomic<UInt64> bytes_to_client {0};

    std::atomic<double> connect_latency_ms {0};
    std::atomic<double> check_latency_ms {0};
    std::atomic<double> cpu_usage {-1};
    std::atomic<double> memory_usage {-1};

    RateCounter connection_rate;
};

using BackendPtr = std::shared_ptr<Backend>;

/// Exponentially weighted moving average without locking. Losing a concurrent update is acceptable.
void updateEWMA(std::atomic<double> & average, double sample, double alpha = 0.2);

}
