#pragma once

/// Cluster: bootstrap, cached metadata (topics -> partitions -> leader, topic
/// UUIDs, broker addresses), per-broker connection pools, coordinator cache.
/// Thread-safe; connections are checked out per operation so parallel calls
/// from different threads do not serialize on one socket.

#include <Storages/Kafka/Clients/BrokerConnection.h>
#include <Storages/Kafka/Clients/Client.h>
#include <Storages/Kafka/Clients/Config.h>
#include <Storages/Kafka/Clients/Net/Transport.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Metadata.h>

#include <base/types.h>

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace Kafka
{

struct BrokerAddress
{
    String host;
    UInt16 port = 0;
};

struct LeaderRef
{
    Int32 node_id = -1;
    Int32 leader_epoch = -1;
    Uuid topic_id;
    BrokerAddress address;
};

using TransportFactory = std::function<std::unique_ptr<Net::Transport>()>;

class Cluster
{
public:
    /// `factory_` defaults to TCPTransport wired to the shared WakeupSource;
    /// tests inject ScriptedTransport factories.
    explicit Cluster(ClientConfig config_, TransportFactory factory_ = {});
    ~Cluster();

    void wakeup() { wakeup_source->wakeup(); }
    const ClientConfig & getConfig() const { return config; }

    /// RAII connection checkout. Returns the connection to the pool on
    /// destruction when it is still healthy, drops it otherwise.
    class Lease
    {
    public:
        Lease(Cluster & cluster_, Int32 node_id_, std::unique_ptr<BrokerConnection> connection_, bool pooled_);
        ~Lease();
        Lease(Lease && other) noexcept;
        Lease & operator=(Lease &&) = delete;
        Lease(const Lease &) = delete;
        Lease & operator=(const Lease &) = delete;

        BrokerConnection & operator*() { return *connection; }
        BrokerConnection * operator->() { return connection.get(); }

    private:
        Cluster * cluster;
        Int32 node_id;
        std::unique_ptr<BrokerConnection> connection;
        bool pooled;
    };

    /// Connection to a specific broker.
    Lease acquire(Int32 node_id, const BrokerAddress & address, const Net::Deadline & deadline);
    /// Connection to any broker (round-robins known brokers, falls back to the
    /// bootstrap list) — used for metadata and coordinator lookups.
    Lease acquireControl(const Net::Deadline & deadline);

    /// Fetch metadata for `topics` from any broker and merge it into the
    /// cache. With tolerate_topic_errors=false a non-zero topic error (except
    /// LEADER_NOT_AVAILABLE, which accompanies in-flight auto-creation) throws.
    void refreshMetadata(
        const std::vector<String> & topics_,
        bool allow_auto_create,
        bool tolerate_topic_errors,
        const Net::Deadline & deadline);

    /// Cached topic metadata; refreshes when missing or older than
    /// metadata_max_age. The returned snapshot is a copy (no locks held).
    Protocol::MetadataTopic topicMetadata(const String & topic, const Net::Deadline & deadline, bool force_refresh = false);

    /// Cache probe without any wire activity.
    std::optional<Protocol::MetadataTopic> cachedTopic(const String & topic) const;

    /// Leader routing for one partition, refreshing stale/unknown entries once.
    LeaderRef leaderFor(const TopicPartition & tp, const Net::Deadline & deadline);

    /// Drop the cached entry so the next use refreshes (stale-leader errors).
    void invalidateTopic(const String & topic);

    /// Connection to the group coordinator, resolving and caching it via
    /// FindCoordinator on first use.
    Lease acquireCoordinator(const String & group, const Net::Deadline & deadline);
    /// Drop the cached coordinator (NOT_COORDINATOR / COORDINATOR_NOT_AVAILABLE).
    void invalidateCoordinator(const String & group);

    struct BrokersSnapshot
    {
        std::vector<BrokerMetadata> brokers;
        std::optional<String> cluster_id;
        Int32 controller_id = -1;
    };
    BrokersSnapshot knownBrokers() const;

    /// Deadline-aware retry backoff that honours wakeup().
    void backoff(std::chrono::milliseconds duration, const Net::Deadline & deadline) const;

    const LoggerPtr & getLogger() const { return log; }

private:
    struct CachedTopic
    {
        Protocol::MetadataTopic raw;
        std::chrono::steady_clock::time_point fetched_at;
    };

    struct Pool
    {
        std::vector<std::unique_ptr<BrokerConnection>> idle;
        size_t total = 0;
    };

    std::unique_ptr<BrokerConnection> makeConnection() const;
    void releaseConnection(Int32 node_id, std::unique_ptr<BrokerConnection> connection, bool pooled);
    std::vector<std::pair<Int32, BrokerAddress>> controlCandidates();

    ClientConfig config;
    std::vector<BrokerAddress> bootstrap;
    std::shared_ptr<Net::WakeupSource> wakeup_source;
    TransportFactory factory;
    LoggerPtr log;

    mutable std::mutex cache_mutex;
    std::unordered_map<String, CachedTopic> topics;
    std::unordered_map<String, std::pair<Int32, BrokerAddress>> coordinators;
    std::map<Int32, BrokerMetadata> brokers;
    std::optional<String> cluster_id;
    Int32 controller_id = -1;

    mutable std::mutex pools_mutex;
    std::map<Int32, Pool> pools;
    size_t control_round_robin = 0;

    friend class Lease;
};

/// Parse "host:port" (port optional, default 9092).
BrokerAddress parseBrokerAddress(const String & entry);

}
