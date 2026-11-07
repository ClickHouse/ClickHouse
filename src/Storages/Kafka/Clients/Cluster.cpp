#include <Storages/Kafka/Clients/Cluster.h>

#include <Storages/Kafka/Clients/Net/TCPTransport.h>
#include <Storages/Kafka/Clients/Net/TLSTransport.h>
#include <Storages/Kafka/Clients/Protocol/BrokerError.h>
#include <Storages/Kafka/Clients/Protocol/Messages/FindCoordinator.h>
#include <Storages/Kafka/Clients/SaslAuthentication.h>

#include "config.h"


#include <Common/logger_useful.h>

#include <algorithm>

#include <poll.h>

namespace Kafka
{

BrokerAddress parseBrokerAddress(const String & entry)
{
    const auto colon = entry.rfind(':');
    if (colon == String::npos)
        return {entry, 9092};
    const String host = entry.substr(0, colon);
    const String port_str = entry.substr(colon + 1);
    int port = 0;
    try
    {
        port = std::stoi(port_str);
    }
    catch (...)
    {
        throw Exception(ErrorKind::Config, "Invalid broker address '{}'", entry);
    }
    if (host.empty() || port <= 0 || port > 65535)
        throw Exception(ErrorKind::Config, "Invalid broker address '{}'", entry);
    return {host, static_cast<UInt16>(port)};
}

Cluster::Cluster(ClientConfig config_, TransportFactory factory_)
    : config(std::move(config_))
    , wakeup_source(std::make_shared<Net::WakeupSource>())
    , factory(std::move(factory_))
    , log(config.log)
{
    if (config.bootstrap_brokers.empty())
        throw Exception(ErrorKind::Config, "The bootstrap_brokers list must not be empty");
    for (const auto & entry : config.bootstrap_brokers)
        bootstrap.push_back(parseBrokerAddress(entry));

    /// Fail close on inconsistent security settings instead of silently
    /// downgrading (an ignored credential or TLS block hides a misconfiguration).
    if (usesSasl(config.security_protocol) && !config.sasl)
        throw Exception(
            ErrorKind::Config, "The security protocol '{}' requires SASL credentials", securityProtocolName(config.security_protocol));
    if (!usesSasl(config.security_protocol) && config.sasl)
        throw Exception(
            ErrorKind::Config,
            "SASL credentials are set, but the security protocol '{}' does not use SASL",
            securityProtocolName(config.security_protocol));
    /// Catch mechanisms this build cannot speak (SCRAM without SSL, GSSAPI
    /// without krb5) and incomplete credentials at construction, not at the
    /// first connect.
    if (config.sasl)
        checkSaslMechanismSupported(*config.sasl);
    if (!usesTls(config.security_protocol) && config.tls)
        throw Exception(
            ErrorKind::Config,
            "TLS settings are set, but the security protocol '{}' does not use TLS",
            securityProtocolName(config.security_protocol));
#if !USE_SSL
    if (usesTls(config.security_protocol))
        throw Exception(
            ErrorKind::NotSupported,
            "The security protocol '{}' requires a build with SSL support",
            securityProtocolName(config.security_protocol));
#endif

    if (!factory)
    {
        auto wakeup = std::shared_ptr<const Net::WakeupSource>(wakeup_source);
        if (usesTls(config.security_protocol))
        {
#if USE_SSL
            factory = [wakeup, tls = config.tls.value_or(TlsConfig{})]() -> std::unique_ptr<Net::Transport>
            { return std::make_unique<Net::TLSTransport>(wakeup, tls); };
#endif
        }
        else
        {
            factory = [wakeup]() -> std::unique_ptr<Net::Transport> { return std::make_unique<Net::TCPTransport>(wakeup); };
        }
    }
}

Cluster::~Cluster() = default;

std::unique_ptr<BrokerConnection> Cluster::makeConnection() const
{
    ConnectionOptions options;
    options.client_id = config.client_id;
    options.sasl = config.sasl;
    options.log = config.log;
    return std::make_unique<BrokerConnection>(factory(), std::move(options));
}

// -- Lease ---------------------------------------------------------------------

Cluster::Lease::Lease(Cluster & cluster_, Int32 node_id_, std::unique_ptr<BrokerConnection> connection_, bool pooled_)
    : cluster(&cluster_)
    , node_id(node_id_)
    , connection(std::move(connection_))
    , pooled(pooled_)
{
}

Cluster::Lease::Lease(Lease && other) noexcept
    : cluster(other.cluster)
    , node_id(other.node_id)
    , connection(std::move(other.connection))
    , pooled(other.pooled)
{
    other.cluster = nullptr;
}

Cluster::Lease::~Lease()
{
    if (cluster && connection)
        cluster->releaseConnection(node_id, std::move(connection), pooled);
}

void Cluster::releaseConnection(Int32 node_id, std::unique_ptr<BrokerConnection> connection, bool pooled)
{
    std::lock_guard lock(pools_mutex);
    auto & pool = pools[node_id];
    if (!pooled)
        return; // ephemeral: drop silently
    /// A connection with pipelined responses still in flight must never serve
    /// another caller — drop it with the socket. The same for connections whose
    /// SASL session is about to expire (KIP-368): the broker will kill them.
    if (connection->isOpen() && !connection->isPoisoned() && connection->getOutstanding() == 0 && !connection->isAuthSessionExpiring())
        pool.idle.push_back(std::move(connection));
    else
        --pool.total;
}

Cluster::Lease Cluster::acquire(Int32 node_id, const BrokerAddress & address, const Net::Deadline & deadline)
{
    bool pooled = false;
    {
        std::lock_guard lock(pools_mutex);
        auto & pool = pools[node_id];
        while (!pool.idle.empty())
        {
            auto connection = std::move(pool.idle.back());
            pool.idle.pop_back();
            /// Drop pooled connections whose SASL session is about to expire (KIP-368).
            if (connection->isAuthSessionExpiring())
            {
                --pool.total;
                continue;
            }
            return Lease(*this, node_id, std::move(connection), true);
        }
        if (pool.total < config.max_connections_per_broker)
        {
            ++pool.total;
            pooled = true;
        }
    }
    auto connection = makeConnection();
    try
    {
        connection->open(address.host, address.port, deadline);
    }
    catch (...)
    {
        if (pooled)
        {
            std::lock_guard lock(pools_mutex);
            --pools[node_id].total;
        }
        throw;
    }
    return Lease(*this, node_id, std::move(connection), pooled);
}

std::vector<std::pair<Int32, BrokerAddress>> Cluster::controlCandidates()
{
    std::vector<std::pair<Int32, BrokerAddress>> candidates;
    {
        std::lock_guard lock(cache_mutex);
        for (const auto & [node_id, broker] : brokers)
            candidates.emplace_back(node_id, BrokerAddress{broker.host, static_cast<UInt16>(broker.port)});
    }
    if (candidates.empty())
    {
        /// Bootstrap addresses get stable pseudo node ids below -1000 so they
        /// pool separately from real brokers.
        for (size_t i = 0; i < bootstrap.size(); ++i)
            candidates.emplace_back(static_cast<Int32>(-1000 - i), bootstrap[i]);
    }
    size_t start = 0;
    {
        std::lock_guard lock(pools_mutex);
        start = control_round_robin++;
    }
    std::rotate(candidates.begin(), candidates.begin() + static_cast<ptrdiff_t>(start % candidates.size()), candidates.end());
    return candidates;
}

Cluster::Lease Cluster::acquireControl(const Net::Deadline & deadline)
{
    std::optional<Exception> last_error;
    for (const auto & [node_id, address] : controlCandidates())
    {
        try
        {
            return acquire(node_id, address, deadline);
        }
        catch (const Exception & e)
        {
            if (e.getKind() == ErrorKind::Wakeup || e.getKind() == ErrorKind::Timeout)
                throw;
            last_error = e;
            LOG_DEBUG(log, "Control connection to {}:{} failed: {}", address.host, address.port, e.message());
        }
    }
    if (last_error)
        throw *last_error;
    throw Exception(ErrorKind::Connection, "No brokers reachable");
}

// -- Metadata cache --------------------------------------------------------------

void Cluster::refreshMetadata(
    const std::vector<String> & topics_, bool allow_auto_create, bool tolerate_topic_errors, const Net::Deadline & deadline)
{
    Protocol::MetadataRequest request;
    request.topics.emplace();
    for (const auto & topic : topics_)
    {
        Protocol::MetadataRequestTopic entry;
        entry.name = topic;
        request.topics->push_back(std::move(entry));
    }
    request.allow_auto_topic_creation = allow_auto_create;

    auto lease = acquireControl(deadline);
    const auto response = lease->call(request, deadline);

    std::lock_guard lock(cache_mutex);
    brokers.clear();
    for (const auto & broker : response.brokers)
        brokers[broker.node_id] = BrokerMetadata{broker.node_id, broker.host, broker.port, broker.rack};
    cluster_id = response.cluster_id;
    controller_id = response.controller_id;

    const auto now = std::chrono::steady_clock::now();
    for (const auto & topic : response.topics)
    {
        if (!topic.name)
            continue;
        if (topic.error_code != BrokerError::NONE && topic.error_code != BrokerError::LEADER_NOT_AVAILABLE)
        {
            topics.erase(*topic.name);
            if (tolerate_topic_errors)
                continue;
            throw Exception(BrokerCode{topic.error_code}, "Metadata for topic '{}' failed", *topic.name);
        }
        topics[*topic.name] = CachedTopic{topic, now};
    }
    LOG_DEBUG(log, "Metadata refreshed: {} brokers, {} topics", response.brokers.size(), response.topics.size());
}

Protocol::MetadataTopic Cluster::topicMetadata(const String & topic, const Net::Deadline & deadline, bool force_refresh)
{
    if (!force_refresh)
    {
        std::lock_guard lock(cache_mutex);
        const auto it = topics.find(topic);
        if (it != topics.end())
        {
            const auto age = std::chrono::steady_clock::now() - it->second.fetched_at;
            if (age < config.metadata_max_age)
                return it->second.raw;
        }
    }
    refreshMetadata({topic}, false, false, deadline);
    std::lock_guard lock(cache_mutex);
    const auto it = topics.find(topic);
    if (it == topics.end())
        throw Exception(BrokerCode{BrokerError::UNKNOWN_TOPIC_OR_PARTITION}, "Topic '{}' missing from metadata response", topic);
    return it->second.raw;
}

std::optional<Protocol::MetadataTopic> Cluster::cachedTopic(const String & topic) const
{
    std::lock_guard lock(cache_mutex);
    const auto it = topics.find(topic);
    if (it == topics.end())
        return std::nullopt;
    return it->second.raw;
}

LeaderRef Cluster::leaderFor(const TopicPartition & tp, const Net::Deadline & deadline)
{
    for (int attempt = 0; attempt < 2; ++attempt)
    {
        const bool force = attempt > 0;
        const auto topic = topicMetadata(tp.topic, deadline, force);
        const Protocol::MetadataPartition * partition = nullptr;
        for (const auto & p : topic.partitions)
            if (p.partition_index == tp.partition)
                partition = &p;
        if (!partition)
        {
            if (!force)
                continue;
            throw Exception(BrokerCode{BrokerError::UNKNOWN_TOPIC_OR_PARTITION}, "Partition {} of topic '{}' does not exist",
                tp.partition,
                tp.topic);
        }
        if (partition->leader_id < 0)
        {
            if (!force)
                continue;
            throw Exception(BrokerCode{BrokerError::LEADER_NOT_AVAILABLE}, "No leader for {}[{}]", tp.topic, tp.partition);
        }
        std::lock_guard lock(cache_mutex);
        const auto broker = brokers.find(partition->leader_id);
        if (broker == brokers.end())
        {
            if (!force)
                continue;
            throw Exception(BrokerCode{BrokerError::BROKER_NOT_AVAILABLE}, "Leader node {} not in broker list",
                partition->leader_id);
        }
        LeaderRef ref;
        ref.node_id = partition->leader_id;
        ref.leader_epoch = partition->leader_epoch;
        ref.topic_id = topic.topic_id;
        ref.address = {broker->second.host, static_cast<UInt16>(broker->second.port)};
        return ref;
    }
    throw Exception(BrokerCode{BrokerError::LEADER_NOT_AVAILABLE}, "Leader resolution failed for {}", tp.topic);
}

void Cluster::invalidateTopic(const String & topic)
{
    std::lock_guard lock(cache_mutex);
    topics.erase(topic);
}

Cluster::Lease Cluster::acquireCoordinator(const String & group, const Net::Deadline & deadline)
{
    {
        std::lock_guard lock(cache_mutex);
        const auto it = coordinators.find(group);
        if (it != coordinators.end())
        {
            const auto [node_id, address] = it->second;
            return acquire(node_id, address, deadline);
        }
    }

    Protocol::FindCoordinatorRequest request;
    request.key_type = Protocol::coordinator_type_group;
    request.coordinator_keys = {group};
    Protocol::FindCoordinatorResponse response;
    {
        auto lease = acquireControl(deadline);
        response = lease->call(request, deadline);
    }
    const Protocol::Coordinator * coordinator = nullptr;
    for (const auto & c : response.coordinators)
        if (c.key == group)
            coordinator = &c;
    /// FindCoordinator v3 responses carry one coordinator without echoing the
    /// key; accept the single entry.
    if (!coordinator && response.coordinators.size() == 1 && response.coordinators.front().key.empty())
        coordinator = &response.coordinators.front();
    if (!coordinator)
        throw Exception(ErrorKind::Protocol, "FindCoordinator response misses group '{}'", group);
    if (coordinator->error_code != BrokerError::NONE)
        throw Exception(BrokerCode{coordinator->error_code}, "Coordinator lookup for group '{}' failed{}",
            group,
            coordinator->error_message ? ": " + *coordinator->error_message : String{});

    const BrokerAddress address{coordinator->host, static_cast<UInt16>(coordinator->port)};
    {
        std::lock_guard lock(cache_mutex);
        coordinators[group] = {coordinator->node_id, address};
    }
    LOG_DEBUG(log, "Coordinator for group '{}' is node {}", group, coordinator->node_id);
    return acquire(coordinator->node_id, address, deadline);
}

void Cluster::invalidateCoordinator(const String & group)
{
    std::lock_guard lock(cache_mutex);
    coordinators.erase(group);
}

Cluster::BrokersSnapshot Cluster::knownBrokers() const
{
    std::lock_guard lock(cache_mutex);
    BrokersSnapshot snapshot;
    for (const auto & [id, broker] : brokers)
        snapshot.brokers.push_back(broker);
    snapshot.cluster_id = cluster_id;
    snapshot.controller_id = controller_id;
    return snapshot;
}

void Cluster::backoff(std::chrono::milliseconds duration, const Net::Deadline & deadline) const
{
    const auto until = Net::Deadline::after(duration);
    const UInt64 start_generation = wakeup_source->getGeneration();
    for (;;)
    {
        if (wakeup_source->getGeneration() != start_generation)
        {
            wakeup_source->drain();
            throw Exception(ErrorKind::Wakeup, "Backoff interrupted by wakeup()");
        }
        if (deadline.expired())
            throw Exception(ErrorKind::Timeout, "Operation timed out during retry backoff");
        if (until.expired())
            return;
        pollfd fd{wakeup_source->getFd(), POLLIN, 0};
        const auto slice = std::min(until.remainingClamped(std::chrono::milliseconds{50}),
                                    deadline.remainingClamped(std::chrono::milliseconds{50}));
        ::poll(&fd, 1, static_cast<int>(slice.count()));
        if ((fd.revents & POLLIN) != 0)
            wakeup_source->drain();
    }
}

}
