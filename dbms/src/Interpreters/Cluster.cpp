#include <Interpreters/Cluster.h>
#include <Common/escapeForFileName.h>
#include <Common/isLocalAddress.h>
#include <Common/SimpleCache.h>
#include <Common/StringUtils.h>
#include <IO/HexWriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>
#include <openssl/sha.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int LOGICAL_ERROR;
    extern const int SHARD_HAS_NO_CONNECTIONS;
}

namespace
{

/// Default shard weight.
static constexpr int default_weight = 1;

inline bool isLocal(const Cluster::Address & address)
{
    ///    If there is replica, for which:
    /// - its port is the same that the server is listening;
    /// - its host is resolved to set of addresses, one of which is the same as one of addresses of network interfaces of the server machine*;
    /// then we must go to this shard without any inter-process communication.
    ///
    /// * - this criteria is somewhat approximate.
    ///
    /// Also, replica is considered non-local, if it has default database set
    ///  (only reason is to avoid query rewrite).

    return address.default_database.empty() && isLocalAddress(address.resolved_address);
}

inline std::string addressToDirName(const Cluster::Address & address)
{
    return
        escapeForFileName(address.user) +
        (address.password.empty() ? "" : (':' + escapeForFileName(address.password))) + '@' +
        escapeForFileName(address.resolved_address.host().toString()) + ':' +
        std::to_string(address.resolved_address.port()) +
        (address.default_database.empty() ? "" : ('#' + escapeForFileName(address.default_database)));
}

/// To cache DNS requests.
Poco::Net::SocketAddress resolveSocketAddressImpl1(const String & host, UInt16 port)
{
    return Poco::Net::SocketAddress(host, port);
}

Poco::Net::SocketAddress resolveSocketAddressImpl2(const String & host_and_port)
{
    return Poco::Net::SocketAddress(host_and_port);
}

Poco::Net::SocketAddress resolveSocketAddress(const String & host, UInt16 port)
{
    static SimpleCache<decltype(resolveSocketAddressImpl1), &resolveSocketAddressImpl1> cache;
    return cache(host, port);
}

Poco::Net::SocketAddress resolveSocketAddress(const String & host_and_port)
{
    static SimpleCache<decltype(resolveSocketAddressImpl2), &resolveSocketAddressImpl2> cache;
    return cache(host_and_port);
}

}

/// Implementation of Cluster::Address class

Cluster::Address::Address(Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    host_name = config.getString(config_prefix + ".host");
    port = config.getInt(config_prefix + ".port");
    resolved_address = resolveSocketAddress(host_name, port);
    user = config.getString(config_prefix + ".user", "default");
    password = config.getString(config_prefix + ".password", "");
    default_database = config.getString(config_prefix + ".default_database", "");
}


Cluster::Address::Address(const String & host_port_, const String & user_, const String & password_)
    : user(user_), password(password_)
{
    UInt16 default_port = Poco::Util::Application::instance().config().getInt("tcp_port", 0);

    /// It's like that 'host_port_' string contains port. If condition is met, it doesn't necessarily mean that port exists (example: [::]).
    if ((nullptr != strchr(host_port_.c_str(), ':')) || !default_port)
    {
        resolved_address = resolveSocketAddress(host_port_);
        host_name = host_port_.substr(0, host_port_.find(':'));
        port = resolved_address.port();
    }
    else
    {
        resolved_address = resolveSocketAddress(host_port_, default_port);
        host_name = host_port_;
        port = default_port;
    }
}

String Cluster::Address::toString() const
{
    return host_name + ':' + DB::toString(port);
}

/// Implementation of Clusters class

Clusters::Clusters(Poco::Util::AbstractConfiguration & config, const Settings & settings, const String & config_name)
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_name, config_keys);

    for (const auto & key : config_keys)
        impl.emplace(key, std::make_shared<Cluster>(config, settings, config_name + "." + key));
}


ClusterPtr Clusters::getCluster(const std::string & cluster_name) const
{
    std::lock_guard<std::mutex> lock(mutex);

    auto it = impl.find(cluster_name);
    return (it != impl.end()) ? it->second : nullptr;
}


void Clusters::updateClusters(Poco::Util::AbstractConfiguration & config, const Settings & settings, const String & config_name)
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_name, config_keys);

    std::lock_guard<std::mutex> lock(mutex);

    for (const auto & key : config_keys)
    {
        auto it = impl.find(key);
        auto new_cluster = std::make_shared<Cluster>(config, settings, config_name + "." + key);

        if (it == impl.end())
            impl.emplace(key, std::move(new_cluster));
        else
        {
            //TODO: Check that cluster update is necessarily
            it->second = std::move(new_cluster);
        }
    }
}

Clusters::Impl Clusters::getContainer() const
{
    std::lock_guard<std::mutex> lock(mutex);
    /// The following line copies container of shared_ptrs to return value under lock
    return impl;
}

/// Implementation of `Cluster` class

Cluster::Cluster(Poco::Util::AbstractConfiguration & config, const Settings & settings, const String & cluster_name)
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(cluster_name, config_keys);

    if (config_keys.empty())
        throw Exception("No cluster elements (shard, node) specified in config at path " + cluster_name, ErrorCodes::SHARD_HAS_NO_CONNECTIONS);

    const auto & config_prefix = cluster_name + ".";

    UInt32 current_shard_num = 1;

    for (const auto & key : config_keys)
    {
        if (startsWith(key, "node"))
        {
            /// Shard without replicas.

            const auto & prefix = config_prefix + key;
            const auto weight = config.getInt(prefix + ".weight", default_weight);
            if (weight == 0)
                continue;

            addresses.emplace_back(config, prefix);
            addresses.back().replica_num = 1;
            const auto & address = addresses.back();

            ShardInfo info;
            info.shard_num = current_shard_num;
            info.weight = weight;

            if (isLocal(address))
                info.local_addresses.push_back(address);
            else
            {
                info.dir_names.push_back(addressToDirName(address));
                ConnectionPoolPtrs pools;
                pools.push_back(std::make_shared<ConnectionPool>(
                    settings.distributed_connections_pool_size,
                    address.host_name, address.port, address.resolved_address,
                    address.default_database, address.user, address.password,
                    "server", Protocol::Compression::Enable,
                    saturate(settings.connect_timeout, settings.limits.max_execution_time),
                    saturate(settings.receive_timeout, settings.limits.max_execution_time),
                    saturate(settings.send_timeout, settings.limits.max_execution_time)));

                info.pool = std::make_shared<ConnectionPoolWithFailover>(
                        std::move(pools), settings.load_balancing, settings.connections_with_failover_max_tries);
            }

            slot_to_shard.insert(std::end(slot_to_shard), weight, shards_info.size());
            shards_info.push_back(info);
        }
        else if (startsWith(key, "shard"))
        {
            /// Shard with replicas.

            Poco::Util::AbstractConfiguration::Keys replica_keys;
            config.keys(config_prefix + key, replica_keys);

            addresses_with_failover.emplace_back();
            Addresses & replica_addresses = addresses_with_failover.back();
            UInt32 current_replica_num = 1;

            const auto & partial_prefix = config_prefix + key + ".";
            const auto weight = config.getInt(partial_prefix + ".weight", default_weight);
            if (weight == 0)
                continue;

            const auto internal_replication = config.getBool(partial_prefix + ".internal_replication", false);

            /** in case of internal_replication we will be appending names to
             *  the first element of vector; otherwise we will just .emplace_back
             */
            std::vector<std::string> dir_names{};

            auto first = true;
            for (const auto & replica_key : replica_keys)
            {
                if (startsWith(replica_key, "weight") || startsWith(replica_key, "internal_replication"))
                    continue;

                if (startsWith(replica_key, "replica"))
                {
                    replica_addresses.emplace_back(config, partial_prefix + replica_key);
                    replica_addresses.back().replica_num = current_replica_num;
                    ++current_replica_num;

                    if (!isLocal(replica_addresses.back()))
                    {
                        if (internal_replication)
                        {
                            auto dir_name = addressToDirName(replica_addresses.back());
                            if (first)
                                dir_names.emplace_back(std::move(dir_name));
                            else
                                dir_names.front() += "," + dir_name;
                        }
                        else
                            dir_names.emplace_back(addressToDirName(replica_addresses.back()));

                        if (first) first = false;
                    }
                }
                else
                    throw Exception("Unknown element in config: " + replica_key, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
            }

            Addresses shard_local_addresses;

            ConnectionPoolPtrs replicas;
            replicas.reserve(replica_addresses.size());

            for (const auto & replica : replica_addresses)
            {
                if (isLocal(replica))
                    shard_local_addresses.push_back(replica);
                else
                {
                    replicas.emplace_back(std::make_shared<ConnectionPool>(
                        settings.distributed_connections_pool_size,
                        replica.host_name, replica.port, replica.resolved_address,
                        replica.default_database, replica.user, replica.password,
                        "server", Protocol::Compression::Enable,
                        saturate(settings.connect_timeout_with_failover_ms, settings.limits.max_execution_time),
                        saturate(settings.receive_timeout, settings.limits.max_execution_time),
                        saturate(settings.send_timeout, settings.limits.max_execution_time)));
                }
            }

            ConnectionPoolWithFailoverPtr shard_pool;
            if (!replicas.empty())
                shard_pool = std::make_shared<ConnectionPoolWithFailover>(
                        std::move(replicas), settings.load_balancing, settings.connections_with_failover_max_tries);

            slot_to_shard.insert(std::end(slot_to_shard), weight, shards_info.size());
            shards_info.push_back({std::move(dir_names), current_shard_num, weight, shard_local_addresses, shard_pool});
        }
        else
            throw Exception("Unknown element in config: " + key, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

        if (!addresses_with_failover.empty() && !addresses.empty())
            throw Exception("There must be either 'node' or 'shard' elements in config", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        ++current_shard_num;
    }

    initMisc();
}


Cluster::Cluster(const Settings & settings, const std::vector<std::vector<String>> & names,
                 const String & username, const String & password)
{
    UInt32 current_shard_num = 1;

    for (const auto & shard : names)
    {
        Addresses current;
        for (auto & replica : shard)
            current.emplace_back(replica, username, password);

        addresses_with_failover.emplace_back(current);

        ConnectionPoolPtrs replicas;
        replicas.reserve(current.size());

        for (const auto & replica : current)
        {
            replicas.emplace_back(std::make_shared<ConnectionPool>(
                settings.distributed_connections_pool_size,
                replica.host_name, replica.port, replica.resolved_address,
                replica.default_database, replica.user, replica.password,
                "server", Protocol::Compression::Enable,
                saturate(settings.connect_timeout_with_failover_ms, settings.limits.max_execution_time),
                saturate(settings.receive_timeout, settings.limits.max_execution_time),
                saturate(settings.send_timeout, settings.limits.max_execution_time)));
        }

        ConnectionPoolWithFailoverPtr shard_pool = std::make_shared<ConnectionPoolWithFailover>(
                std::move(replicas), settings.load_balancing, settings.connections_with_failover_max_tries);

        slot_to_shard.insert(std::end(slot_to_shard), default_weight, shards_info.size());
        shards_info.push_back({{}, current_shard_num, default_weight, {}, shard_pool});
        ++current_shard_num;
    }

    initMisc();
}


Poco::Timespan Cluster::saturate(const Poco::Timespan & v, const Poco::Timespan & limit)
{
    if (limit.totalMicroseconds() == 0)
        return v;
    else
        return (v > limit) ? limit : v;
}


void Cluster::initMisc()
{
    for (const auto & shard_info : shards_info)
    {
        if (!shard_info.isLocal() && !shard_info.hasRemoteConnections())
            throw Exception("Found shard without any specified connection",
                ErrorCodes::SHARD_HAS_NO_CONNECTIONS);
    }

    for (const auto & shard_info : shards_info)
    {
        if (shard_info.isLocal())
            ++local_shard_count;
        else
            ++remote_shard_count;
    }

    for (auto & shard_info : shards_info)
    {
        if (!shard_info.isLocal())
        {
            any_remote_shard_info = &shard_info;
            break;
        }
    }

    calculateHashOfAddresses();
}


void Cluster::calculateHashOfAddresses()
{
    std::vector<std::string> elements;

    if (!addresses.empty())
    {
        for (const auto & address : addresses)
            elements.push_back(address.host_name + ":" + toString(address.port));
    }
    else if (!addresses_with_failover.empty())
    {
        for (const auto & addresses : addresses_with_failover)
        {
            for (const auto & address : addresses)
                elements.push_back(address.host_name + ":" + toString(address.port));
        }
    }
    else
        throw Exception("Cluster: ill-formed cluster", ErrorCodes::LOGICAL_ERROR);

    std::sort(elements.begin(), elements.end());

    unsigned char hash[SHA512_DIGEST_LENGTH];

    SHA512_CTX ctx;
    SHA512_Init(&ctx);

    for (const auto & host : elements)
        SHA512_Update(&ctx, reinterpret_cast<const void *>(host.c_str()), host.size() + 1);

    SHA512_Final(hash, &ctx);

    {
        WriteBufferFromString buf(hash_of_addresses);
        HexWriteBuffer hex_buf(buf);
        hex_buf.write(reinterpret_cast<const char *>(hash), sizeof(hash));
    }
}


std::unique_ptr<Cluster> Cluster::getClusterWithSingleShard(size_t index) const
{
    return std::unique_ptr<Cluster>{ new Cluster(*this, index) };
}

Cluster::Cluster(const Cluster & from, size_t index)
    : shards_info{from.shards_info[index]}
{
    if (!from.addresses.empty())
        addresses.emplace_back(from.addresses[index]);
    if (!from.addresses_with_failover.empty())
        addresses_with_failover.emplace_back(from.addresses_with_failover[index]);

    initMisc();
}

}
