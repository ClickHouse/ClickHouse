#include <Interpreters/Cluster.h>
#include <common/SimpleCache.h>
#include <Common/DNSResolver.h>
#include <Common/escapeForFileName.h>
#include <Common/isLocalAddress.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/parseAddress.h>
#include <IO/HexWriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int LOGICAL_ERROR;
    extern const int SHARD_HAS_NO_CONNECTIONS;
    extern const int SYNTAX_ERROR;
}

namespace
{

/// Default shard weight.
static constexpr UInt32 default_weight = 1;

inline bool isLocalImpl(const Cluster::Address & address, const Poco::Net::SocketAddress & resolved_address, UInt16 clickhouse_port)
{
    /// If there is replica, for which:
    /// - its port is the same that the server is listening;
    /// - its host is resolved to set of addresses, one of which is the same as one of addresses of network interfaces of the server machine*;
    /// then we must go to this shard without any inter-process communication.
    ///
    /// * - this criteria is somewhat approximate.
    ///
    /// Also, replica is considered non-local, if it has default database set
    ///  (only reason is to avoid query rewrite).

    return address.default_database.empty() && isLocalAddress(resolved_address, clickhouse_port);
}

}

/// Implementation of Cluster::Address class

std::optional<Poco::Net::SocketAddress> Cluster::Address::getResolvedAddress() const
{
    try
    {
        return DNSResolver::instance().resolveAddress(host_name, port);
    }
    catch (...)
    {
        /// Failure in DNS resolution in cluster initialization is Ok.
        tryLogCurrentException("Cluster");
        return {};
    }
}


bool Cluster::Address::isLocal(UInt16 clickhouse_port) const
{
    if (auto resolved = getResolvedAddress())
        return isLocalImpl(*this, *resolved, clickhouse_port);
    return false;
}


Cluster::Address::Address(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    host_name = config.getString(config_prefix + ".host");
    port = static_cast<UInt16>(config.getInt(config_prefix + ".port"));
    if (config.has(config_prefix + ".user"))
        user_specified = true;

    user = config.getString(config_prefix + ".user", "default");
    password = config.getString(config_prefix + ".password", "");
    default_database = config.getString(config_prefix + ".default_database", "");
    secure = config.getBool(config_prefix + ".secure", false) ? Protocol::Secure::Enable : Protocol::Secure::Disable;
    compression = config.getBool(config_prefix + ".compression", true) ? Protocol::Compression::Enable : Protocol::Compression::Disable;
    is_local = isLocal(config.getInt("tcp_port", 0));
}


Cluster::Address::Address(const String & host_port_, const String & user_, const String & password_, UInt16 clickhouse_port, bool secure_)
    : user(user_), password(password_)
{
    auto parsed_host_port = parseAddress(host_port_, clickhouse_port);
    host_name = parsed_host_port.first;
    port = parsed_host_port.second;
    secure = secure_ ? Protocol::Secure::Enable : Protocol::Secure::Disable;
    is_local = isLocal(clickhouse_port);
}


String Cluster::Address::toString() const
{
    return toString(host_name, port);
}

String Cluster::Address::toString(const String & host_name, UInt16 port)
{
    return escapeForFileName(host_name) + ':' + DB::toString(port);
}

String Cluster::Address::readableString() const
{
    String res;

    /// If it looks like IPv6 address add braces to avoid ambiguity in ipv6_host:port notation
    if (host_name.find_first_of(':') != std::string::npos && !host_name.empty() && host_name.back() != ']')
        res += '[' + host_name + ']';
    else
        res += host_name;

    res += ':' + DB::toString(port);
    return res;
}

std::pair<String, UInt16> Cluster::Address::fromString(const String & host_port_string)
{
    auto pos = host_port_string.find_last_of(':');
    if (pos == std::string::npos)
        throw Exception("Incorrect <host>:<port> format " + host_port_string, ErrorCodes::SYNTAX_ERROR);

    return {unescapeForFileName(host_port_string.substr(0, pos)), parse<UInt16>(host_port_string.substr(pos + 1))};
}


String Cluster::Address::toFullString() const
{
    return
        escapeForFileName(user) +
        (password.empty() ? "" : (':' + escapeForFileName(password))) + '@' +
        escapeForFileName(host_name) + ':' +
        std::to_string(port) +
        (default_database.empty() ? "" : ('#' + escapeForFileName(default_database)))
        + ((secure == Protocol::Secure::Enable) ? "+secure" : "");
}

Cluster::Address Cluster::Address::fromFullString(const String & full_string)
{
    const char * address_begin = full_string.data();
    const char * address_end = address_begin + full_string.size();

    Protocol::Secure secure = Protocol::Secure::Disable;
    const char * secure_tag = "+secure";
    if (endsWith(full_string, secure_tag))
    {
        address_end -= strlen(secure_tag);
        secure = Protocol::Secure::Enable;
    }

    const char * user_pw_end = strchr(full_string.data(), '@');
    const char * colon = strchr(full_string.data(), ':');
    if (!user_pw_end || !colon)
        throw Exception("Incorrect user[:password]@host:port#default_database format " + full_string, ErrorCodes::SYNTAX_ERROR);

    const bool has_pw = colon < user_pw_end;
    const char * host_end = has_pw ? strchr(user_pw_end + 1, ':') : colon;
    if (!host_end)
        throw Exception("Incorrect address '" + full_string + "', it does not contain port", ErrorCodes::SYNTAX_ERROR);

    const char * has_db = strchr(full_string.data(), '#');
    const char * port_end = has_db ? has_db : address_end;

    Address address;
    address.secure = secure;
    address.port = parse<UInt16>(host_end + 1, port_end - (host_end + 1));
    address.host_name = unescapeForFileName(std::string(user_pw_end + 1, host_end));
    address.user = unescapeForFileName(std::string(address_begin, has_pw ? colon : user_pw_end));
    address.password = has_pw ? unescapeForFileName(std::string(colon + 1, user_pw_end)) : std::string();
    address.default_database = has_db ? unescapeForFileName(std::string(has_db + 1, address_end)) : std::string();
    return address;
}


/// Implementation of Clusters class

Clusters::Clusters(const Poco::Util::AbstractConfiguration & config, const Settings & settings, const String & config_name)
{
    updateClusters(config, settings, config_name);
}


ClusterPtr Clusters::getCluster(const std::string & cluster_name) const
{
    std::lock_guard lock(mutex);

    auto it = impl.find(cluster_name);
    return (it != impl.end()) ? it->second : nullptr;
}


void Clusters::setCluster(const String & cluster_name, const std::shared_ptr<Cluster> & cluster)
{
    std::lock_guard lock(mutex);
    impl[cluster_name] = cluster;
}


void Clusters::updateClusters(const Poco::Util::AbstractConfiguration & config, const Settings & settings, const String & config_name)
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_name, config_keys);

    std::lock_guard lock(mutex);

    impl.clear();
    for (const auto & key : config_keys)
    {
        if (key.find('.') != String::npos)
            throw Exception("Cluster names with dots are not supported: '" + key + "'", ErrorCodes::SYNTAX_ERROR);

        impl.emplace(key, std::make_shared<Cluster>(config, settings, config_name + "." + key));
    }
}

Clusters::Impl Clusters::getContainer() const
{
    std::lock_guard lock(mutex);
    /// The following line copies container of shared_ptrs to return value under lock
    return impl;
}


/// Implementation of `Cluster` class

Cluster::Cluster(const Poco::Util::AbstractConfiguration & config, const Settings & settings, const String & cluster_name)
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

            Addresses addresses;

            const auto & prefix = config_prefix + key;
            const auto weight = config.getInt(prefix + ".weight", default_weight);

            addresses.emplace_back(config, prefix);
            const auto & address = addresses.back();

            ShardInfo info;
            info.shard_num = current_shard_num;
            info.weight = weight;

            if (address.is_local)
                info.local_addresses.push_back(address);

            ConnectionPoolPtr pool = std::make_shared<ConnectionPool>(
                settings.distributed_connections_pool_size,
                address.host_name, address.port,
                address.default_database, address.user, address.password,
                "server", address.compression, address.secure);

            info.pool = std::make_shared<ConnectionPoolWithFailover>(
                ConnectionPoolPtrs{pool}, settings.load_balancing);
            info.per_replica_pools = {std::move(pool)};

            if (weight)
                slot_to_shard.insert(std::end(slot_to_shard), weight, shards_info.size());

            shards_info.emplace_back(std::move(info));
            addresses_with_failover.emplace_back(std::move(addresses));
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
            const auto weight = config.getUInt(partial_prefix + ".weight", default_weight);

            bool internal_replication = config.getBool(partial_prefix + ".internal_replication", false);

            /// In case of internal_replication we will be appending names to dir_name_for_internal_replication
            std::string dir_name_for_internal_replication;

            auto first = true;
            for (const auto & replica_key : replica_keys)
            {
                if (startsWith(replica_key, "weight") || startsWith(replica_key, "internal_replication"))
                    continue;

                if (startsWith(replica_key, "replica"))
                {
                    replica_addresses.emplace_back(config, partial_prefix + replica_key);
                    ++current_replica_num;

                    if (!replica_addresses.back().is_local)
                    {
                        if (internal_replication)
                        {
                            auto dir_name = replica_addresses.back().toFullString();
                            if (first)
                                dir_name_for_internal_replication = dir_name;
                            else
                                dir_name_for_internal_replication += "," + dir_name;
                        }

                        if (first) first = false;
                    }
                }
                else
                    throw Exception("Unknown element in config: " + replica_key, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
            }

            Addresses shard_local_addresses;

            ConnectionPoolPtrs all_replicas_pools;
            all_replicas_pools.reserve(replica_addresses.size());

            for (const auto & replica : replica_addresses)
            {
                auto replica_pool = std::make_shared<ConnectionPool>(
                    settings.distributed_connections_pool_size,
                    replica.host_name, replica.port,
                    replica.default_database, replica.user, replica.password,
                    "server", replica.compression, replica.secure);

                all_replicas_pools.emplace_back(replica_pool);
                if (replica.is_local)
                    shard_local_addresses.push_back(replica);
            }

            ConnectionPoolWithFailoverPtr shard_pool = std::make_shared<ConnectionPoolWithFailover>(
                        all_replicas_pools, settings.load_balancing,
                        settings.distributed_replica_error_half_life.totalSeconds(), settings.distributed_replica_error_cap);

            if (weight)
                slot_to_shard.insert(std::end(slot_to_shard), weight, shards_info.size());

            shards_info.push_back({std::move(dir_name_for_internal_replication), current_shard_num, weight,
                std::move(shard_local_addresses), std::move(shard_pool), std::move(all_replicas_pools), internal_replication});
        }
        else
            throw Exception("Unknown element in config: " + key, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

        ++current_shard_num;
    }

    if (addresses_with_failover.empty())
        throw Exception("There must be either 'node' or 'shard' elements in config", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

    initMisc();
}


Cluster::Cluster(const Settings & settings, const std::vector<std::vector<String>> & names,
                 const String & username, const String & password, UInt16 clickhouse_port, bool treat_local_as_remote, bool secure)
{
    UInt32 current_shard_num = 1;

    for (const auto & shard : names)
    {
        Addresses current;
        for (auto & replica : shard)
            current.emplace_back(replica, username, password, clickhouse_port, secure);

        addresses_with_failover.emplace_back(current);

        Addresses shard_local_addresses;
        ConnectionPoolPtrs all_replicas;
        all_replicas.reserve(current.size());

        for (const auto & replica : current)
        {
            auto replica_pool = std::make_shared<ConnectionPool>(
                        settings.distributed_connections_pool_size,
                        replica.host_name, replica.port,
                        replica.default_database, replica.user, replica.password,
                        "server", replica.compression, replica.secure);
            all_replicas.emplace_back(replica_pool);
            if (replica.is_local && !treat_local_as_remote)
                shard_local_addresses.push_back(replica);
        }

        ConnectionPoolWithFailoverPtr shard_pool = std::make_shared<ConnectionPoolWithFailover>(
                all_replicas, settings.load_balancing,
                settings.distributed_replica_error_half_life.totalSeconds(), settings.distributed_replica_error_cap);

        slot_to_shard.insert(std::end(slot_to_shard), default_weight, shards_info.size());
        shards_info.push_back({{}, current_shard_num, default_weight, std::move(shard_local_addresses), std::move(shard_pool),
                               std::move(all_replicas), false});
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
}


std::unique_ptr<Cluster> Cluster::getClusterWithSingleShard(size_t index) const
{
    return std::unique_ptr<Cluster>{ new Cluster(*this, {index}) };
}

std::unique_ptr<Cluster> Cluster::getClusterWithMultipleShards(const std::vector<size_t> & indices) const
{
    return std::unique_ptr<Cluster>{ new Cluster(*this, indices) };
}

Cluster::Cluster(const Cluster & from, const std::vector<size_t> & indices)
    : shards_info{}
{
    for (size_t index : indices)
    {
        shards_info.emplace_back(from.shards_info.at(index));

        if (!from.addresses_with_failover.empty())
            addresses_with_failover.emplace_back(from.addresses_with_failover.at(index));
    }

    initMisc();
}

}
