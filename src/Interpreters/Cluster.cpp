#include <Core/Settings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Cluster.h>
#include <base/range.h>
#include <base/sort.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>
#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/Config/ConfigHelper.h>
#include <Common/DNSResolver.h>
#include <Common/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/isLocalAddress.h>
#include <Common/parseAddress.h>
#include <Common/randomSeed.h>

#include <span>
#include <pcg_random.hpp>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 distributed_connections_pool_size;
    extern const SettingsUInt64 distributed_replica_error_cap;
    extern const SettingsSeconds distributed_replica_error_half_life;
    extern const SettingsLoadBalancing load_balancing;
    extern const SettingsBool prefer_localhost_replica;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int LOGICAL_ERROR;
    extern const int SHARD_HAS_NO_CONNECTIONS;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SYNTAX_ERROR;
    extern const int INVALID_SHARD_ID;
    extern const int NO_SUCH_REPLICA;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Default shard weight.
constexpr UInt32 default_weight = 1;

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

void concatInsertPath(std::string & insert_path, const std::string & dir_name)
{
    if (insert_path.empty())
        insert_path = dir_name;
    else
        insert_path += "," + dir_name;
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


Cluster::Address::Address(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        const String & cluster_,
        const String & cluster_secret_,
        UInt32 shard_index_,
        UInt32 replica_index_)
    : cluster(cluster_)
    , cluster_secret(cluster_secret_)
    , shard_index(shard_index_)
    , replica_index(replica_index_)
{
    host_name = config.getString(config_prefix + ".host");
    if (config.has(config_prefix + ".user"))
        user_specified = true;

    user = config.getString(config_prefix + ".user", "default");
    password = config.getString(config_prefix + ".password", "");
    default_database = config.getString(config_prefix + ".default_database", "");
    secure = ConfigHelper::getBool(config, config_prefix + ".secure", false, /* empty_as */true) ? Protocol::Secure::Enable : Protocol::Secure::Disable;
    priority = Priority{config.getInt(config_prefix + ".priority", 1)};

    proto_send_chunked = config.getString(config_prefix + ".proto_caps.send", "notchunked");
    proto_recv_chunked = config.getString(config_prefix + ".proto_caps.recv", "notchunked");

    const char * port_type = secure == Protocol::Secure::Enable ? "tcp_port_secure" : "tcp_port";
    auto default_port = config.getInt(port_type, 0);

    port = static_cast<UInt16>(config.getInt(config_prefix + ".port", default_port));
    if (!port)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Port is not specified in cluster configuration: {}.port", config_prefix);

    is_local = isLocal(config.getInt(port_type, 0));

    /// By default compression is disabled if address looks like localhost.
    /// NOTE: it's still enabled when interacting with servers on different port, but we don't want to complicate the logic.
    compression = config.getBool(config_prefix + ".compression", !is_local)
        ? Protocol::Compression::Enable : Protocol::Compression::Disable;
}


Cluster::Address::Address(
    const DatabaseReplicaInfo & info,
    const ClusterConnectionParameters & params,
    UInt32 shard_index_,
    UInt32 replica_index_)
    : user(params.username), password(params.password)
{
    bool can_be_local = true;
    std::pair<std::string, UInt16> parsed_host_port;
    if (!params.treat_local_port_as_remote)
    {
        parsed_host_port = parseAddress(info.hostname, params.clickhouse_port);
    }
    else
    {
        /// For clickhouse-local (treat_local_port_as_remote) try to read the address without passing a default port
        /// If it works we have a full address that includes a port, which means it won't be local
        /// since clickhouse-local doesn't listen in any port
        /// If it doesn't include a port then use the default one and it could be local (if the address is)
        try
        {
            parsed_host_port = parseAddress(info.hostname, 0);
            can_be_local = false;
        }
        catch (...)
        {
            parsed_host_port = parseAddress(info.hostname, params.clickhouse_port);
        }
    }
    host_name = parsed_host_port.first;
    database_shard_name = info.shard_name;
    database_replica_name = info.replica_name;
    port = parsed_host_port.second;
    secure = params.secure ? Protocol::Secure::Enable : Protocol::Secure::Disable;
    priority = params.priority;
    is_local = can_be_local && isLocal(params.clickhouse_port);
    shard_index = shard_index_;
    replica_index = replica_index_;
    cluster = params.cluster_name;
    cluster_secret = params.cluster_secret;
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
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Incorrect <host>:<port> format {}", host_port_string);

    return {unescapeForFileName(host_port_string.substr(0, pos)), parse<UInt16>(host_port_string.substr(pos + 1))};
}


String Cluster::Address::toFullString(bool use_compact_format) const
{
    if (use_compact_format)
    {
        if (shard_index == 0 || replica_index == 0)
            // shard_num/replica_num like in system.clusters table
            throw Exception(ErrorCodes::LOGICAL_ERROR, "shard_num/replica_num cannot be zero");

        return fmt::format("shard{}_replica{}", shard_index, replica_index);
    }
    else
    {
        return
            escapeForFileName(user)
            + (password.empty() ? "" : (':' + escapeForFileName(password))) + '@'
            + escapeForFileName(host_name) + ':' + std::to_string(port)
            + (default_database.empty() ? "" : ('#' + escapeForFileName(default_database)))
            + ((secure == Protocol::Secure::Enable) ? "+secure" : "");
    }
}

Cluster::Address Cluster::Address::fromFullString(std::string_view full_string)
{
    std::string_view user_password;
    if (auto pos = full_string.find('@'); pos != std::string_view::npos)
        user_password = full_string.substr(pos + 1);

    /// parsing with the new shard{shard_index}[_replica{replica_index}] format
    if (user_password.empty() && full_string.starts_with("shard"))
    {
        Address address;

        if (auto underscore_pos = full_string.find('_'); underscore_pos != std::string_view::npos)
        {
            address.shard_index = parse<UInt32>(full_string.substr(0, underscore_pos).substr(strlen("shard")));

            if (full_string.substr(underscore_pos + 1).starts_with("replica"))
            {
                address.replica_index = parse<UInt32>(full_string.substr(underscore_pos + 1 + strlen("replica")));
            }
            else if (full_string.substr(underscore_pos + 1).starts_with("all_replicas"))
            {
                address.replica_index = 0;
            }
            else
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Incorrect address '{}', should be in a form of `shardN_all_replicas` or `shardN_replicaM`", full_string);
        }
        else
        {
            address.shard_index = parse<UInt32>(full_string.substr(strlen("shard")));
            address.replica_index = 0;
        }

        return address;
    }
    else
    {
        /// parsing with the old user[:password]@host:port#default_database format
        /// This format is appeared to be inconvenient for the following reasons:
        /// - credentials are exposed in file name;
        /// - the file name can be too long.

        const char * address_begin = full_string.data();
        const char * address_end = address_begin + full_string.size();
        const char * user_pw_end = strchr(address_begin, '@');

        Protocol::Secure secure = Protocol::Secure::Disable;
        const char * secure_tag = "+secure";
        if (full_string.ends_with(secure_tag))
        {
            address_end -= strlen(secure_tag);
            secure = Protocol::Secure::Enable;
        }

        const char * colon = strchr(full_string.data(), ':');
        if (!user_pw_end || !colon)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Incorrect user[:password]@host:port#default_database format {}", full_string);

        const bool has_pw = colon < user_pw_end;
        const char * host_end = has_pw ? strchr(user_pw_end + 1, ':') : colon;
        if (!host_end)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Incorrect address '{}', it does not contain port", full_string);

        const char * has_db = strchr(full_string.data(), '#');
        const char * port_end = has_db ? has_db : address_end;

        Address address;
        address.secure = secure;
        address.port = parse<UInt16>(host_end + 1, port_end - (host_end + 1));
        address.host_name = unescapeForFileName(std::string(user_pw_end + 1, host_end));
        address.user = unescapeForFileName(std::string(address_begin, has_pw ? colon : user_pw_end));
        address.password = has_pw ? unescapeForFileName(std::string(colon + 1, user_pw_end)) : std::string();
        address.default_database = has_db ? unescapeForFileName(std::string(has_db + 1, address_end)) : std::string();
        // address.priority ignored
        return address;
    }
}


/// Implementation of Clusters class

Clusters::Clusters(const Poco::Util::AbstractConfiguration & config, const Settings & settings, MultiVersion<Macros>::Version macros, const String & config_prefix)
{
    this->macros_ = macros;
    updateClusters(config, settings, config_prefix);
}


ClusterPtr Clusters::getCluster(const std::string & cluster_name) const
{
    std::lock_guard lock(mutex);

    auto expanded_cluster_name = macros_->expand(cluster_name);
    auto it = impl.find(expanded_cluster_name);
    return (it != impl.end()) ? it->second : nullptr;
}


void Clusters::setCluster(const String & cluster_name, const std::shared_ptr<Cluster> & cluster)
{
    std::lock_guard lock(mutex);
    impl[cluster_name] = cluster;
}


void Clusters::updateClusters(const Poco::Util::AbstractConfiguration & new_config, const Settings & settings, const String & config_prefix, Poco::Util::AbstractConfiguration * old_config)
{
    Poco::Util::AbstractConfiguration::Keys new_config_keys;
    new_config.keys(config_prefix, new_config_keys);

    /// If old config is set, we will update only clusters with updated config.
    /// In this case, we first need to find clusters that were deleted from config.
    Poco::Util::AbstractConfiguration::Keys deleted_keys;
    if (old_config)
    {
        ::sort(new_config_keys.begin(), new_config_keys.end());

        Poco::Util::AbstractConfiguration::Keys old_config_keys;
        old_config->keys(config_prefix, old_config_keys);
        ::sort(old_config_keys.begin(), old_config_keys.end());

        std::set_difference(
            old_config_keys.begin(), old_config_keys.end(), new_config_keys.begin(), new_config_keys.end(), std::back_inserter(deleted_keys));
    }

    std::lock_guard lock(mutex);

    /// If old config is set, remove deleted clusters from impl, otherwise just clear it.
    if (old_config)
    {
        for (const auto & key : deleted_keys)
        {
            if (!automatic_clusters.contains(key))
                impl.erase(key);
        }
    }
    else
    {
        if (!automatic_clusters.empty())
            std::erase_if(impl, [this](const auto & e) { return automatic_clusters.contains(e.first); });
        else
            impl.clear();
    }


    for (const auto & key : new_config_keys)
    {
        if (new_config.has(config_prefix + "." + key + ".discovery"))
        {
            /// Handled in ClusterDiscovery
            automatic_clusters.insert(key);
            continue;
        }

        if (key.find('.') != String::npos)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Cluster names with dots are not supported: '{}'", key);

        /// If old config is set and cluster config wasn't changed, don't update this cluster.
        if (!old_config || !isSameConfiguration(new_config, *old_config, config_prefix + "." + key))
            impl[key] = std::make_shared<Cluster>(new_config, settings, config_prefix, key);
    }
}

Clusters::Impl Clusters::getContainer() const
{
    std::lock_guard lock(mutex);
    /// The following line copies container of shared_ptrs to return value under lock
    return impl;
}


/// Implementation of `Cluster` class

Cluster::Cluster(const Poco::Util::AbstractConfiguration & config,
                 const Settings & settings,
                 const String & config_prefix_,
                 const String & cluster_name) : name(cluster_name)
{
    auto config_prefix = config_prefix_ + "." + cluster_name;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_prefix, config_keys);

    config_prefix += ".";

    secret = config.getString(config_prefix + "secret", "");
    std::erase(config_keys, "secret");

    allow_distributed_ddl_queries = config.getBool(config_prefix + "allow_distributed_ddl_queries", true);
    std::erase(config_keys, "allow_distributed_ddl_queries");

    if (config_keys.empty())
        throw Exception(ErrorCodes::SHARD_HAS_NO_CONNECTIONS, "No cluster elements (shard, node) specified in config at path {}", config_prefix);

    UInt32 current_shard_num = 1;
    for (const auto & key : config_keys)
    {
        if (startsWith(key, "node"))
        {
            /// Shard without replicas.

            Addresses addresses;

            const auto & prefix = config_prefix + key;
            const auto weight = config.getInt(prefix + ".weight", default_weight);

            addresses.emplace_back(config, prefix, cluster_name, secret, current_shard_num, 1);
            const auto & address = addresses.back();

            ShardInfo info;
            info.shard_num = current_shard_num;
            info.weight = weight;

            if (address.is_local)
                info.local_addresses.push_back(address);

            auto pool = ConnectionPoolFactory::instance().get(
                static_cast<unsigned>(settings[Setting::distributed_connections_pool_size]),
                address.host_name,
                address.port,
                address.default_database,
                address.user,
                address.password,
                address.proto_send_chunked,
                address.proto_recv_chunked,
                address.quota_key,
                address.cluster,
                address.cluster_secret,
                "server",
                address.compression,
                address.secure,
                address.priority);

            info.pool = std::make_shared<ConnectionPoolWithFailover>(ConnectionPoolPtrs{pool}, settings[Setting::load_balancing]);
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

            ShardInfoInsertPathForInternalReplication insert_paths;
            /// "_all_replicas" is a marker that will be replaced with all replicas
            /// (for creating connections in the Distributed engine)
            insert_paths.compact = fmt::format("shard{}_all_replicas", current_shard_num);

            for (const auto & replica_key : replica_keys)
            {
                if (startsWith(replica_key, "weight") || startsWith(replica_key, "internal_replication"))
                    continue;

                if (startsWith(replica_key, "replica"))
                {
                    replica_addresses.emplace_back(config,
                        partial_prefix + replica_key,
                        cluster_name,
                        secret,
                        current_shard_num,
                        current_replica_num);
                    ++current_replica_num;

                    if (internal_replication)
                    {
                        auto dir_name = replica_addresses.back().toFullString(/* use_compact_format= */ false);
                        if (!replica_addresses.back().is_local)
                            concatInsertPath(insert_paths.prefer_localhost_replica, dir_name);
                        concatInsertPath(insert_paths.no_prefer_localhost_replica, dir_name);
                    }
                }
                else
                    throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown element in config: {}", replica_key);
            }

            addShard(
                settings,
                replica_addresses,
                /* treat_local_as_remote = */ false,
                current_shard_num,
                weight,
                std::move(insert_paths),
                internal_replication);
        }
        else
            throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown element in config: {}", key);

        ++current_shard_num;
    }

    if (addresses_with_failover.empty())
        throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "There must be either 'node' or 'shard' elements in config");

    initMisc();
}


Cluster::Cluster(
    const Settings & settings,
    const std::vector<std::vector<String>> & names,
    const ClusterConnectionParameters & params)
{
    UInt32 current_shard_num = 1;

    secret = params.cluster_secret;

    for (const auto & shard : names)
    {
        Addresses current;
        for (const auto & replica : shard)
            current.emplace_back(
                DatabaseReplicaInfo{replica, "", ""},
                params,
                current_shard_num,
                current.size() + 1);

        addresses_with_failover.emplace_back(current);

        addShard(settings, std::move(current), params.treat_local_as_remote, current_shard_num, /* weight= */ 1);
        ++current_shard_num;
    }

    initMisc();
}

Cluster::Cluster(
    const Settings & settings,
    const std::vector<std::vector<DatabaseReplicaInfo>> & infos,
    const ClusterConnectionParameters & params)
{
    UInt32 current_shard_num = 1;

    secret = params.cluster_secret;

    for (const auto & shard : infos)
    {
        Addresses current;
        for (const auto & replica : shard)
            current.emplace_back(
                replica,
                params,
                current_shard_num,
                current.size() + 1);

        addresses_with_failover.emplace_back(current);

        addShard(settings, std::move(current), params.treat_local_as_remote, current_shard_num, /* weight= */ 1);
        ++current_shard_num;
    }

    initMisc();
}

void Cluster::addShard(
    const Settings & settings,
    Addresses addresses,
    bool treat_local_as_remote,
    UInt32 current_shard_num,
    UInt32 weight,
    ShardInfoInsertPathForInternalReplication insert_paths,
    bool internal_replication)
{
    Addresses shard_local_addresses;

    ConnectionPoolPtrs all_replicas_pools;
    all_replicas_pools.reserve(addresses.size());

    for (const auto & replica : addresses)
    {
        auto replica_pool = ConnectionPoolFactory::instance().get(
            static_cast<unsigned>(settings[Setting::distributed_connections_pool_size]),
            replica.host_name,
            replica.port,
            replica.default_database,
            replica.user,
            replica.password,
            replica.proto_send_chunked,
            replica.proto_recv_chunked,
            replica.quota_key,
            replica.cluster,
            replica.cluster_secret,
            "server",
            replica.compression,
            replica.secure,
            replica.priority);

        all_replicas_pools.emplace_back(replica_pool);
        if (replica.is_local && !treat_local_as_remote)
            shard_local_addresses.push_back(replica);
    }
    ConnectionPoolWithFailoverPtr shard_pool = std::make_shared<ConnectionPoolWithFailover>(
        all_replicas_pools,
        settings[Setting::load_balancing],
        settings[Setting::distributed_replica_error_half_life].totalSeconds(),
        settings[Setting::distributed_replica_error_cap]);

    if (weight)
        slot_to_shard.insert(std::end(slot_to_shard), weight, shards_info.size());

    shards_info.push_back({
        std::move(insert_paths),
        current_shard_num,
        weight,
        std::move(shard_local_addresses),
        std::move(shard_pool),
        std::move(all_replicas_pools),
        internal_replication
    });
}


Poco::Timespan Cluster::saturate(Poco::Timespan v, Poco::Timespan limit)
{
    if (limit.totalMicroseconds() == 0)
        return v;
    else
        return (v > limit) ? limit : v;
}


void Cluster::initMisc()
{
    /// NOTE: It is possible to have cluster w/o shards for
    /// optimize_skip_unused_shards (i.e. WHERE 0 expression), so check the
    /// slots only if shards is not empty.
    if (!shards_info.empty() && slot_to_shard.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cluster with zero weight on all shards is prohibited");

    for (const auto & shard_info : shards_info)
    {
        if (!shard_info.isLocal() && !shard_info.hasRemoteConnections())
            throw Exception(ErrorCodes::SHARD_HAS_NO_CONNECTIONS, "Found shard without any specified connection");
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

std::unique_ptr<Cluster> Cluster::getClusterWithReplicasAsShards(const Settings & settings, size_t max_replicas_from_shard) const
{
    return std::unique_ptr<Cluster>{ new Cluster(ReplicasAsShardsTag{}, *this, settings, max_replicas_from_shard)};
}

std::unique_ptr<Cluster> Cluster::getClusterWithSingleShard(size_t index) const
{
    return std::unique_ptr<Cluster>{ new Cluster(SubclusterTag{}, *this, {index}) };
}

std::unique_ptr<Cluster> Cluster::getClusterWithMultipleShards(const std::vector<size_t> & indices) const
{
    return std::unique_ptr<Cluster>{ new Cluster(SubclusterTag{}, *this, indices) };
}

namespace
{

void shuffleReplicas(std::vector<Cluster::Address> & replicas, const Settings & settings, size_t replicas_needed)
{
    pcg64_fast gen{randomSeed()};

    if (settings[Setting::prefer_localhost_replica])
    {
        // force for local replica to always be included
        auto first_non_local_replica = std::partition(replicas.begin(), replicas.end(), [](const auto & replica) { return replica.is_local; });
        size_t local_replicas_count = first_non_local_replica - replicas.begin();

        if (local_replicas_count == replicas_needed)
        {
            /// we have exact amount of local replicas as needed, no need to do anything
            return;
        }

        if (local_replicas_count > replicas_needed)
        {
            /// we can use only local replicas, shuffle them
            std::shuffle(replicas.begin(), first_non_local_replica, gen);
            return;
        }

        /// shuffle just non local replicas
        std::shuffle(first_non_local_replica, replicas.end(), gen);
        return;
    }

    std::shuffle(replicas.begin(), replicas.end(), gen);
}

}

Cluster::Cluster(Cluster::ReplicasAsShardsTag, const Cluster & from, const Settings & settings, size_t max_replicas_from_shard)
{
    if (from.addresses_with_failover.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster is empty");

    UInt32 shard_num = 0;
    std::set<std::pair<String, int>> unique_hosts;
    for (size_t shard_index : collections::range(0, from.shards_info.size()))
    {
        auto create_shards_from_replicas = [&](std::span<const Address> replicas)
        {
            for (const auto & address : replicas)
            {
                if (!unique_hosts.emplace(address.host_name, address.port).second)
                    continue;   /// Duplicate host, skip.

                ShardInfo info;
                info.shard_num = ++shard_num;
                info.weight = 1;

                if (address.is_local)
                    info.local_addresses.push_back(address);

                auto pool = ConnectionPoolFactory::instance().get(
                    static_cast<unsigned>(settings[Setting::distributed_connections_pool_size]),
                    address.host_name,
                    address.port,
                    address.default_database,
                    address.user,
                    address.password,
                    address.proto_send_chunked,
                    address.proto_recv_chunked,
                    address.quota_key,
                    address.cluster,
                    address.cluster_secret,
                    "server",
                    address.compression,
                    address.secure,
                    address.priority);

                info.pool = std::make_shared<ConnectionPoolWithFailover>(ConnectionPoolPtrs{pool}, settings[Setting::load_balancing]);
                info.per_replica_pools = {std::move(pool)};

                addresses_with_failover.emplace_back(Addresses{address});

                slot_to_shard.insert(std::end(slot_to_shard), info.weight, shards_info.size());
                shards_info.emplace_back(std::move(info));
            }
        };

        const auto & replicas = from.addresses_with_failover[shard_index];
        if (!max_replicas_from_shard || replicas.size() <= max_replicas_from_shard)
        {
            create_shards_from_replicas(replicas);
        }
        else
        {
            auto shuffled_replicas = replicas;
            // shuffle replicas so we don't always pick the same subset
            shuffleReplicas(shuffled_replicas, settings, max_replicas_from_shard);
            create_shards_from_replicas(std::span{shuffled_replicas.begin(), max_replicas_from_shard});
        }
    }

    secret = from.secret;
    name = from.name;

    initMisc();
}


Cluster::Cluster(Cluster::SubclusterTag, const Cluster & from, const std::vector<size_t> & indices)
{
    for (size_t index : indices)
    {
        const auto & from_shard = from.shards_info.at(index);

        if (from_shard.weight)
            slot_to_shard.insert(std::end(slot_to_shard), from_shard.weight, shards_info.size());
        shards_info.emplace_back(from_shard);

        if (!from.addresses_with_failover.empty())
            addresses_with_failover.emplace_back(from.addresses_with_failover.at(index));
    }

    secret = from.secret;
    name = from.name;

    initMisc();
}

std::vector<Strings> Cluster::getHostIDs() const
{
    std::vector<Strings> host_ids;
    host_ids.resize(addresses_with_failover.size());
    for (size_t i = 0; i != addresses_with_failover.size(); ++i)
    {
        const auto & addresses = addresses_with_failover[i];
        host_ids[i].resize(addresses.size());
        for (size_t j = 0; j != addresses.size(); ++j)
            host_ids[i][j] = addresses[j].toString();
    }
    return host_ids;
}

std::vector<const Cluster::Address *> Cluster::filterAddressesByShardOrReplica(size_t only_shard_num, size_t only_replica_num) const
{
    std::vector<const Address *> res;

    auto enumerate_replicas = [&](size_t shard_index)
    {
        if (shard_index > addresses_with_failover.size())
            throw Exception(ErrorCodes::INVALID_SHARD_ID, "Cluster {} doesn't have shard #{}", name, shard_index);
        const auto & replicas = addresses_with_failover[shard_index - 1];
        if (only_replica_num)
        {
            if (only_replica_num > replicas.size())
                throw Exception(ErrorCodes::NO_SUCH_REPLICA, "Cluster {} doesn't have replica #{} in shard #{}", name, only_replica_num, shard_index);
            res.emplace_back(&replicas[only_replica_num - 1]);
        }
        else
        {
            for (const auto & addr : replicas)
                res.emplace_back(&addr);
        }
    };

    if (only_shard_num)
    {
        enumerate_replicas(only_shard_num);
    }
    else
    {
        for (size_t shard_index = 1; shard_index <= addresses_with_failover.size(); ++shard_index)
            enumerate_replicas(shard_index);
    }

    return res;
}

const std::string & Cluster::ShardInfo::insertPathForInternalReplication(bool prefer_localhost_replica, bool use_compact_format) const
{
    if (!has_internal_replication)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "internal_replication is not set");

    const auto & paths = insert_path_for_internal_replication;
    if (!use_compact_format)
    {
        const auto & path = prefer_localhost_replica ? paths.prefer_localhost_replica : paths.no_prefer_localhost_replica;
        if (path.size() > NAME_MAX)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Path '{}' for async distributed INSERT is too long (exceed {} limit)", path, NAME_MAX);
        }
        return path;
    }
    else
    {
        return paths.compact;
    }
}

bool Cluster::maybeCrossReplication() const
{
    /// Cluster can be used for cross-replication if some replicas have different default database names,
    /// so one clickhouse-server instance can contain multiple replicas.

    if (addresses_with_failover.empty())
        return false;

    const String & database_name = addresses_with_failover.front().front().default_database;
    for (const auto & shard : addresses_with_failover)
        for (const auto & replica : shard)
            if (replica.default_database != database_name)
                return true;

    return false;
}

}
