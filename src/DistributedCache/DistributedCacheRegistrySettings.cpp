#if ENABLE_DISTRIBUTED_CACHE

#include <DistributedCache/DistributedCacheRegistrySettings.h>
#include <DistributedCache/DistributedCacheCommon.h>
#include <Core/DistributedCacheDefines.h>

#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Common/Exception.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <algorithm>
#include <array>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NO_ELEMENTS_IN_CONFIG;
}

#define LIST_OF_REGISTRY_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, keeper_path, DistributedCache::DEFAULT_ZOOKEEPER_PATH, "ZooKeeper path for distributed cache registry", 0) \
    DECLARE(String, client_id, "", "Client identifier for distributed cache", 0) \
    DECLARE(UInt64, client_weight, DistributedCache::DEFAULT_CLIENT_WEIGHT, "Client weight for load balancing", 0) \
    DECLARE(Bool, secure, false, "Use secure connections", 0) \
    DECLARE(Bool, extended_verification_for_cache_server_connection, false, "Check that the certificate of a distributed cache server names the host the client connects to. Independent of `openSSL.client.extendedVerification`. Requires a restart to change.", 0) \
    DECLARE(Bool, extended_verification_for_keeper_connection, false, "Check that the certificate of the registry keeper names the host the client connects to. Independent of `openSSL.client.extendedVerification`. Requires a restart to change.", 0) \
    DECLARE(UInt64, connection_pool_size, DistributedCache::DEFAULT_CONNECTION_POOL_SIZE, "Total connection pool size across all servers", 0) \
    DECLARE(UInt64, connection_pool_size_per_server, DistributedCache::DEFAULT_CONNECTION_POOL_SIZE_PER_SERVER, "Connection pool size per server", 0) \
    DECLARE(UInt64, connection_pool_ttl_sec, DistributedCache::DEFAULT_CONNECTION_TTL_SEC, "Connection TTL in seconds; 0 disables age-based expiry", 0) \
    DECLARE(UInt64, max_packet_size, DistributedCache::DEFAULT_MAX_PACKET_SIZE, "Maximum packet size for distributed cache protocol", 0) \
    DECLARE(UInt64, virtual_nodes_per_reference_space, DistributedCache::DEFAULT_VIRTUAL_NODES_PER_REFERENCE_SPACE, "Number of virtual nodes assigned to a server whose disk size equals virtual_nodes_reference_space_bytes", 0) \
    DECLARE(UInt64, virtual_nodes_reference_space_bytes, DistributedCache::DEFAULT_VIRTUAL_NODES_REFERENCE_SPACE_BYTES, "Reference disk capacity in bytes used to scale vnode count", 0) \
    DECLARE(UInt64, max_virtual_nodes_per_server_hard_limit, DistributedCache::DEFAULT_MAX_VIRTUAL_NODES_PER_SERVER_HARD_LIMIT, "Hard cap on vnodes per server regardless of disk size or self-published weight. Must be greater than 0 (unlimited is not allowed).", 0) \
    DECLARE(Bool, is_local, false, "Use local mode (for testing)", 0) \
    DECLARE(Bool, ci_mode, false, "CI-tests-only mode: honor the per-query `distributed_cache_client_id` override. Off in production, where the client id is fixed by config.", 0) \
    DECLARE(UInt64, registry_watch_wait_timeout_milliseconds, 5000, "Timeout for watching registry changes", 0) \
    DECLARE(String, offset_alignment_path, "", "Path to store offset alignment information (derived from keeper_path when empty)", 0) \
    DECLARE(UInt64, offset_alignment, DistributedCache::DEFAULT_OFFSET_ALIGNMENT, "Offset alignment for cache operations", 0) \
    DECLARE(Bool, force_update_offset_alignment, false, "Force update of offset alignment", 0) \
    DECLARE(String, certificate_chain_path, "", "Path to certificate chain for secure connections", 0) \
    DECLARE(Bool, tcp_no_delay, true, "Enable TCP_NODELAY socket option", 0) \
    DECLARE(UInt64, server_backoff_min_ms, DistributedCache::DEFAULT_SERVER_BACKOFF_MIN_MS, "Minimum backoff time for server retry", 0) \
    DECLARE(UInt64, server_backoff_max_ms, DistributedCache::DEFAULT_SERVER_BACKOFF_MAX_MS, "Maximum backoff time for server retry", 0)

DECLARE_SETTINGS_TRAITS(RegistrySettingsTraits, LIST_OF_REGISTRY_SETTINGS, REGISTRY_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(RegistrySettingsTraits, LIST_OF_REGISTRY_SETTINGS, RegistrySettings, RegistrySetting)

RegistrySettings::RegistrySettings() : impl(std::make_unique<RegistrySettingsImpl>())
{
}

RegistrySettings::~RegistrySettings() = default;

REGISTRY_SETTINGS_SUPPORTED_TYPES(RegistrySettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

RegistrySettings::RegistrySettings(const RegistrySettings & settings)
    : impl(std::make_unique<RegistrySettingsImpl>(*settings.impl))
{
}

RegistrySettings::RegistrySettings(RegistrySettings && settings) noexcept = default;

RegistrySettings & RegistrySettings::operator=(RegistrySettings && settings) noexcept = default;

bool RegistrySettings::operator==(const RegistrySettings & settings) const noexcept
{
    return *impl == *settings.impl;
}

void RegistrySettings::validate() const
{
    const auto & client_id = (*this)[RegistrySetting::client_id].value;
    if (client_id.empty())
    {
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                        "Expected non-empty `client_id` in config. "
                        "Note that the value must be the same for all servers within the cluster");
    }

    if (auto reason = DistributedCache::getClientIDRejectionReason(client_id))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid `client_id` in config: it {}", *reason);

    const auto & client_weight = (*this)[RegistrySetting::client_weight].value;
    if (client_weight == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "client_weight cannot be zero");

    const auto & offset_alignment = (*this)[RegistrySetting::offset_alignment].value;
    if (offset_alignment == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Offset alignment cannot be zero");

    const auto & virtual_nodes_reference_space_bytes = (*this)[RegistrySetting::virtual_nodes_reference_space_bytes].value;
    if (virtual_nodes_reference_space_bytes == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "virtual_nodes_reference_space_bytes cannot be zero");

    const auto & max_packet_size = (*this)[RegistrySetting::max_packet_size].value;
    if (max_packet_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "max_packet_size cannot be zero");

    const auto & server_backoff_min_ms = (*this)[RegistrySetting::server_backoff_min_ms].value;
    const auto & server_backoff_max_ms = (*this)[RegistrySetting::server_backoff_max_ms].value;
    if (server_backoff_min_ms > server_backoff_max_ms)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "server_backoff_min_ms ({}) must not exceed server_backoff_max_ms ({})",
            server_backoff_min_ms, server_backoff_max_ms);

    const auto & connection_pool_size = (*this)[RegistrySetting::connection_pool_size].value;
    const auto & connection_pool_size_per_server = (*this)[RegistrySetting::connection_pool_size_per_server].value;
    if (connection_pool_size < connection_pool_size_per_server)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "connection_pool_size cannot be less than connection_pool_size_per_server");
    }

    const auto & max_virtual_nodes_per_server_hard_limit = (*this)[RegistrySetting::max_virtual_nodes_per_server_hard_limit].value;
    if (max_virtual_nodes_per_server_hard_limit == 0)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Setting 'max_virtual_nodes_per_server_hard_limit' must be greater than 0, "
            "got {} (unlimited is not allowed)",
            max_virtual_nodes_per_server_hard_limit);
}

void RegistrySettings::loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    if (!config.has(config_prefix))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_prefix, config_keys);

    for (const std::string & key : config_keys)
    {
        const auto full_key = config_prefix + "." + key;

        /// Skip subsections (e.g. `zookeeper`/`keeper`) —
        /// they are handled separately and are not registry settings.
        Poco::Util::AbstractConfiguration::Keys sub_keys;
        config.keys(full_key, sub_keys);
        if (!sub_keys.empty())
            continue;

        /// Normalize legacy config key names to current setting names.
        static constexpr std::array<std::pair<std::string_view, std::string_view>, 3> aliases = {{
            {"zookeeper_path",                     "keeper_path"},
            {"local",                              "is_local"},
            {"connection_pool_connection_ttl_sec", "connection_pool_ttl_sec"},
        }};
        const auto alias_it = std::ranges::find_if(aliases, [&](const auto & p) { return p.first == key; });
        const std::string_view actual_key = (alias_it != aliases.end()) ? alias_it->second : std::string_view{key};
        impl->set(actual_key, config.getString(full_key));
    }
}

}

#endif
