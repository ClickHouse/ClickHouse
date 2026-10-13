#include <Server/DistributedCache/DistributedCacheServerSettings.h>

#if ENABLE_DISTRIBUTED_CACHE

#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/Defines.h>
#include <Core/DistributedCacheDefines.h>

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorToString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <cmath>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// `HOT_RELOAD` settings are re-read on `SYSTEM RELOAD CONFIG`, the rest once at startup.
#define LIST_OF_DISTRIBUTED_CACHE_SERVER_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, path, "/var/lib/clickhouse-distributed-cache/", "Working directory of the distributed cache server. Keeps the server uuid file and, unless the `cache` section overrides it, the cache data. Requires a restart to change.", 0) \
    DECLARE(UInt64, tcp_port, 0, "Port to listen on for plain distributed cache connections. Either this or `tcp_port_secure` must be set. Requires a restart to change.", 0) \
    DECLARE(UInt64, tcp_port_secure, 0, "Port to listen on for secure (TLS) distributed cache connections. Either this or `tcp_port` must be set. Requires a restart to change.", 0) \
    DECLARE(String, zookeeper_path, DistributedCache::DEFAULT_ZOOKEEPER_PATH, "Keeper path under which the server registers itself for discovery by the clients. Requires a restart to change.", 0) \
    DECLARE(String, tls_certificate_path, "", "Path to the TLS certificate which is published in the keeper registry entry and used to sign it. The path requires a restart to change, the file is read again on a config reload.", 0) \
    DECLARE(String, tls_key_path, "", "Path to the private key matching `tls_certificate_path`. The path requires a restart to change, the file is read again on a config reload.", 0) \
    DECLARE(Bool, extended_verification_for_keeper_connection, false, "Check that the certificate of the registry keeper names the host the server connects to. Independent of `openSSL.client.extendedVerification`. Requires a restart to change.", 0) \
    DECLARE(Bool, local, false, "Local (single node) mode, used for testing in CI or locally: skips availability zone detection. Requires a restart to change.", 0) \
    DECLARE(Double, weight, 0, "Self-published routing weight of this server, scaling its share of the hash ring. Must be a finite positive number. The default value 0 means that no weight is published and the share is derived from the disk size alone.", HOT_RELOAD) \
    DECLARE(UInt64, socket_receive_timeout_sec, DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, "Receive timeout of a client connection socket, in seconds. Applied when a connection is accepted, so a reloaded value affects new connections only.", HOT_RELOAD) \
    DECLARE(UInt64, socket_send_timeout_sec, DBMS_DEFAULT_SEND_TIMEOUT_SEC, "Send timeout of a client connection socket, in seconds. Applied when a connection is accepted, so a reloaded value affects new connections only.", HOT_RELOAD) \
    DECLARE(UInt64, idle_connection_timeout_sec, 300, "Close a client connection after it has been idle for this many seconds.", HOT_RELOAD) \
    DECLARE(UInt64, poll_timeout_sec, 20, "How long a single poll of a client connection waits for data, in seconds. Capped by `idle_connection_timeout_sec`.", HOT_RELOAD) \
    DECLARE(UInt64, graceful_shutdown_timeout_sec, 300, "How long the server waits for live connections to finish on shutdown before force-closing them, in seconds.", HOT_RELOAD) \
    DECLARE(Bool, use_client_id_from_certificate, false, "Take the client id from the client TLS certificate instead of the handshake packet. Requires the clients to connect to `tcp_port_secure` and to present a certificate. Requires a restart to change.", 0) \
    DECLARE(Bool, ci_mode, false, "CI-tests-only mode: allow the client id of a connection to change between requests, because the client randomizes `distributed_cache_client_id` per query. Off in production, where the client id is fixed per connection. Requires a restart to change.", 0) \
    DECLARE(UInt64, stats_log_interval_sec, 60, "Interval of the periodic distributed cache server activity log, in seconds. The value 0 disables it.", HOT_RELOAD) \
    DECLARE(UInt64, slow_down_retry_after_ms, DistributedCache::DEFAULT_SLOW_DOWN_RETRY_AFTER_MS, "Backoff period advertised in the `SlowDown` packet when a new connection is rejected because the client exceeded its share of connections, in milliseconds.", HOT_RELOAD) \
    DECLARE(UInt64, send_metrics_interval_ms, 1000, "Obsolete setting, has no effect.", SettingsTierType::OBSOLETE) \

DECLARE_SETTINGS_TRAITS(DistributedCacheServerSettingsTraits, LIST_OF_DISTRIBUTED_CACHE_SERVER_SETTINGS, DISTRIBUTED_CACHE_SERVER_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(DistributedCacheServerSettingsTraits, LIST_OF_DISTRIBUTED_CACHE_SERVER_SETTINGS, DistributedCacheServerSettings, DistributedCacheServerSetting)

DistributedCacheServerSettings::DistributedCacheServerSettings()
    : impl(std::make_unique<DistributedCacheServerSettingsImpl>())
{
}

DistributedCacheServerSettings::~DistributedCacheServerSettings() = default;

DISTRIBUTED_CACHE_SERVER_SETTINGS_SUPPORTED_TYPES(DistributedCacheServerSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

DistributedCacheServerSettings::DistributedCacheServerSettings(const DistributedCacheServerSettings & settings)
    : impl(std::make_unique<DistributedCacheServerSettingsImpl>(*settings.impl))
{
}

DistributedCacheServerSettings::DistributedCacheServerSettings(DistributedCacheServerSettings && settings) noexcept = default;

DistributedCacheServerSettings & DistributedCacheServerSettings::operator=(DistributedCacheServerSettings && settings) noexcept = default;

bool DistributedCacheServerSettings::operator==(const DistributedCacheServerSettings & settings) const noexcept
{
    return *impl == *settings.impl;
}

void DistributedCacheServerSettings::loadFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix)
{
    for (const auto & setting : impl->all())
    {
        const auto & name = setting.getName();
        const auto config_path = config_prefix + "." + name;
        if (!config.has(config_path))
            continue;

        try
        {
            /// Parse with the `Poco` getters, as before this became a settings object: they accept
            /// `yes`/`no`/`on`/`off` and a `0x` prefix, which the `SettingField` parsers do not.
            const auto type = setting.getTypeName();
            if (type == "Bool")
                impl->set(name, config.getBool(config_path));
            else if (type == "UInt64")
                impl->set(name, config.getUInt64(config_path));
            else if (type == "Double")
                impl->set(name, config.getDouble(config_path));
            else
                impl->set(name, config.getString(config_path));
        }
        catch (Exception & e)
        {
            e.addMessage("while parsing distributed cache server setting '{}'", name);
            throw;
        }
    }
}

void DistributedCacheServerSettings::validate() const
{
    const auto & settings = *this;

    if (settings[DistributedCacheServerSetting::weight].changed)
    {
        const double weight = settings[DistributedCacheServerSetting::weight];
        if (!(std::isfinite(weight) && weight > 0.0))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Distributed cache server weight must be a finite positive number, got {}", weight);
    }

    /// The ports are narrowed to `UInt16` when the listeners are created.
    const auto check_port = [&](const auto & setting, std::string_view name)
    {
        const UInt64 port = settings[setting];
        if (port > std::numeric_limits<UInt16>::max())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "`{}` must not exceed {}, got {}", name, std::numeric_limits<UInt16>::max(), port);
    };
    check_port(DistributedCacheServerSetting::tcp_port, "tcp_port");
    check_port(DistributedCacheServerSetting::tcp_port_secure, "tcp_port_secure");

    if (settings[DistributedCacheServerSetting::tls_certificate_path].value.empty()
        != settings[DistributedCacheServerSetting::tls_key_path].value.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "`tls_certificate_path` and `tls_key_path` must be either both set or both unset");
    }
}

DistributedCacheServerSettings::ReloadResult DistributedCacheServerSettings::updateHotReloadableSettings(
    const DistributedCacheServerSettings & new_settings)
{
    ReloadResult result;
    for (const auto & new_setting : new_settings.impl->all())
    {
        if (new_setting.getTier() == SettingsTierType::OBSOLETE)
            continue;

        const auto & name = new_setting.getName();
        const bool value_differs = impl->get(name) != new_setting.getValue();

        if (!new_setting.isHotReload())
        {
            if (value_differs)
                result.requiring_restart.push_back(name);
            continue;
        }

        if (!value_differs && impl->isChanged(name) == new_setting.isValueChanged())
            continue;

        /// Not `BaseSettings::updateHotReloadableSettings`: it marks every copied setting as changed,
        /// while `changed` here must keep meaning "set in the config" after a reload.
        if (new_setting.isValueChanged())
            impl->set(name, new_setting.getValue());
        else
            impl->resetToDefault(name);
        result.reloaded.push_back(name);
    }
    return result;
}

ColumnsDescription DistributedCacheServerSettings::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"server_uuid", std::make_shared<DataTypeString>(),
         "UUID of the distributed cache server the setting belongs to. A single process can be configured with more than one server."},
        {"name", std::make_shared<DataTypeString>(), "Setting name."},
        {"value", std::make_shared<DataTypeString>(), "Current setting value."},
        {"default", std::make_shared<DataTypeString>(), "Setting default value."},
        {"changed", std::make_shared<DataTypeUInt8>(), "Whether the setting was specified in the configuration."},
        {"description", std::make_shared<DataTypeString>(), "Short setting description."},
        {"type", std::make_shared<DataTypeString>(), "Setting value type."},
        {"is_hot_reloadable", std::make_shared<DataTypeUInt8>(),
         "Whether `SYSTEM RELOAD CONFIG` applies a change of this setting. A change of the remaining settings is reported and ignored until the server is restarted."},
        {"is_obsolete", std::make_shared<DataTypeUInt8>(), "Whether the setting is obsolete and has no effect."},
    };
}

void DistributedCacheServerSettings::dumpToSystemSettingsColumns(MutableColumns & res_columns, const String & server_uuid) const
{
    for (const auto & setting : impl->all())
    {
        size_t i = 0;
        res_columns[i++]->insert(server_uuid);
        res_columns[i++]->insert(setting.getName());
        res_columns[i++]->insert(setting.getValueString(/* show_secrets */true));
        res_columns[i++]->insert(setting.getDefaultValueString(/* show_secrets */true));
        res_columns[i++]->insert(setting.isValueChanged());
        res_columns[i++]->insert(setting.getDescription());
        res_columns[i++]->insert(setting.getTypeName());
        res_columns[i++]->insert(setting.isHotReload());
        res_columns[i++]->insert(setting.getTier() == SettingsTierType::OBSOLETE);
    }
}

std::string DistributedCacheServerSettings::toString() const
{
    /// Unlike `BaseSettings::toString`, lists the unchanged settings too.
    WriteBufferFromOwnString out;
    bool first = true;
    for (const auto & setting : impl->all())
    {
        if (!first)
            out << ", ";
        out << setting.getName() << " = " << applyVisitor(FieldVisitorToString(), setting.getValue());
        first = false;
    }
    return out.str();
}

}

#endif
