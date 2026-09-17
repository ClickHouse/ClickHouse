#pragma once

#include <DataTypes/UDT/IAuthorityAdapter.h>
#include <DataTypes/UDT/ResourceLimits.h>

#include <Core/Types.h>
#include <Core/UUID.h>

#include <optional>
#include <stdexcept>
#include <string_view>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB::UDT
{

inline constexpr UInt16 database_resource_quota_override_format_version = 2;
inline constexpr std::string_view database_resource_quota_override_hash_domain
    = "ClickHouse UDT Atomic database resource quota override V2";

class DatabaseResourceQuotaSettingsError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidFormat,
        DatabaseMismatch,
        ChecksumMismatch,
    };

    DatabaseResourceQuotaSettingsError(Code code_, std::string_view message);

    const Code code;
};

/// Process-global server layer plus an optional canonical, UUID-selected
/// database-layer replacement. The database bytes are persisted separately
/// from canonical authority V1 and may be replayed without the server config.
struct ResolvedDatabaseResourceQuotaConfiguration final
{
    ResourceLimitLayer server_layer;
    std::optional<String> encoded_database_override;
};

/// Encodes one complete Database layer. Every database-applicable ResourceLimit
/// is present exactly once in stable enum order; query/server-only identities
/// are rejected instead of being silently retained.
[[nodiscard]] String encodeDatabaseResourceQuotaOverrideV2(UUID database_uuid, const ResourceLimitLayer & database_layer);
[[nodiscard]] ResourceLimitLayer decodeDatabaseResourceQuotaOverrideV2(std::string_view bytes, UUID expected_database_uuid);

/// Reads `user_defined_types.resource_limits.server` and the optional exact
/// UUID child under `user_defined_types.resource_limits.database_overrides`.
/// Configured values replace the normative default at their own layer and may
/// raise it only up to the immutable implementation maximum.
[[nodiscard]] ResolvedDatabaseResourceQuotaConfiguration resolveDatabaseResourceQuotaConfigurationFromConfig(
    const Poco::Util::AbstractConfiguration & config, UUID database_uuid, UInt64 detected_memory_bytes);

/// Complete database admission tuple. The immutable implementation layer is
/// implicit in calculateEffectiveResourceLimits; the three supplied layers are
/// all finite and the authority adapter can only retain or lower the result.
[[nodiscard]] EffectiveResourceLimits calculateEffectiveDatabaseResourceLimits(
    const ResourceLimitLayer & server_layer, const ResourceLimitLayer & database_layer, const TypeAuthorityLimits & authority_limits);

}
