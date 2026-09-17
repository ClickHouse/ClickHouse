#include <Databases/UDT/DatabaseResourceQuotaSettings.h>

#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/ResourceLimitAdapters.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <algorithm>
#include <array>
#include <limits>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

constexpr std::string_view quota_override_magic = "CHUDTQL2";

[[noreturn]] void settingsFail(DatabaseResourceQuotaSettingsError::Code code, std::string_view message)
{
    throw DatabaseResourceQuotaSettingsError(code, message);
}

void appendUInt16LE(String & output, UInt16 value)
{
    output.push_back(static_cast<char>(value));
    output.push_back(static_cast<char>(value >> 8));
}

void appendUInt64LE(String & output, UInt64 value)
{
    for (size_t index = 0; index < sizeof(value); ++index)
        output.push_back(static_cast<char>(value >> (8 * index)));
}

void appendUUID(String & output, UUID value)
{
    const auto canonical = uuidToCanonicalBytes(value);
    output.append(reinterpret_cast<const char *>(canonical.data()), canonical.size());
}

void appendDigest(String & output, const Digest & digest)
{
    output.append(reinterpret_cast<const char *>(digest.data()), digest.size());
}

class QuotaOverrideReader final
{
public:
    explicit QuotaOverrideReader(std::string_view bytes_)
        : bytes(bytes_)
    {
    }

    std::string_view readBytes(size_t size)
    {
        if (size > bytes.size() - position)
            settingsFail(DatabaseResourceQuotaSettingsError::Code::InvalidFormat, "database resource quota override is truncated");
        const auto result = bytes.substr(position, size);
        position += size;
        return result;
    }

    UInt16 readUInt16LE()
    {
        const auto value = readBytes(sizeof(UInt16));
        return static_cast<UInt16>(static_cast<UInt8>(value[0]) | (static_cast<UInt16>(static_cast<UInt8>(value[1])) << 8));
    }

    UInt64 readUInt64LE()
    {
        const auto value = readBytes(sizeof(UInt64));
        UInt64 result = 0;
        for (size_t index = 0; index < sizeof(UInt64); ++index)
            result |= static_cast<UInt64>(static_cast<UInt8>(value[index])) << (8 * index);
        return result;
    }

    UUID readUUID()
    {
        const auto value = readBytes(sizeof(CanonicalUUID));
        CanonicalUUID canonical{};
        std::copy(value.begin(), value.end(), reinterpret_cast<char *>(canonical.data()));
        return uuidFromCanonicalBytes(canonical);
    }

    Digest readDigest()
    {
        const auto value = readBytes(sizeof(Digest));
        Digest result{};
        std::copy(value.begin(), value.end(), reinterpret_cast<char *>(result.data()));
        return result;
    }

    bool atEnd() const noexcept { return position == bytes.size(); }
    size_t getPosition() const noexcept { return position; }

private:
    std::string_view bytes;
    size_t position = 0;
};

bool isDatabaseApplicable(ResourceLimit limit)
{
    static const ResourceLimitLayer defaults = makeDatabaseDefaultResourceLimitLayer();
    return defaults.contains(limit);
}

UInt16 databaseApplicableCount()
{
    static const UInt16 count = []
    {
        static_assert(resource_limit_count <= std::numeric_limits<UInt16>::max());
        UInt16 result = 0;
        for (size_t index = 0; index < resource_limit_count; ++index)
            if (isDatabaseApplicable(static_cast<ResourceLimit>(index)))
                ++result;
        return result;
    }();
    return count;
}

void validateCompleteDatabaseLayer(const ResourceLimitLayer & layer)
{
    if (layer.getKind() != ResourceLimitLayerKind::Database)
        settingsFail(DatabaseResourceQuotaSettingsError::Code::InvalidConfiguration, "resource quota override is not a Database layer");

    const auto & implementation = getResourceImplementationLimits();
    for (size_t index = 0; index < resource_limit_count; ++index)
    {
        const auto limit = static_cast<ResourceLimit>(index);
        const bool expected = isDatabaseApplicable(limit);
        if (layer.contains(limit) != expected)
            settingsFail(
                DatabaseResourceQuotaSettingsError::Code::InvalidConfiguration,
                expected ? "database resource quota override omits an applicable limit"
                         : "database resource quota override contains a non-database limit");
        if (expected && *layer.get(limit) > implementation.get(limit))
            settingsFail(
                DatabaseResourceQuotaSettingsError::Code::InvalidConfiguration,
                "database resource quota override exceeds an implementation maximum");
    }
}

void validateLayerKeys(const Poco::Util::AbstractConfiguration & config, const String & prefix, const ResourceLimitLayer & applicable_layer)
{
    if (!config.has(prefix))
        return;
    std::vector<String> keys;
    config.keys(prefix, keys);
    for (const auto & key : keys)
    {
        bool known = false;
        for (size_t index = 0; index < resource_limit_count; ++index)
        {
            const auto limit = static_cast<ResourceLimit>(index);
            if (applicable_layer.contains(limit) && key == resourceLimitName(limit))
            {
                known = true;
                break;
            }
        }
        if (!known)
            settingsFail(DatabaseResourceQuotaSettingsError::Code::InvalidConfiguration, "resource limit configuration has an unknown key");
    }
}

ResourceLimitLayer readCompleteLayer(const Poco::Util::AbstractConfiguration & config, const String & prefix, ResourceLimitLayer result)
{
    validateLayerKeys(config, prefix, result);
    const auto & implementation = getResourceImplementationLimits();
    for (size_t index = 0; index < resource_limit_count; ++index)
    {
        const auto limit = static_cast<ResourceLimit>(index);
        const auto fallback = result.get(limit);
        if (!fallback)
            continue;
        const UInt64 value = config.getUInt64(prefix + "." + String(resourceLimitName(limit)), *fallback);
        if (value == 0 || value > implementation.get(limit))
            settingsFail(
                DatabaseResourceQuotaSettingsError::Code::InvalidConfiguration,
                "configured resource limit is zero or exceeds its implementation maximum");
        result.set(limit, value);
    }
    return result;
}

void validateCanonicalDatabaseOverrideSelectors(
    const Poco::Util::AbstractConfiguration & config, const String & overrides_root, std::vector<String> & selectors)
{
    if (!config.has(overrides_root))
        return;
    config.keys(overrides_root, selectors);
    for (const auto & selector : selectors)
    {
        UUID parsed = UUIDHelpers::Nil;
        if (!tryParseUUID({reinterpret_cast<const UInt8 *>(selector.data()), selector.size()}, parsed) || toString(parsed) != selector)
        {
            settingsFail(
                DatabaseResourceQuotaSettingsError::Code::InvalidConfiguration,
                "database resource quota override selector must be one canonical database UUID");
        }
    }
}

}

DatabaseResourceQuotaSettingsError::DatabaseResourceQuotaSettingsError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

String encodeDatabaseResourceQuotaOverrideV2(UUID database_uuid, const ResourceLimitLayer & database_layer)
{
    if (database_uuid == UUIDHelpers::Nil)
        settingsFail(DatabaseResourceQuotaSettingsError::Code::InvalidConfiguration, "database resource quota override UUID is nil");
    validateCompleteDatabaseLayer(database_layer);

    String result;
    result.reserve(
        quota_override_magic.size() + 3 * sizeof(UInt16) + sizeof(CanonicalUUID)
        + static_cast<size_t>(databaseApplicableCount()) * (sizeof(UInt16) + sizeof(UInt64)) + sizeof(Digest));
    result.append(quota_override_magic);
    appendUInt16LE(result, database_resource_quota_override_format_version);
    appendUInt16LE(result, resource_limit_contract_abi);
    appendUUID(result, database_uuid);
    appendUInt16LE(result, databaseApplicableCount());
    for (size_t index = 0; index < resource_limit_count; ++index)
    {
        const auto limit = static_cast<ResourceLimit>(index);
        if (!isDatabaseApplicable(limit))
            continue;
        appendUInt16LE(result, static_cast<UInt16>(limit));
        appendUInt64LE(result, *database_layer.get(limit));
    }
    appendDigest(result, hashFramedDomainSeparated(database_resource_quota_override_hash_domain, result));
    return result;
}

ResourceLimitLayer decodeDatabaseResourceQuotaOverrideV2(std::string_view bytes, UUID expected_database_uuid)
{
    if (expected_database_uuid == UUIDHelpers::Nil)
        settingsFail(DatabaseResourceQuotaSettingsError::Code::InvalidConfiguration, "expected database resource quota UUID is nil");

    QuotaOverrideReader reader(bytes);
    if (reader.readBytes(quota_override_magic.size()) != quota_override_magic)
        settingsFail(DatabaseResourceQuotaSettingsError::Code::InvalidFormat, "database resource quota override magic differs");
    if (reader.readUInt16LE() != database_resource_quota_override_format_version)
        settingsFail(DatabaseResourceQuotaSettingsError::Code::InvalidFormat, "database resource quota override version is unsupported");
    if (reader.readUInt16LE() != resource_limit_contract_abi)
        settingsFail(
            DatabaseResourceQuotaSettingsError::Code::InvalidFormat, "database resource quota override resource ABI is unsupported");
    if (reader.readUUID() != expected_database_uuid)
        settingsFail(
            DatabaseResourceQuotaSettingsError::Code::DatabaseMismatch, "database resource quota override belongs to another database");

    const UInt16 count = reader.readUInt16LE();
    if (count != databaseApplicableCount())
        settingsFail(
            DatabaseResourceQuotaSettingsError::Code::InvalidFormat, "database resource quota override has an incomplete limit set");

    ResourceLimitLayer result(ResourceLimitLayerKind::Database);
    std::optional<UInt16> previous;
    const auto & implementation = getResourceImplementationLimits();
    for (UInt16 index = 0; index < count; ++index)
    {
        const UInt16 encoded_limit = reader.readUInt16LE();
        if (previous && encoded_limit <= *previous)
            settingsFail(
                DatabaseResourceQuotaSettingsError::Code::InvalidFormat, "database resource quota limits are not strictly ordered");
        previous = encoded_limit;
        if (encoded_limit >= resource_limit_count)
            settingsFail(DatabaseResourceQuotaSettingsError::Code::InvalidFormat, "database resource quota limit identity is unknown");
        const auto limit = static_cast<ResourceLimit>(encoded_limit);
        if (!isDatabaseApplicable(limit))
            settingsFail(DatabaseResourceQuotaSettingsError::Code::InvalidFormat, "database resource quota contains a non-database limit");
        const UInt64 value = reader.readUInt64LE();
        if (value == 0 || value > implementation.get(limit))
            settingsFail(
                DatabaseResourceQuotaSettingsError::Code::InvalidFormat,
                "database resource quota value is zero or exceeds its implementation maximum");
        result.set(limit, value);
    }

    const size_t digest_offset = reader.getPosition();
    const Digest checksum = reader.readDigest();
    if (!reader.atEnd())
        settingsFail(DatabaseResourceQuotaSettingsError::Code::InvalidFormat, "database resource quota override has trailing bytes");
    if (checksum != hashFramedDomainSeparated(database_resource_quota_override_hash_domain, bytes.substr(0, digest_offset)))
        settingsFail(DatabaseResourceQuotaSettingsError::Code::ChecksumMismatch, "database resource quota override checksum differs");
    validateCompleteDatabaseLayer(result);
    return result;
}

ResolvedDatabaseResourceQuotaConfiguration resolveDatabaseResourceQuotaConfigurationFromConfig(
    const Poco::Util::AbstractConfiguration & config, UUID database_uuid, UInt64 detected_memory_bytes)
{
    if (database_uuid == UUIDHelpers::Nil)
        settingsFail(DatabaseResourceQuotaSettingsError::Code::InvalidConfiguration, "database resource quota selector UUID is nil");

    ResourceLimitLayer server_layer(ResourceLimitLayerKind::Server);
    try
    {
        server_layer = readCompleteLayer(
            config, "user_defined_types.resource_limits.server", makeServerDefaultResourceLimitLayer(detected_memory_bytes));
    }
    catch (const ResourceLimitError & error)
    {
        settingsFail(DatabaseResourceQuotaSettingsError::Code::InvalidConfiguration, error.what());
    }

    std::optional<String> encoded_database_override;
    static const String overrides_root = "user_defined_types.resource_limits.database_overrides";
    std::vector<String> selectors;
    validateCanonicalDatabaseOverrideSelectors(config, overrides_root, selectors);
    const String selector = toString(database_uuid);
    if (std::find(selectors.begin(), selectors.end(), selector) != selectors.end())
    {
        ResourceLimitLayer database_layer(ResourceLimitLayerKind::Database);
        try
        {
            database_layer = readCompleteLayer(config, overrides_root + "." + selector, makeDatabaseDefaultResourceLimitLayer());
        }
        catch (const ResourceLimitError & error)
        {
            settingsFail(DatabaseResourceQuotaSettingsError::Code::InvalidConfiguration, error.what());
        }
        encoded_database_override = encodeDatabaseResourceQuotaOverrideV2(database_uuid, database_layer);
    }

    return {
        .server_layer = std::move(server_layer),
        .encoded_database_override = std::move(encoded_database_override),
    };
}

EffectiveResourceLimits calculateEffectiveDatabaseResourceLimits(
    const ResourceLimitLayer & server_layer, const ResourceLimitLayer & database_layer, const TypeAuthorityLimits & authority_limits)
{
    if (server_layer.getKind() != ResourceLimitLayerKind::Server)
        settingsFail(DatabaseResourceQuotaSettingsError::Code::InvalidConfiguration, "resource quota server layer has the wrong kind");
    validateCompleteDatabaseLayer(database_layer);
    try
    {
        const std::array<ResourceLimitLayer, 3> layers{
            server_layer,
            database_layer,
            makeAuthorityResourceLimitLayer(authority_limits),
        };
        return calculateEffectiveResourceLimits(layers);
    }
    catch (const ResourceLimitError & error)
    {
        settingsFail(DatabaseResourceQuotaSettingsError::Code::InvalidConfiguration, error.what());
    }
}

}
