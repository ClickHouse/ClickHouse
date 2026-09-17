#include <Databases/UDT/AtomicDatabaseSchemaMutationStorage.h>
#include <Databases/UDT/AuthorityRoot.h>
#include <Databases/UDT/AuthorityVerificationBatchExecutor.h>
#include <Databases/UDT/AuthorityVerificationScheduler.h>
#include <Databases/UDT/DatabaseResourceQuotaSettings.h>

#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>
#include <DataTypes/UDT/Record.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>
#include <DataTypes/UDT/isUDTResourceOrControlExceptionCode.h>

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/Stopwatch.h>
#include <Common/escapeForFileName.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <set>
#include <string>
#include <tuple>
#include <utility>

namespace DB::FailPoints
{
extern const char udt_schema_storage_temp_write_failure[];
extern const char udt_schema_storage_temp_sync_failure[];
extern const char udt_schema_storage_temp_rename_failure[];
}

namespace DB::UDT
{
namespace
{

using StorageError = AtomicDatabaseSchemaMutationStorageError;

constexpr std::string_view staged_magic = "CHUDTSA1";
constexpr std::string_view high_water_magic = "CHUDTHW1";
constexpr std::string_view activation_marker_magic = "CHUDTAM1";
constexpr std::string_view activation_marker_v2_magic = "CHUDTAM2";
constexpr std::string_view recovery_magic = "CHUDTRC1";
constexpr std::string_view verification_cursor_magic = "CHUDTVC1";
constexpr std::string_view udt_configuration_magic = "CHUDTCF2";
constexpr std::string_view retired_checkpoint_image_prefix = "checkpoint-";
constexpr UInt64 checkpoint_transaction_interval = 1024;
constexpr UInt64 maximum_checkpoint_record_bytes = 4ULL << 10;
/// Permanent V1 preimage domains. The exact envelope/decision bytes are part
/// of restart compatibility even though only this backend interprets them.
constexpr size_t internal_record_overhead = 256;
constexpr UInt64 maximum_udt_configuration_component_bytes = 64ULL << 10;
constexpr UInt64 maximum_udt_configuration_record_bytes = 2 * maximum_udt_configuration_component_bytes + internal_record_overhead;
constexpr UInt8 udt_configuration_scheduler_flag = UInt8{1} << 0;
constexpr UInt8 udt_configuration_quota_flag = UInt8{1} << 1;
constexpr UInt8 udt_configuration_supported_flags = udt_configuration_scheduler_flag | udt_configuration_quota_flag;
constexpr std::array<std::string_view, 6> transaction_file_removal_order{
    "recovery.bin.recovery.tmp",
    "commit.wal.commit.tmp",
    "prepare.wal.prepare.tmp",
    "recovery.bin",
    "commit.wal",
    "prepare.wal",
};
constexpr UInt8 transaction_prepare_file_bit = UInt8{1} << 5;
constexpr UInt8 transaction_prepare_temporary_file_bit = UInt8{1} << 2;
constexpr UInt8 transaction_commit_file_bit = UInt8{1} << 4;
constexpr UInt8 transaction_recovery_file_bit = UInt8{1} << 3;

[[noreturn]] void storageFail(StorageError::Code code, std::string_view message)
{
    throw StorageError(code, message);
}

[[noreturn]] void replayConflict(std::string_view message)
{
    throw DatabaseSchemaMutationReplayConflictError(message);
}

struct VerificationTargetBudgetMismatch
{
};

bool verificationPassBudgetExpired(const AuthorityVerificationPassBudget & budget) noexcept
{
    if (budget.cancellation.stop_requested())
        return true;
    if (budget.monotonic_deadline && std::chrono::steady_clock::now() >= *budget.monotonic_deadline)
        return true;
    return budget.thread_cpu_deadline_nanoseconds && clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID) >= *budget.thread_cpu_deadline_nanoseconds;
}

String joinPath(std::string_view lhs, std::string_view rhs)
{
    if (lhs.empty())
        return String(rhs);
    if (rhs.empty())
        return String(lhs);
    String result(lhs);
    if (result.back() != '/')
        result.push_back('/');
    result.append(rhs);
    return result;
}

String parentPath(std::string_view path)
{
    return std::filesystem::path(path).parent_path().generic_string();
}

String fixedWidthNumber(UInt64 value)
{
    std::array<char, 20> digits{};
    const auto [end, error] = std::to_chars(digits.data(), digits.data() + digits.size(), value);
    if (error != std::errc{})
        storageFail(StorageError::Code::UnsafePath, "durable identifier cannot be formatted");
    const size_t size = static_cast<size_t>(end - digits.data());
    String result(digits.size() - size, '0');
    result.append(digits.data(), size);
    return result;
}

String fixedWidthID(UInt64 value)
{
    if (value == 0)
        storageFail(StorageError::Code::UnsafePath, "zero durable identifier has no storage path");
    return fixedWidthNumber(value);
}

std::optional<UInt64> parseFixedWidthNumber(std::string_view value) noexcept
{
    if (value.size() != 20 || !std::all_of(value.begin(), value.end(), [](char byte) { return byte >= '0' && byte <= '9'; }))
        return std::nullopt;
    UInt64 result = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
    if (error != std::errc{} || end != value.data() + value.size())
        return std::nullopt;
    return result;
}

std::optional<UInt64> parseFixedWidthID(std::string_view value) noexcept
{
    const auto result = parseFixedWidthNumber(value);
    if (!result || *result == 0)
        return std::nullopt;
    return result;
}

std::optional<UInt64> parseRetiredCheckpointImageName(std::string_view value) noexcept
{
    if (!value.starts_with(retired_checkpoint_image_prefix) || value.size() != retired_checkpoint_image_prefix.size() + 20)
        return std::nullopt;
    return parseFixedWidthID(value.substr(retired_checkpoint_image_prefix.size()));
}

struct StagedArtifactFileName
{
    UInt64 ordinal = 0;
    bool temporary = false;
};

std::optional<StagedArtifactFileName> parseStagedArtifactName(std::string_view name) noexcept
{
    constexpr std::string_view prefix = "artifact-";
    constexpr std::string_view durable_suffix = ".bin";
    constexpr std::string_view temporary_suffix = ".bin.stage.tmp";
    if (!name.starts_with(prefix))
        return std::nullopt;
    const bool temporary = name.ends_with(temporary_suffix);
    const size_t suffix_size = temporary ? temporary_suffix.size() : durable_suffix.size();
    if ((!name.ends_with(durable_suffix) && !name.ends_with(temporary_suffix)) || name.size() != prefix.size() + 20 + suffix_size)
        return std::nullopt;
    const auto ordinal = parseFixedWidthNumber(name.substr(prefix.size(), 20));
    if (!ordinal)
        return std::nullopt;
    return StagedArtifactFileName{.ordinal = *ordinal, .temporary = temporary};
}

bool isCanonicalStagedArtifactName(std::string_view name) noexcept
{
    return parseStagedArtifactName(name).has_value();
}

std::optional<UInt64> parseHighWaterTemporaryName(std::string_view name) noexcept
{
    constexpr std::string_view prefix = "transaction_high_water.bin.";
    constexpr std::string_view suffix = ".tmp";
    if (!name.starts_with(prefix) || !name.ends_with(suffix) || name.size() != prefix.size() + 20 + suffix.size())
        return std::nullopt;
    const auto transaction_id = parseFixedWidthID(name.substr(prefix.size(), 20));
    if (!transaction_id)
        return std::nullopt;
    return transaction_id;
}

String uuidPathComponent(UUID uuid)
{
    if (uuid == UUIDHelpers::Nil)
        storageFail(StorageError::Code::UnsafePath, "nil UUID has no storage path");
    static constexpr std::array<char, 16> hex{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    const auto bytes = uuidToCanonicalBytes(uuid);
    String result;
    result.reserve(36);
    for (size_t index = 0; index < bytes.size(); ++index)
    {
        if (index == 4 || index == 6 || index == 8 || index == 10)
            result.push_back('-');
        result.push_back(hex[bytes[index] >> 4]);
        result.push_back(hex[bytes[index] & 0x0f]);
    }
    return result;
}

String normalizeMetadataRoot(String root)
{
    if (root.empty() || root.find('\0') != String::npos)
        storageFail(StorageError::Code::UnsafePath, "Atomic schema metadata root is empty or contains NUL");

    /// DatabaseOnDisk keeps directory paths in their conventional trailing-
    /// separator form. The separator names the same disk-relative directory
    /// and must not be mistaken for an empty path component.
    while (root.ends_with('/'))
        root.pop_back();
    if (root.empty())
        storageFail(StorageError::Code::UnsafePath, "Atomic schema metadata root is empty");

    const std::filesystem::path parsed(root);
    if (parsed.is_absolute() || parsed.has_root_name() || parsed.has_root_directory())
        storageFail(StorageError::Code::UnsafePath, "Atomic schema metadata root must be disk-relative");
    for (const auto & component : parsed)
    {
        if (component.empty() || component == "." || component == "..")
            storageFail(StorageError::Code::UnsafePath, "Atomic schema metadata root contains an unsafe component");
    }
    const String normalized = parsed.lexically_normal().generic_string();
    if (normalized.empty() || normalized == "." || normalized.starts_with("../") || normalized.find("/../") != String::npos)
        storageFail(StorageError::Code::UnsafePath, "Atomic schema metadata root escapes the disk namespace");
    return normalized;
}

void appendUInt8(String & output, UInt8 value)
{
    output.push_back(static_cast<char>(value));
}

void appendUInt16LE(String & output, UInt16 value)
{
    for (size_t index = 0; index < sizeof(value); ++index)
        output.push_back(static_cast<char>(value >> (8 * index)));
}

void appendUInt32LE(String & output, UInt32 value)
{
    for (size_t index = 0; index < sizeof(value); ++index)
        output.push_back(static_cast<char>(value >> (8 * index)));
}

void appendUInt64LE(String & output, UInt64 value)
{
    for (size_t index = 0; index < sizeof(value); ++index)
        output.push_back(static_cast<char>(value >> (8 * index)));
}

void appendUUID(String & output, UUID value)
{
    const auto bytes = uuidToCanonicalBytes(value);
    output.append(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}

void appendDigest(String & output, const Digest & value)
{
    output.append(reinterpret_cast<const char *>(value.data()), value.size());
}

UInt64 checkedConfigurationSizeAdd(UInt64 lhs, UInt64 rhs, StorageError::Code code, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        storageFail(code, message);
    return lhs + rhs;
}

UInt64 checkedStorageSizeMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        storageFail(StorageError::Code::LimitExceeded, message);
    return lhs * rhs;
}

UInt32 checkedConfigurationComponentSize(std::string_view bytes, StorageError::Code code)
{
    if (!std::in_range<UInt32>(bytes.size()) || bytes.size() > maximum_udt_configuration_component_bytes)
        storageFail(code, "Atomic UDT configuration component exceeds its byte limit");
    return static_cast<UInt32>(bytes.size());
}

void validateConfigurationComponentPayloads(
    const AtomicDatabaseUDTPersistedConfigurationV2 & configuration, UUID expected_database_uuid, StorageError::Code code)
{
    if (configuration.verification_scheduler_override)
    {
        try
        {
            static_cast<void>(
                decodeAuthorityVerificationSchedulerOverrideV2(*configuration.verification_scheduler_override, expected_database_uuid));
        }
        catch (const AuthorityVerificationScheduleError &)
        {
            storageFail(code, "Atomic UDT configuration has an invalid verification scheduler override");
        }
    }
    if (configuration.resource_quota_override)
    {
        try
        {
            static_cast<void>(decodeDatabaseResourceQuotaOverrideV2(*configuration.resource_quota_override, expected_database_uuid));
        }
        catch (const DatabaseResourceQuotaSettingsError &)
        {
            storageFail(code, "Atomic UDT configuration has an invalid database resource quota override");
        }
    }
}

class InternalReader final
{
public:
    explicit InternalReader(std::string_view input_)
        : input(input_)
    {
    }

    std::string_view readBytes(size_t size)
    {
        if (size > input.size() - position)
            storageFail(StorageError::Code::CorruptDurableState, "Atomic schema internal record is truncated");
        const auto result = input.substr(position, size);
        position += size;
        return result;
    }

    UInt8 readUInt8() { return static_cast<UInt8>(readBytes(1).front()); }

    UInt16 readUInt16LE()
    {
        const auto bytes = readBytes(sizeof(UInt16));
        UInt16 result = 0;
        for (size_t index = 0; index < sizeof(UInt16); ++index)
            result |= static_cast<UInt16>(static_cast<UInt8>(bytes[index])) << (8 * index);
        return result;
    }

    UInt32 readUInt32LE()
    {
        const auto bytes = readBytes(sizeof(UInt32));
        UInt32 result = 0;
        for (size_t index = 0; index < sizeof(UInt32); ++index)
            result |= static_cast<UInt32>(static_cast<UInt8>(bytes[index])) << (8 * index);
        return result;
    }

    UInt64 readUInt64LE()
    {
        const auto bytes = readBytes(sizeof(UInt64));
        UInt64 result = 0;
        for (size_t index = 0; index < sizeof(UInt64); ++index)
            result |= static_cast<UInt64>(static_cast<UInt8>(bytes[index])) << (8 * index);
        return result;
    }

    UUID readUUID()
    {
        CanonicalUUID bytes{};
        const auto source = readBytes(bytes.size());
        std::copy(source.begin(), source.end(), reinterpret_cast<char *>(bytes.data()));
        return uuidFromCanonicalBytes(bytes);
    }

    Digest readDigest()
    {
        Digest result{};
        const auto source = readBytes(result.size());
        std::copy(source.begin(), source.end(), reinterpret_cast<char *>(result.data()));
        return result;
    }

    void requireEnd() const
    {
        if (position != input.size())
            storageFail(StorageError::Code::CorruptDurableState, "Atomic schema internal record has trailing bytes");
    }

private:
    std::string_view input;
    size_t position = 0;
};

struct StagedEnvelope
{
    DatabaseSchemaWALStagedArtifactLocator locator;
    DatabaseSchemaWALStagedArtifactRef artifact;
    String canonical_bytes;
};

bool isOrdinaryDependentObjectKind(SchemaObjectKind kind) noexcept
{
    return kind == SchemaObjectKind::Table || kind == SchemaObjectKind::View || kind == SchemaObjectKind::Dictionary;
}

void validateSupportedArtifact(UUID database_uuid, DatabaseSchemaWALStagedArtifactKind kind, const SchemaObjectID & object)
{
    if (!object.isValid() || object.database_uuid != database_uuid)
        replayConflict("schema-mutation artifact belongs to another database or has an invalid identity");
    bool supported_kind = false;
    switch (kind)
    {
        case DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord:
        case DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord:
        case DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata:
        case DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar:
        case DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord: supported_kind = true; break;
    }
    if (!supported_kind)
        replayConflict("schema-mutation artifact uses an unknown artifact kind");
    switch (object.kind)
    {
        case SchemaObjectKind::TypeDefinition:
            if (kind != DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord)
                replayConflict("type-definition object uses a non-definition artifact kind");
            return;
        case SchemaObjectKind::SyntheticTestObject:
            if (kind == DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord)
                replayConflict("synthetic object uses the type-definition artifact kind");
            if (kind == DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord)
                replayConflict("synthetic object uses a real-object metadata installation record");
            return;
        case SchemaObjectKind::Table:
        case SchemaObjectKind::View:
        case SchemaObjectKind::Dictionary:
            if (kind == DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord)
                replayConflict("dependent object uses the type-definition artifact kind");
            return;
    }
    replayConflict("schema-mutation artifact uses an unknown object kind");
}

String encodeStagedEnvelope(
    const DatabaseSchemaWALStagedArtifactLocator & locator,
    const DatabaseSchemaWALStagedArtifactRef & artifact,
    std::string_view canonical_bytes)
{
    validateSupportedArtifact(locator.database_uuid, artifact.kind, artifact.object);
    if (locator.transaction_id == 0 || artifact.byte_size != canonical_bytes.size()
        || artifact.content_hash != computeDatabaseSchemaWALStagedArtifactHash(artifact.kind, canonical_bytes))
        replayConflict("staged artifact reference does not address the supplied canonical bytes");

    String prefix(staged_magic);
    appendUUID(prefix, locator.database_uuid);
    appendUInt64LE(prefix, locator.transaction_id);
    appendUInt64LE(prefix, locator.ordinal);
    appendUInt8(prefix, static_cast<UInt8>(artifact.kind));
    appendUInt8(prefix, static_cast<UInt8>(artifact.image));
    appendUInt8(prefix, static_cast<UInt8>(artifact.object.kind));
    appendUUID(prefix, artifact.object.database_uuid);
    appendUUID(prefix, artifact.object.object_uuid);
    appendUInt64LE(prefix, artifact.revision);
    appendUInt64LE(prefix, artifact.byte_size);
    appendDigest(prefix, artifact.content_hash);
    prefix.append(canonical_bytes);
    const Digest checksum = hashFramedDomainSeparated(atomic_database_schema_mutation_staged_artifact_hash_domain, prefix);
    appendDigest(prefix, checksum);
    return prefix;
}

StagedEnvelope decodeStagedEnvelope(std::string_view bytes, UInt64 maximum_artifact_bytes)
{
    if (bytes.size() < staged_magic.size() + sizeof(Digest))
        storageFail(StorageError::Code::CorruptDurableState, "staged artifact envelope is truncated");
    const auto prefix = bytes.substr(0, bytes.size() - sizeof(Digest));
    InternalReader checksum_reader(bytes.substr(bytes.size() - sizeof(Digest)));
    const Digest stored_checksum = checksum_reader.readDigest();
    checksum_reader.requireEnd();
    if (stored_checksum != hashFramedDomainSeparated(atomic_database_schema_mutation_staged_artifact_hash_domain, prefix))
        storageFail(StorageError::Code::CorruptDurableState, "staged artifact envelope checksum differs");

    InternalReader reader(prefix);
    if (reader.readBytes(staged_magic.size()) != staged_magic)
        storageFail(StorageError::Code::CorruptDurableState, "staged artifact envelope magic differs");
    StagedEnvelope result;
    result.locator.database_uuid = reader.readUUID();
    result.locator.transaction_id = reader.readUInt64LE();
    result.locator.ordinal = reader.readUInt64LE();
    result.artifact.kind = static_cast<DatabaseSchemaWALStagedArtifactKind>(reader.readUInt8());
    result.artifact.image = static_cast<DatabaseSchemaWALStagedArtifactImage>(reader.readUInt8());
    result.artifact.object.kind = static_cast<SchemaObjectKind>(reader.readUInt8());
    result.artifact.object.database_uuid = reader.readUUID();
    result.artifact.object.object_uuid = reader.readUUID();
    result.artifact.revision = reader.readUInt64LE();
    result.artifact.byte_size = reader.readUInt64LE();
    result.artifact.content_hash = reader.readDigest();
    if (result.artifact.byte_size > maximum_artifact_bytes)
        storageFail(StorageError::Code::LimitExceeded, "staged artifact envelope exceeds its byte limit");
    if (!std::in_range<size_t>(result.artifact.byte_size))
        storageFail(StorageError::Code::LimitExceeded, "staged artifact envelope does not fit the host size type");
    result.canonical_bytes = reader.readBytes(static_cast<size_t>(result.artifact.byte_size));
    reader.requireEnd();
    if (result.locator.database_uuid == UUIDHelpers::Nil || result.locator.transaction_id == 0 || result.artifact.revision == 0)
        storageFail(StorageError::Code::CorruptDurableState, "staged artifact envelope has an invalid identity");
    if (result.artifact.image != DatabaseSchemaWALStagedArtifactImage::Before
        && result.artifact.image != DatabaseSchemaWALStagedArtifactImage::After)
        storageFail(StorageError::Code::CorruptDurableState, "staged artifact envelope image tag is invalid");
    Digest computed_content_hash{};
    try
    {
        validateSupportedArtifact(result.locator.database_uuid, result.artifact.kind, result.artifact.object);
        computed_content_hash = computeDatabaseSchemaWALStagedArtifactHash(result.artifact.kind, result.canonical_bytes);
    }
    catch (const DatabaseSchemaMutationReplayConflictError &)
    {
        storageFail(StorageError::Code::CorruptDurableState, "staged artifact envelope kind and object identity are invalid");
    }
    catch (const DatabaseSchemaWALError &)
    {
        storageFail(StorageError::Code::CorruptDurableState, "staged artifact envelope kind is invalid");
    }
    if (result.artifact.content_hash != computed_content_hash)
        storageFail(StorageError::Code::CorruptDurableState, "staged artifact envelope content address differs");
    return result;
}

String encodeHighWaterMarkInternal(UUID database_uuid, UInt64 transaction_id)
{
    String prefix(high_water_magic);
    appendUUID(prefix, database_uuid);
    appendUInt64LE(prefix, transaction_id);
    appendDigest(prefix, hashFramedDomainSeparated(atomic_database_schema_mutation_high_water_hash_domain, prefix));
    return prefix;
}

UInt64 decodeHighWaterMarkInternal(std::string_view bytes, UUID expected_database_uuid)
{
    if (bytes.size() != high_water_magic.size() + CanonicalUUID{}.size() + sizeof(UInt64) + sizeof(Digest))
        storageFail(StorageError::Code::CorruptDurableState, "schema-mutation high-water record has the wrong size");
    const auto prefix = bytes.substr(0, bytes.size() - sizeof(Digest));
    InternalReader reader(bytes);
    if (reader.readBytes(high_water_magic.size()) != high_water_magic)
        storageFail(StorageError::Code::CorruptDurableState, "schema-mutation high-water record magic differs");
    if (reader.readUUID() != expected_database_uuid)
        storageFail(StorageError::Code::CorruptDurableState, "schema-mutation high-water record belongs to another database");
    const UInt64 transaction_id = reader.readUInt64LE();
    const Digest checksum = reader.readDigest();
    reader.requireEnd();
    if (checksum != hashFramedDomainSeparated(atomic_database_schema_mutation_high_water_hash_domain, prefix))
        storageFail(StorageError::Code::CorruptDurableState, "schema-mutation high-water record checksum differs");
    return transaction_id;
}

String encodeActivationMarkerInternal(UUID database_uuid, UInt64 activation_transaction_id)
{
    String prefix(activation_marker_magic);
    appendUUID(prefix, database_uuid);
    appendUInt64LE(prefix, activation_transaction_id);
    appendDigest(prefix, hashFramedDomainSeparated(authority_activation_marker_hash_domain, prefix));
    return prefix;
}

struct ActivationMarkerState
{
    UInt64 activation_transaction_id = 0;
    bool requires_combined_configuration = false;
};

String encodeActivationMarkerV2Internal(UUID database_uuid, UInt64 activation_transaction_id)
{
    String prefix(activation_marker_v2_magic);
    appendUUID(prefix, database_uuid);
    appendUInt64LE(prefix, activation_transaction_id);
    appendUInt16LE(prefix, atomic_database_udt_configuration_format_version);
    appendDigest(prefix, hashFramedDomainSeparated(authority_activation_marker_v2_hash_domain, prefix));
    return prefix;
}

ActivationMarkerState decodeActivationMarkerInternal(std::string_view bytes, UUID expected_database_uuid)
{
    const bool is_v1 = bytes.starts_with(activation_marker_magic);
    const bool is_v2 = bytes.starts_with(activation_marker_v2_magic);
    const size_t expected_size
        = activation_marker_magic.size() + CanonicalUUID{}.size() + sizeof(UInt64) + (is_v2 ? sizeof(UInt16) : 0) + sizeof(Digest);
    if ((!is_v1 && !is_v2) || bytes.size() != expected_size)
        storageFail(StorageError::Code::CorruptDurableState, "Atomic authority activation marker has the wrong size");
    const auto prefix = bytes.substr(0, bytes.size() - sizeof(Digest));
    InternalReader reader(bytes);
    static_cast<void>(reader.readBytes(activation_marker_magic.size()));
    if (reader.readUUID() != expected_database_uuid)
        storageFail(StorageError::Code::CorruptDurableState, "Atomic authority activation marker belongs to another database");
    const UInt64 activation_transaction_id = reader.readUInt64LE();
    if (is_v2 && reader.readUInt16LE() != atomic_database_udt_configuration_format_version)
        storageFail(StorageError::Code::CorruptDurableState, "Atomic authority activation marker configuration format differs");
    const Digest checksum = reader.readDigest();
    reader.requireEnd();
    const auto hash_domain = is_v2 ? authority_activation_marker_v2_hash_domain : authority_activation_marker_hash_domain;
    if (checksum != hashFramedDomainSeparated(hash_domain, prefix))
        storageFail(StorageError::Code::CorruptDurableState, "Atomic authority activation marker checksum differs");
    return {
        .activation_transaction_id = activation_transaction_id,
        .requires_combined_configuration = is_v2,
    };
}

String encodeUDTConfigurationGenerationInternal(const AtomicDatabaseUDTPersistedConfigurationGenerationV2 & generation)
{
    if (generation.format_version != atomic_database_udt_configuration_format_version || generation.database_uuid == UUIDHelpers::Nil
        || generation.generation == 0)
        storageFail(StorageError::Code::InvalidConfiguration, "Atomic UDT configuration generation identity is invalid");
    validateConfigurationComponentPayloads(generation.configuration, generation.database_uuid, StorageError::Code::InvalidConfiguration);

    const UInt32 scheduler_size = generation.configuration.verification_scheduler_override
        ? checkedConfigurationComponentSize(
              *generation.configuration.verification_scheduler_override, StorageError::Code::InvalidConfiguration)
        : 0;
    const UInt32 quota_size = generation.configuration.resource_quota_override
        ? checkedConfigurationComponentSize(*generation.configuration.resource_quota_override, StorageError::Code::InvalidConfiguration)
        : 0;
    UInt64 encoded_size = udt_configuration_magic.size() + sizeof(UInt16) + sizeof(CanonicalUUID) + sizeof(UInt64) + sizeof(UInt8)
        + 2 * sizeof(UInt32) + sizeof(Digest);
    encoded_size = checkedConfigurationSizeAdd(
        encoded_size, scheduler_size, StorageError::Code::InvalidConfiguration, "Atomic UDT configuration byte count overflows");
    encoded_size = checkedConfigurationSizeAdd(
        encoded_size, quota_size, StorageError::Code::InvalidConfiguration, "Atomic UDT configuration byte count overflows");
    if (encoded_size > maximum_udt_configuration_record_bytes || !std::in_range<size_t>(encoded_size))
        storageFail(StorageError::Code::InvalidConfiguration, "Atomic UDT configuration generation exceeds its byte limit");

    UInt8 flags = 0;
    if (scheduler_size)
        flags |= udt_configuration_scheduler_flag;
    if (quota_size)
        flags |= udt_configuration_quota_flag;

    String prefix;
    prefix.reserve(static_cast<size_t>(encoded_size));
    prefix.append(udt_configuration_magic);
    appendUInt16LE(prefix, generation.format_version);
    appendUUID(prefix, generation.database_uuid);
    appendUInt64LE(prefix, generation.generation);
    appendUInt8(prefix, flags);
    appendUInt32LE(prefix, scheduler_size);
    appendUInt32LE(prefix, quota_size);
    if (generation.configuration.verification_scheduler_override)
        prefix.append(*generation.configuration.verification_scheduler_override);
    if (generation.configuration.resource_quota_override)
        prefix.append(*generation.configuration.resource_quota_override);
    appendDigest(prefix, hashFramedDomainSeparated(atomic_database_udt_configuration_hash_domain, prefix));
    return prefix;
}

AtomicDatabaseUDTPersistedConfigurationGenerationV2
decodeUDTConfigurationGenerationInternal(std::string_view bytes, UUID expected_database_uuid)
{
    constexpr UInt64 fixed_size = udt_configuration_magic.size() + sizeof(UInt16) + sizeof(CanonicalUUID) + sizeof(UInt64) + sizeof(UInt8)
        + 2 * sizeof(UInt32) + sizeof(Digest);
    if (bytes.size() < fixed_size)
        storageFail(StorageError::Code::CorruptDurableState, "Atomic UDT configuration generation is truncated");
    if (bytes.size() > maximum_udt_configuration_record_bytes)
        storageFail(StorageError::Code::LimitExceeded, "Atomic UDT configuration generation exceeds its byte limit");

    const auto prefix = bytes.substr(0, bytes.size() - sizeof(Digest));
    InternalReader checksum_reader(bytes.substr(bytes.size() - sizeof(Digest)));
    const Digest checksum = checksum_reader.readDigest();
    checksum_reader.requireEnd();
    if (checksum != hashFramedDomainSeparated(atomic_database_udt_configuration_hash_domain, prefix))
        storageFail(StorageError::Code::CorruptDurableState, "Atomic UDT configuration generation checksum differs");

    InternalReader reader(prefix);
    if (reader.readBytes(udt_configuration_magic.size()) != udt_configuration_magic)
        storageFail(StorageError::Code::CorruptDurableState, "Atomic UDT configuration generation magic differs");
    AtomicDatabaseUDTPersistedConfigurationGenerationV2 result;
    result.format_version = reader.readUInt16LE();
    result.database_uuid = reader.readUUID();
    result.generation = reader.readUInt64LE();
    const UInt8 flags = reader.readUInt8();
    const UInt32 scheduler_size = reader.readUInt32LE();
    const UInt32 quota_size = reader.readUInt32LE();
    if (result.format_version != atomic_database_udt_configuration_format_version)
        storageFail(StorageError::Code::CorruptDurableState, "Atomic UDT configuration generation version is unsupported");
    if (result.database_uuid != expected_database_uuid || result.database_uuid == UUIDHelpers::Nil || result.generation == 0)
        storageFail(StorageError::Code::CorruptDurableState, "Atomic UDT configuration generation identity differs");
    if ((flags & ~udt_configuration_supported_flags) != 0 || ((flags & udt_configuration_scheduler_flag) != 0) != (scheduler_size != 0)
        || ((flags & udt_configuration_quota_flag) != 0) != (quota_size != 0))
        storageFail(StorageError::Code::CorruptDurableState, "Atomic UDT configuration generation flags are noncanonical");
    if (scheduler_size > maximum_udt_configuration_component_bytes || quota_size > maximum_udt_configuration_component_bytes)
        storageFail(StorageError::Code::LimitExceeded, "Atomic UDT configuration component exceeds its byte limit");
    const UInt64 payload_size = checkedConfigurationSizeAdd(
        scheduler_size, quota_size, StorageError::Code::LimitExceeded, "Atomic UDT configuration payload size overflows");
    if (payload_size > maximum_udt_configuration_record_bytes)
        storageFail(StorageError::Code::LimitExceeded, "Atomic UDT configuration payload exceeds its byte limit");

    if (scheduler_size)
        result.configuration.verification_scheduler_override = String(reader.readBytes(scheduler_size));
    if (quota_size)
        result.configuration.resource_quota_override = String(reader.readBytes(quota_size));
    reader.requireEnd();
    validateConfigurationComponentPayloads(result.configuration, expected_database_uuid, StorageError::Code::CorruptDurableState);
    return result;
}

String encodeRecoveryDecisionInternal(
    UUID database_uuid, UInt64 transaction_id, DatabaseSchemaWALRecoveryDecision decision, const Digest & prepare_hash)
{
    String prefix(recovery_magic);
    appendUUID(prefix, database_uuid);
    appendUInt64LE(prefix, transaction_id);
    appendUInt8(prefix, static_cast<UInt8>(decision));
    appendDigest(prefix, prepare_hash);
    appendDigest(prefix, hashFramedDomainSeparated(atomic_database_schema_mutation_recovery_decision_hash_domain, prefix));
    return prefix;
}

struct RecoveryDecisionEnvelope
{
    UUID database_uuid;
    UInt64 transaction_id;
    DatabaseSchemaWALRecoveryDecision decision;
    Digest prepare_hash;
};

RecoveryDecisionEnvelope decodeRecoveryDecisionEnvelopeInternal(std::string_view bytes)
{
    constexpr size_t expected_size = recovery_magic.size() + CanonicalUUID{}.size() + sizeof(UInt64) + sizeof(UInt8) + 2 * sizeof(Digest);
    if (bytes.size() != expected_size)
        storageFail(StorageError::Code::CorruptDurableState, "schema-mutation recovery record has the wrong size");
    const auto prefix = bytes.substr(0, bytes.size() - sizeof(Digest));
    InternalReader reader(bytes);
    if (reader.readBytes(recovery_magic.size()) != recovery_magic)
        storageFail(StorageError::Code::CorruptDurableState, "schema-mutation recovery record magic differs");
    RecoveryDecisionEnvelope result{
        .database_uuid = reader.readUUID(),
        .transaction_id = reader.readUInt64LE(),
        .decision = static_cast<DatabaseSchemaWALRecoveryDecision>(reader.readUInt8()),
        .prepare_hash = reader.readDigest(),
    };
    if (result.database_uuid == UUIDHelpers::Nil || result.transaction_id == 0)
        storageFail(StorageError::Code::CorruptDurableState, "schema-mutation recovery record identity is invalid");
    if (result.decision != DatabaseSchemaWALRecoveryDecision::RollBackPrepared
        && result.decision != DatabaseSchemaWALRecoveryDecision::CompleteCommitted)
        storageFail(StorageError::Code::CorruptDurableState, "schema-mutation recovery record decision is invalid");
    const Digest checksum = reader.readDigest();
    reader.requireEnd();
    if (checksum != hashFramedDomainSeparated(atomic_database_schema_mutation_recovery_decision_hash_domain, prefix))
        storageFail(StorageError::Code::CorruptDurableState, "schema-mutation recovery record checksum differs");
    return result;
}

DatabaseSchemaWALRecoveryDecision decodeRecoveryDecisionInternal(
    std::string_view bytes, UUID expected_database_uuid, UInt64 expected_transaction_id, const Digest & expected_prepare_hash)
{
    const auto result = decodeRecoveryDecisionEnvelopeInternal(bytes);
    if (result.database_uuid != expected_database_uuid || result.transaction_id != expected_transaction_id)
        storageFail(StorageError::Code::CorruptDurableState, "schema-mutation recovery record identity differs");
    if (result.prepare_hash != expected_prepare_hash)
        storageFail(StorageError::Code::CorruptDurableState, "schema-mutation recovery record binds another Prepare marker");
    return result.decision;
}

UInt8 encodeInventoryRecordKind(AuthorityInventoryRecordKind kind)
{
    switch (kind)
    {
        case AuthorityInventoryRecordKind::TypeDefinition: return 1;
        case AuthorityInventoryRecordKind::SidecarExpectation: return 2;
    }
    storageFail(StorageError::Code::InvalidConfiguration, "authority verification cursor has an unknown inventory record kind");
}

AuthorityInventoryRecordKind decodeInventoryRecordKind(UInt8 value)
{
    switch (value)
    {
        case 1: return AuthorityInventoryRecordKind::TypeDefinition;
        case 2: return AuthorityInventoryRecordKind::SidecarExpectation;
        default: storageFail(StorageError::Code::CorruptDurableState, "authority verification cursor has an unknown inventory record kind");
    }
}

void validateVerificationCursor(const AuthorityVerificationScheduleCursor & cursor, UUID expected_database_uuid, StorageError::Code code)
{
    constexpr AuthorityVerificationScheduleLimits implementation_maxima;
    const bool valid_resume = !cursor.resume_after
        || (cursor.resume_after->format_version == authority_inventory_format_version
            && cursor.resume_after->object_uuid != UUIDHelpers::Nil
            && (cursor.resume_after->record_kind == AuthorityInventoryRecordKind::TypeDefinition
                || cursor.resume_after->record_kind == AuthorityInventoryRecordKind::SidecarExpectation));
    if (cursor.contract_abi != authority_verification_schedule_contract_abi || cursor.database_uuid != expected_database_uuid
        || cursor.database_uuid == UUIDHelpers::Nil || cursor.bucket_count == 0
        || cursor.bucket_count > implementation_maxima.maximum_buckets || cursor.current_bucket >= cursor.bucket_count || !valid_resume)
        storageFail(code, "authority verification cursor identity or bounded state is invalid");
}

String encodeVerificationCursorInternal(const AuthorityVerificationScheduleCursor & cursor)
{
    validateVerificationCursor(cursor, cursor.database_uuid, StorageError::Code::InvalidConfiguration);
    String prefix(verification_cursor_magic);
    appendUInt16LE(prefix, cursor.contract_abi);
    appendUUID(prefix, cursor.database_uuid);
    appendUInt32LE(prefix, cursor.bucket_count);
    appendUInt64LE(prefix, cursor.bucket_seed);
    appendUInt32LE(prefix, cursor.current_bucket);
    appendUInt8(prefix, cursor.resume_after ? 1 : 0);
    if (cursor.resume_after)
    {
        appendUInt16LE(prefix, cursor.resume_after->format_version);
        appendUInt8(prefix, encodeInventoryRecordKind(cursor.resume_after->record_kind));
        appendUUID(prefix, cursor.resume_after->object_uuid);
    }
    appendUInt64LE(prefix, cursor.completed_rotations);
    appendUInt64LE(prefix, cursor.planned_batches);
    appendDigest(prefix, hashFramedDomainSeparated(authority_verification_cursor_hash_domain, prefix));
    return prefix;
}

AuthorityVerificationScheduleCursor decodeVerificationCursorInternal(std::string_view bytes, UUID expected_database_uuid)
{
    constexpr size_t fixed_size = verification_cursor_magic.size() + sizeof(UInt16) + CanonicalUUID{}.size() + sizeof(UInt32)
        + sizeof(UInt64) + sizeof(UInt32) + sizeof(UInt8) + 2 * sizeof(UInt64) + sizeof(Digest);
    constexpr size_t resume_size = sizeof(UInt16) + sizeof(UInt8) + CanonicalUUID{}.size();
    if (bytes.size() != fixed_size && bytes.size() != fixed_size + resume_size)
        storageFail(StorageError::Code::CorruptDurableState, "authority verification cursor has the wrong size");
    const auto prefix = bytes.substr(0, bytes.size() - sizeof(Digest));
    InternalReader reader(bytes);
    if (reader.readBytes(verification_cursor_magic.size()) != verification_cursor_magic)
        storageFail(StorageError::Code::CorruptDurableState, "authority verification cursor magic differs");

    AuthorityVerificationScheduleCursor cursor;
    cursor.contract_abi = reader.readUInt16LE();
    cursor.database_uuid = reader.readUUID();
    cursor.bucket_count = reader.readUInt32LE();
    cursor.bucket_seed = reader.readUInt64LE();
    cursor.current_bucket = reader.readUInt32LE();
    const UInt8 has_resume = reader.readUInt8();
    if (has_resume > 1 || (has_resume == 0 && bytes.size() != fixed_size) || (has_resume == 1 && bytes.size() != fixed_size + resume_size))
        storageFail(StorageError::Code::CorruptDurableState, "authority verification cursor optional key framing is invalid");
    if (has_resume)
    {
        cursor.resume_after = AuthorityInventoryKey{
            .format_version = reader.readUInt16LE(),
            .record_kind = decodeInventoryRecordKind(reader.readUInt8()),
            .object_uuid = reader.readUUID(),
        };
    }
    cursor.completed_rotations = reader.readUInt64LE();
    cursor.planned_batches = reader.readUInt64LE();
    const Digest checksum = reader.readDigest();
    reader.requireEnd();
    if (checksum != hashFramedDomainSeparated(authority_verification_cursor_hash_domain, prefix))
        storageFail(StorageError::Code::CorruptDurableState, "authority verification cursor checksum differs");
    validateVerificationCursor(cursor, expected_database_uuid, StorageError::Code::CorruptDurableState);
    return cursor;
}

bool commitMatchesPrepare(const DatabaseSchemaWALCommit & commit, const DatabaseSchemaWALPrepare & prepare) noexcept
{
    const auto & after = prepare.after_authority_state;
    return commit.transaction_id == prepare.transaction_id && commit.database_uuid == after.database_uuid
        && commit.database_catalog_epoch == after.database_catalog_epoch && commit.inventory_root == after.inventory_root
        && commit.schema_graph_root == after.schema_graph_root && commit.authority_anchor == after.anchor_hash
        && commit.prepare_hash == prepare.prepare_hash;
}

}

String encodeAtomicDatabaseSchemaMutationHighWaterMark(UUID database_uuid, UInt64 transaction_id)
{
    if (database_uuid == UUIDHelpers::Nil || transaction_id == 0)
        storageFail(StorageError::Code::InvalidConfiguration, "schema-mutation high-water identity is invalid");
    return encodeHighWaterMarkInternal(database_uuid, transaction_id);
}

UInt64 decodeAtomicDatabaseSchemaMutationHighWaterMark(std::string_view bytes, UUID expected_database_uuid)
{
    if (expected_database_uuid == UUIDHelpers::Nil)
        storageFail(StorageError::Code::InvalidConfiguration, "schema-mutation high-water database UUID is nil");
    const UInt64 transaction_id = decodeHighWaterMarkInternal(bytes, expected_database_uuid);
    if (transaction_id == 0)
        storageFail(StorageError::Code::CorruptDurableState, "schema-mutation high-water transaction ID is zero");
    return transaction_id;
}

String encodeAuthorityActivationMarker(UUID database_uuid, UInt64 activation_transaction_id)
{
    if (database_uuid == UUIDHelpers::Nil || activation_transaction_id == 0)
        storageFail(StorageError::Code::InvalidConfiguration, "Atomic authority activation identity is invalid");
    return encodeActivationMarkerInternal(database_uuid, activation_transaction_id);
}

UInt64 decodeAuthorityActivationMarker(std::string_view bytes, UUID expected_database_uuid)
{
    if (expected_database_uuid == UUIDHelpers::Nil)
        storageFail(StorageError::Code::InvalidConfiguration, "Atomic authority activation database UUID is nil");
    const UInt64 activation_transaction_id = decodeActivationMarkerInternal(bytes, expected_database_uuid).activation_transaction_id;
    if (activation_transaction_id == 0)
        storageFail(StorageError::Code::CorruptDurableState, "Atomic authority activation transaction ID is zero");
    return activation_transaction_id;
}

String encodeAtomicDatabaseSchemaMutationRecoveryDecision(
    UUID database_uuid, UInt64 transaction_id, DatabaseSchemaWALRecoveryDecision decision, const Digest & prepare_hash)
{
    if (database_uuid == UUIDHelpers::Nil || transaction_id == 0
        || (decision != DatabaseSchemaWALRecoveryDecision::RollBackPrepared
            && decision != DatabaseSchemaWALRecoveryDecision::CompleteCommitted))
        storageFail(StorageError::Code::InvalidConfiguration, "schema-mutation recovery-decision identity is invalid");
    return encodeRecoveryDecisionInternal(database_uuid, transaction_id, decision, prepare_hash);
}

DatabaseSchemaWALRecoveryDecision decodeAtomicDatabaseSchemaMutationRecoveryDecision(
    std::string_view bytes, UUID expected_database_uuid, UInt64 expected_transaction_id, const Digest & expected_prepare_hash)
{
    if (expected_database_uuid == UUIDHelpers::Nil || expected_transaction_id == 0)
        storageFail(StorageError::Code::InvalidConfiguration, "schema-mutation recovery-decision identity is invalid");
    return decodeRecoveryDecisionInternal(bytes, expected_database_uuid, expected_transaction_id, expected_prepare_hash);
}

String encodeAuthorityVerificationScheduleCursor(const AuthorityVerificationScheduleCursor & cursor)
{
    return encodeVerificationCursorInternal(cursor);
}

AuthorityVerificationScheduleCursor decodeAuthorityVerificationScheduleCursor(std::string_view bytes, UUID expected_database_uuid)
{
    if (expected_database_uuid == UUIDHelpers::Nil)
        storageFail(StorageError::Code::InvalidConfiguration, "authority verification cursor expected database UUID is nil");
    return decodeVerificationCursorInternal(bytes, expected_database_uuid);
}

String encodeAtomicDatabaseUDTConfigurationGenerationV2(const AtomicDatabaseUDTPersistedConfigurationGenerationV2 & generation)
{
    return encodeUDTConfigurationGenerationInternal(generation);
}

AtomicDatabaseUDTPersistedConfigurationGenerationV2
decodeAtomicDatabaseUDTConfigurationGenerationV2(std::string_view bytes, UUID expected_database_uuid)
{
    if (expected_database_uuid == UUIDHelpers::Nil)
        storageFail(StorageError::Code::InvalidConfiguration, "Atomic UDT configuration expected database UUID is nil");
    return decodeUDTConfigurationGenerationInternal(bytes, expected_database_uuid);
}

AtomicDatabaseSchemaMutationStorageError::AtomicDatabaseSchemaMutationStorageError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

struct AtomicDatabaseUDTConfigurationCleanupState
{
    std::mutex mutex;
    AtomicDatabaseSchemaMutationStorage * owner = nullptr;
};

AtomicDatabaseSchemaMutationPaths::AtomicDatabaseSchemaMutationPaths(String metadata_root_, UUID database_uuid_, String database_name_)
    : metadata_root(normalizeMetadataRoot(std::move(metadata_root_)))
    , database_uuid(database_uuid_)
    , database_name(std::move(database_name_))
{
    if (database_uuid == UUIDHelpers::Nil)
        storageFail(StorageError::Code::InvalidConfiguration, "Atomic schema storage database UUID is nil");
    if (database_name.find('\0') != String::npos)
        storageFail(StorageError::Code::InvalidConfiguration, "Atomic schema storage database name contains NUL");
}

String AtomicDatabaseSchemaMutationPaths::typesDirectory() const
{
    return joinPath(metadata_root, "types");
}

String AtomicDatabaseSchemaMutationPaths::authorityDirectory() const
{
    return joinPath(joinPath(joinPath(typesDirectory(), ".authority"), "databases"), uuidPathComponent(database_uuid));
}

String AtomicDatabaseSchemaMutationPaths::stagingDirectory() const
{
    return joinPath(authorityDirectory(), "staging");
}

String AtomicDatabaseSchemaMutationPaths::stagingTransactionDirectory(UInt64 transaction_id) const
{
    return joinPath(stagingDirectory(), fixedWidthID(transaction_id));
}

String AtomicDatabaseSchemaMutationPaths::stagedArtifactPath(UInt64 transaction_id, UInt64 ordinal) const
{
    return joinPath(stagingTransactionDirectory(transaction_id), "artifact-" + fixedWidthNumber(ordinal) + ".bin");
}

String AtomicDatabaseSchemaMutationPaths::walDirectory() const
{
    return joinPath(authorityDirectory(), "wal");
}

String AtomicDatabaseSchemaMutationPaths::walTransactionDirectory(UInt64 transaction_id) const
{
    return joinPath(walDirectory(), fixedWidthID(transaction_id));
}

String AtomicDatabaseSchemaMutationPaths::preparePath(UInt64 transaction_id) const
{
    return joinPath(walTransactionDirectory(transaction_id), "prepare.wal");
}

String AtomicDatabaseSchemaMutationPaths::commitPath(UInt64 transaction_id) const
{
    return joinPath(walTransactionDirectory(transaction_id), "commit.wal");
}

String AtomicDatabaseSchemaMutationPaths::recoveryDecisionPath(UInt64 transaction_id) const
{
    return joinPath(walTransactionDirectory(transaction_id), "recovery.bin");
}

String AtomicDatabaseSchemaMutationPaths::retiredDirectory() const
{
    return joinPath(authorityDirectory(), "retired");
}

String AtomicDatabaseSchemaMutationPaths::retiredRollbackDirectory() const
{
    return joinPath(retiredDirectory(), "rollback");
}

String AtomicDatabaseSchemaMutationPaths::retiredRollbackTransactionDirectory(UInt64 transaction_id) const
{
    return joinPath(retiredRollbackDirectory(), fixedWidthID(transaction_id));
}

String AtomicDatabaseSchemaMutationPaths::retiredCheckpointDirectory() const
{
    return joinPath(retiredDirectory(), "checkpoint");
}

String AtomicDatabaseSchemaMutationPaths::retiredCheckpointTransactionDirectory(UInt64 checkpoint_id, UInt64 transaction_id) const
{
    return joinPath(joinPath(retiredCheckpointDirectory(), fixedWidthID(checkpoint_id)), fixedWidthID(transaction_id));
}

String AtomicDatabaseSchemaMutationPaths::retiredCheckpointImageDirectory(UInt64 checkpoint_id) const
{
    return joinPath(retiredCheckpointDirectory(), String(retired_checkpoint_image_prefix) + fixedWidthID(checkpoint_id));
}

String AtomicDatabaseSchemaMutationPaths::checkpointsDirectory() const
{
    return joinPath(authorityDirectory(), "checkpoints");
}

String AtomicDatabaseSchemaMutationPaths::checkpointDirectory(UInt64 checkpoint_id) const
{
    return joinPath(checkpointsDirectory(), fixedWidthID(checkpoint_id));
}

String AtomicDatabaseSchemaMutationPaths::checkpointRecordPath(UInt64 checkpoint_id) const
{
    return joinPath(checkpointDirectory(checkpoint_id), "checkpoint.wal");
}

String AtomicDatabaseSchemaMutationPaths::checkpointInventorySnapshotPath(UInt64 checkpoint_id) const
{
    return joinPath(checkpointDirectory(checkpoint_id), "inventory.snapshot");
}

String AtomicDatabaseSchemaMutationPaths::checkpointSchemaGraphSnapshotPath(UInt64 checkpoint_id) const
{
    return joinPath(checkpointDirectory(checkpoint_id), "schema_graph.snapshot");
}

String AtomicDatabaseSchemaMutationPaths::activationMarkerPath() const
{
    return joinPath(metadata_root, ".udt_activation.bin");
}

String AtomicDatabaseSchemaMutationPaths::activationMarkerTemporaryPath() const
{
    return activationMarkerPath() + ".activation.tmp";
}

String AtomicDatabaseSchemaMutationPaths::verificationCursorPath() const
{
    return joinPath(metadata_root, ".udt_verification_cursor.bin");
}

String AtomicDatabaseSchemaMutationPaths::verificationCursorTemporaryPath() const
{
    return verificationCursorPath() + ".verification.tmp";
}

String AtomicDatabaseSchemaMutationPaths::udtConfigurationV2Path() const
{
    return joinPath(metadata_root, ".udt_configuration_v2.bin");
}

String AtomicDatabaseSchemaMutationPaths::udtConfigurationV2TemporaryPath() const
{
    return udtConfigurationV2Path() + ".configuration-v2.tmp";
}

String AtomicDatabaseSchemaMutationPaths::verificationSchedulerOverrideV2Path() const
{
    return joinPath(metadata_root, ".udt_verification_scheduler_override_v2.bin");
}

String AtomicDatabaseSchemaMutationPaths::verificationSchedulerOverrideV2TemporaryPath() const
{
    return verificationSchedulerOverrideV2Path() + ".scheduler-override-v2.tmp";
}

String AtomicDatabaseSchemaMutationPaths::resourceQuotaOverrideV2Path() const
{
    return joinPath(metadata_root, ".udt_resource_quota_v2.bin");
}

String AtomicDatabaseSchemaMutationPaths::resourceQuotaOverrideV2TemporaryPath() const
{
    return resourceQuotaOverrideV2Path() + ".resource-quota-v2.tmp";
}

String AtomicDatabaseSchemaMutationPaths::highWaterMarkPath() const
{
    return joinPath(authorityDirectory(), "transaction_high_water.bin");
}

String AtomicDatabaseSchemaMutationPaths::authorityRecordPath(const AuthorityInventoryKey & key) const
{
    if (key.format_version != authority_inventory_format_version || key.object_uuid == UUIDHelpers::Nil)
        replayConflict("authority inventory key has no canonical storage path");
    const String uuid = uuidPathComponent(key.object_uuid);
    switch (key.record_kind)
    {
        case AuthorityInventoryRecordKind::TypeDefinition: return joinPath(typesDirectory(), uuid + ".sql");
        case AuthorityInventoryRecordKind::SidecarExpectation:
            return joinPath(joinPath(authorityDirectory(), "expectations"), uuid + ".bin");
    }
    replayConflict("authority inventory key uses an unknown record kind");
}

String AtomicDatabaseSchemaMutationPaths::tableReferencesPath(const SchemaObjectID & object) const
{
    validateSupportedArtifact(database_uuid, DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar, object);
    if (object.kind != SchemaObjectKind::Table && object.kind != SchemaObjectKind::View && object.kind != SchemaObjectKind::Dictionary)
        replayConflict("dependent-object references path requires a durable storage identity");
    return joinPath(joinPath(authorityDirectory(), "expectations"), uuidPathComponent(object.object_uuid) + ".references");
}

String AtomicDatabaseSchemaMutationPaths::metadataInstallationRecordPath(const SchemaObjectID & object) const
{
    validateSupportedArtifact(database_uuid, DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord, object);
    if (object.kind != SchemaObjectKind::Table && object.kind != SchemaObjectKind::View && object.kind != SchemaObjectKind::Dictionary)
        replayConflict("metadata installation path requires a durable storage identity");
    return joinPath(joinPath(authorityDirectory(), "expectations"), uuidPathComponent(object.object_uuid) + ".installation");
}

String AtomicDatabaseSchemaMutationPaths::tableMetadataPath(std::string_view object_name) const
{
    if (object_name.empty() || object_name.find('\0') != std::string_view::npos)
        storageFail(StorageError::Code::UnsafePath, "table metadata name is empty or contains NUL");
    const String escaped_name = escapeForFileName(String(object_name));
    if (unescapeForFileName(escaped_name) != String(object_name))
        storageFail(StorageError::Code::UnsafePath, "table metadata name is not reversibly filesystem-escaped");
    return joinPath(metadata_root, escaped_name + ".sql");
}

String AtomicDatabaseSchemaMutationPaths::droppedTableMetadataPath(std::string_view object_name, UUID object_uuid) const
{
    if (database_name.empty() || object_name.empty() || object_name.find('\0') != std::string_view::npos || object_uuid == UUIDHelpers::Nil)
        storageFail(StorageError::Code::InvalidConfiguration, "dropped table metadata identity is incomplete");
    return joinPath(
        "metadata_dropped",
        escapeForFileName(database_name) + "." + escapeForFileName(String(object_name)) + "." + toString(object_uuid) + ".sql");
}

String
AtomicDatabaseSchemaMutationPaths::canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind kind, const SchemaObjectID & object) const
{
    validateSupportedArtifact(database_uuid, kind, object);
    const String uuid = uuidPathComponent(object.object_uuid);
    switch (kind)
    {
        case DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord: return joinPath(typesDirectory(), uuid + ".sql");
        case DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord:
            return joinPath(joinPath(authorityDirectory(), "expectations"), uuid + ".bin");
        case DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata:
            if (object.kind != SchemaObjectKind::SyntheticTestObject)
                replayConflict("ordinary dependent-object metadata path requires its exact durable installation record");
            return joinPath(joinPath(authorityDirectory(), "synthetic"), uuid + ".metadata");
        case DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar:
            if (object.kind != SchemaObjectKind::SyntheticTestObject)
                return tableReferencesPath(object);
            return joinPath(joinPath(authorityDirectory(), "synthetic"), uuid + ".references");
        case DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord: return metadataInstallationRecordPath(object);
    }
    replayConflict("schema-mutation artifact kind is invalid");
}

class AtomicDatabaseSchemaMutationDurableTransactionDiscovery::Impl final
{
public:
    static constexpr size_t sorted_run_size = 256;

    struct Entry
    {
        UInt64 transaction_id = 0;
        bool prepared = false;
    };

    struct RunCursor
    {
        size_t current = 0;
        size_t end = 0;
    };

    enum class Phase : UInt8
    {
        Collect,
        PrepareMerge,
        Merge,
        Validate,
        BuildResult,
        Complete,
    };

    const void * storage_identity = nullptr;
    std::optional<AuthorityRootGraphIdentity> root;
    UInt64 maximum_transactions = 0;
    UInt64 maximum_control_bytes = 0;
    Phase phase = Phase::Collect;
    DirectoryIteratorPtr iterator;
    std::vector<Entry> collected;
    std::vector<Entry> sorted;
    std::vector<RunCursor> merge_heap;
    std::vector<UInt64> durable_ids;
    size_t next_run_begin = 0;
    size_t build_result_index = 0;
    UInt64 namespace_entries = 0;
    UInt64 validation_entries = 0;
    UInt64 durable_entries = 0;

    static_assert(
        2 * sizeof(Entry) + sizeof(RunCursor) + sizeof(UInt64) <= atomic_database_schema_mutation_discovery_control_bytes_per_transaction);
};

AtomicDatabaseSchemaMutationDurableTransactionDiscovery::AtomicDatabaseSchemaMutationDurableTransactionDiscovery()
    : impl(std::make_unique<Impl>())
{
}

AtomicDatabaseSchemaMutationDurableTransactionDiscovery::~AtomicDatabaseSchemaMutationDurableTransactionDiscovery() = default;

class AtomicDatabaseSchemaMutationStorage::Impl final
{
public:
    Impl(
        DiskPtr disk_, UUID database_uuid_, String metadata_root_, String database_name_, AtomicDatabaseSchemaMutationStorageLimits limits_)
        : disk(std::move(disk_))
        , paths(std::move(metadata_root_), database_uuid_, std::move(database_name_))
        , limits(std::move(limits_))
    {
        if (!disk)
            storageFail(StorageError::Code::InvalidConfiguration, "Atomic schema storage disk is null");
        if (paths.getMetadataRoot().size() > limits.maximum_metadata_root_bytes || limits.maximum_directory_entries == 0
            || limits.maximum_checkpoint_namespace_entries == 0 || limits.maximum_checkpoint_namespace_entries > 64
            || limits.maximum_metadata_root_bytes == 0 || limits.maximum_total_authority_record_bytes == 0
            || limits.maximum_total_authority_record_bytes > resource_implementation_maximum_deterministic_catalog_bytes
            || limits.maximum_total_durable_dependent_object_bytes == 0
            || limits.maximum_total_durable_dependent_object_bytes > resource_implementation_maximum_durable_dependent_object_bytes
            || limits.wal.maximum_staged_artifacts == 0 || limits.wal.maximum_staged_artifact_bytes == 0
            || limits.wal.maximum_staged_artifact_bytes > std::numeric_limits<UInt64>::max() - internal_record_overhead)
            storageFail(StorageError::Code::InvalidConfiguration, "Atomic schema storage limits are invalid");
        if (disk->getDataSourceDescription().type != DataSourceType::Local || disk->isReadOnly())
            storageFail(StorageError::Code::UnsupportedDisk, "Atomic schema storage requires a writable local DiskPtr");
        if (!disk->isSymlinkSupported())
            storageFail(StorageError::Code::UnsupportedDisk, "Atomic schema storage requires explicit symlink inspection");
    }

    void requireNotSymlink(std::string_view path) const
    {
        if (!path.empty() && disk->isSymlink(String(path)))
            storageFail(StorageError::Code::UnsafePath, "Atomic schema metadata path contains a symbolic link");
    }

    void syncDirectory(std::string_view path) const
    {
        requireNotSymlink(path);
        auto guard = disk->getDirectorySyncGuard(String(path));
        if (!guard)
            storageFail(StorageError::Code::DirectorySyncUnavailable, "Atomic schema metadata directory cannot be synchronized");
        guard->sync();
    }

    void ensureDirectory(std::string_view path) const
    {
        String current;
        for (const auto & component : std::filesystem::path(path))
        {
            current = joinPath(current, component.generic_string());
            requireNotSymlink(current);
            if (disk->existsFileOrDirectory(current))
            {
                if (!disk->existsDirectory(current))
                    storageFail(StorageError::Code::UnsafePath, "Atomic schema metadata directory path is occupied by a file");
                continue;
            }
            const String parent = parentPath(current);
            disk->createDirectory(current);
            syncDirectory(parent);
        }
    }

    void ensureLayout() const
    {
        ensureDirectory(paths.typesDirectory());
        ensureDirectory(paths.authorityDirectory());
        ensureDirectory(paths.stagingDirectory());
        ensureDirectory(paths.walDirectory());
        ensureDirectory(paths.checkpointsDirectory());
        ensureDirectory(paths.retiredRollbackDirectory());
        ensureDirectory(paths.retiredCheckpointDirectory());
    }

    void ensureRegularFileOrAbsent(std::string_view path) const
    {
        requireNotSymlink(path);
        if (disk->existsFileOrDirectory(String(path)) && !disk->existsFile(String(path)))
            storageFail(StorageError::Code::UnsafePath, "Atomic schema metadata file path is occupied by a non-file");
    }

    std::optional<String> readOptionalFile(std::string_view path, UInt64 maximum_bytes) const
    {
        ensureRegularFileOrAbsent(path);
        if (!disk->existsFile(String(path)))
            return std::nullopt;
        const size_t size = disk->getFileSize(String(path));
        if (size > maximum_bytes)
            storageFail(StorageError::Code::LimitExceeded, "Atomic schema metadata file exceeds its byte limit");
        auto input = disk->readFile(String(path), ReadSettings{}, size);
        String result;
        result.reserve(size);
        readStringUntilEOF(result, *input);
        if (result.size() != size)
            storageFail(StorageError::Code::CorruptDurableState, "Atomic schema metadata file changed while being read");
        return result;
    }

    String readRequiredFile(std::string_view path, UInt64 maximum_bytes) const
    {
        auto result = readOptionalFile(path, maximum_bytes);
        if (!result)
            storageFail(StorageError::Code::CorruptDurableState, "required Atomic schema metadata file is missing");
        return std::move(*result);
    }

    AtomicDatabaseSchemaMutationStorage::VerificationTargetRead readAuthorityVerificationTarget(
        const AuthorityRoot & anchored_root, const ScheduledAuthorityVerificationTarget & target, bool snapshot_probe = false) const
    {
        using Read = AtomicDatabaseSchemaMutationStorage::VerificationTargetRead;
        struct ArtifactFormatDamage
        {
        };

        if (anchored_root.getDatabaseUUID() != paths.getDatabaseUUID())
            storageFail(StorageError::Code::InvalidConfiguration, "verification root belongs to another Atomic database");
        const auto inventory = anchored_root.pinAuthorityInventory();
        const auto * rooted_leaf = inventory ? inventory->find(target.leaf.key) : nullptr;
        if (!rooted_leaf || *rooted_leaf != target.leaf)
            storageFail(StorageError::Code::InvalidConfiguration, "verification target is absent from its anchored root");

        Read result;
        UInt64 internally_retained_bytes = 0;
        const auto working_retained_bytes = [&]
        {
            if (internally_retained_bytes > std::numeric_limits<UInt64>::max() - result.retained_bytes)
                storageFail(StorageError::Code::LimitExceeded, "verification retained bytes overflow UInt64");
            return result.retained_bytes + internally_retained_bytes;
        };
        const auto charge_validation = [&](UInt64 scratch_bytes)
        {
            const UInt64 retained_bytes = working_retained_bytes();
            if (scratch_bytes > std::numeric_limits<UInt64>::max() - retained_bytes)
                storageFail(StorageError::Code::LimitExceeded, "verification validation scratch overflows UInt64");
            const UInt64 peak = retained_bytes + scratch_bytes;
            if (peak > target.cost.transient_bytes)
                throw VerificationTargetBudgetMismatch{};
            result.charged_cost.transient_bytes = std::max(result.charged_cost.transient_bytes, peak);
        };
        const auto read_file = [&](std::string_view path, UInt64 format_maximum_bytes) -> std::optional<String>
        {
            ensureRegularFileOrAbsent(path);
            if (!disk->existsFile(String(path)))
                return std::nullopt;

            const size_t size = disk->getFileSize(String(path));
            const UInt64 bytes = static_cast<UInt64>(size);
            if (bytes > format_maximum_bytes)
                throw ArtifactFormatDamage{};
            if (result.charged_cost.io_bytes > target.cost.io_bytes || bytes > target.cost.io_bytes - result.charged_cost.io_bytes)
                throw VerificationTargetBudgetMismatch{};

            const UInt64 retained_before_read = working_retained_bytes();
            const UInt64 read_buffer_bytes = std::max<UInt64>(bytes, 1);
            if (bytes > std::numeric_limits<UInt64>::max() - retained_before_read
                || read_buffer_bytes > std::numeric_limits<UInt64>::max() - retained_before_read - bytes)
                storageFail(StorageError::Code::LimitExceeded, "verification artifact buffer peak overflows UInt64");
            const UInt64 read_peak = retained_before_read + bytes + read_buffer_bytes;
            if (read_peak > target.cost.transient_bytes)
                throw VerificationTargetBudgetMismatch{};

            ReadSettings read_settings;
            read_settings.disableCaches();
            read_settings.local_fs_settings.method = LocalFSReadMethod::pread;
            read_settings.local_fs_settings.prefetch = false;
            read_settings.local_fs_settings.direct_io_threshold = 0;
            read_settings.local_fs_settings.mmap_threshold = 0;
            read_settings.reader_executor.enabled = false;
            read_settings = read_settings.adjustBufferSize(size);
            auto input = disk->readFile(String(path), read_settings, size);
            result.charged_cost.transient_bytes = std::max(result.charged_cost.transient_bytes, retained_before_read + read_buffer_bytes);
            String loaded;
            loaded.resize(size);
            result.charged_cost.transient_bytes = std::max(result.charged_cost.transient_bytes, read_peak);
            const size_t read_bytes = input->read(loaded.data(), size);
            result.charged_cost.io_bytes += static_cast<UInt64>(read_bytes);
            if (read_bytes != size || disk->getFileSize(String(path)) != size)
                storageFail(StorageError::Code::CorruptDurableState, "verification artifact changed while being read");
            result.retained_bytes += bytes;
            return loaded;
        };

        try
        {
            UInt64 authority_maximum = 0;
            switch (target.leaf.key.record_kind)
            {
                case AuthorityInventoryRecordKind::TypeDefinition:
                    authority_maximum = limits.wal.definition_record.maximum_record_bytes;
                    break;
                case AuthorityInventoryRecordKind::SidecarExpectation: authority_maximum = limits.wal.maximum_staged_artifact_bytes; break;
            }
            result.authority_record_bytes = read_file(paths.authorityRecordPath(target.leaf.key), authority_maximum);
            if (!result.authority_record_bytes || target.leaf.key.record_kind == AuthorityInventoryRecordKind::TypeDefinition)
            {
                result.state = Read::State::Complete;
                return result;
            }

            SidecarExpectationRecord expectation;
            try
            {
                const UInt64 expectation_bytes = static_cast<UInt64>(result.authority_record_bytes->size());
                if (expectation_bytes > (std::numeric_limits<UInt64>::max() - sizeof(SidecarExpectationRecord)) / 2)
                    storageFail(StorageError::Code::LimitExceeded, "verification expectation scratch overflows UInt64");
                charge_validation(sizeof(SidecarExpectationRecord) + 2 * expectation_bytes);
                expectation = decodeSidecarExpectationRecord(*result.authority_record_bytes);
                const auto * rooted_expectation = anchored_root.findExpectationRecord(expectation.object);
                if (expectation.object.database_uuid != anchored_root.getDatabaseUUID()
                    || expectation.object.object_uuid != target.leaf.key.object_uuid
                    || expectation.object_schema_revision != target.leaf.object_revision
                    || computeSidecarExpectationRecordHash(expectation) != target.leaf.canonical_record_hash || !rooted_expectation
                    || *rooted_expectation != expectation)
                {
                    result.state = Read::State::Complete;
                    return result;
                }
                internally_retained_bytes = sizeof(SidecarExpectationRecord);
            }
            catch (const SidecarExpectationRecordError &)
            {
                result.state = Read::State::Complete;
                return result;
            }

            if (isOrdinaryDependentObjectKind(expectation.object.kind))
            {
                const UInt64 installation_maximum
                    = std::min(limits.wal.maximum_staged_artifact_bytes, limits.wal.installation_record.maximum_encoded_bytes);
                result.installation_record_bytes
                    = read_file(paths.metadataInstallationRecordPath(expectation.object), installation_maximum);
                if (!result.installation_record_bytes)
                {
                    result.state = Read::State::Complete;
                    return result;
                }

                try
                {
                    const UInt64 installation_bytes = static_cast<UInt64>(result.installation_record_bytes->size());
                    if (installation_bytes > (std::numeric_limits<UInt64>::max() - sizeof(DependentObjectMetadataInstallationRecord)) / 2)
                        storageFail(StorageError::Code::LimitExceeded, "verification installation scratch overflows UInt64");
                    charge_validation(sizeof(DependentObjectMetadataInstallationRecord) + 2 * installation_bytes);
                    const auto installation = decodeDependentObjectMetadataInstallationRecord(
                        *result.installation_record_bytes, limits.wal.installation_record);
                    if (installation.object != expectation.object
                        || installation.object_schema_revision != expectation.object_schema_revision
                        || !expectation.installation_record_hash
                        || computeDependentObjectMetadataInstallationRecordHash(installation, limits.wal.installation_record)
                            != *expectation.installation_record_hash)
                    {
                        result.state = Read::State::Complete;
                        return result;
                    }
                    const UInt64 installation_retained
                        = sizeof(DependentObjectMetadataInstallationRecord) + static_cast<UInt64>(installation.object_name.size());
                    if (installation_retained > std::numeric_limits<UInt64>::max() - internally_retained_bytes)
                        storageFail(StorageError::Code::LimitExceeded, "verification installation retained bytes overflow UInt64");
                    internally_retained_bytes += installation_retained;

                    result.persisted_references_bytes
                        = read_file(paths.tableReferencesPath(expectation.object), limits.wal.maximum_staged_artifact_bytes);
                    if (!result.persisted_references_bytes)
                    {
                        result.state = Read::State::Complete;
                        return result;
                    }
                    result.metadata_bytes
                        = read_file(paths.tableMetadataPath(installation.object_name), limits.wal.maximum_staged_artifact_bytes);
                }
                catch (const DependentObjectMetadataInstallationRecordError &)
                {
                    result.state = Read::State::Complete;
                    return result;
                }
            }
            else if (expectation.object.kind == SchemaObjectKind::SyntheticTestObject)
            {
                if (expectation.installation_record_hash)
                {
                    result.state = Read::State::Complete;
                    return result;
                }
                result.persisted_references_bytes = read_file(
                    paths.canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar, expectation.object),
                    limits.wal.maximum_staged_artifact_bytes);
                if (!result.persisted_references_bytes)
                {
                    result.state = Read::State::Complete;
                    return result;
                }
                result.metadata_bytes = read_file(
                    paths.canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, expectation.object),
                    limits.wal.maximum_staged_artifact_bytes);
            }
            else
            {
                storageFail(StorageError::Code::InvalidConfiguration, "verification target has no registered durable object adapter");
            }
            result.state = Read::State::Complete;
        }
        catch (const ArtifactFormatDamage &)
        {
            result.state = Read::State::Damaged;
        }
        catch (const VerificationTargetBudgetMismatch &)
        {
            if (snapshot_probe)
                throw;
            result.state = Read::State::Damaged;
        }
        catch (const AtomicDatabaseSchemaMutationStorageError &)
        {
            if (snapshot_probe)
                throw;
            result.state = Read::State::Failed;
        }
        catch (const std::bad_alloc &)
        {
            throw;
        }
        catch (const Exception & exception)
        {
            if (isUDTResourceOrControlExceptionCode(exception.code()))
                throw;
            if (snapshot_probe)
                throw;
            result.state = Read::State::Failed;
        }
        return result;
    }

    AtomicDatabaseSchemaMutationStorage::RepairAuditTargetRead readAuthorityRepairAuditTarget(
        const AuthorityRoot & anchored_root, const AuthorityInventoryLeaf & leaf, UInt64 maximum_retained_bytes) const
    {
        using Read = AtomicDatabaseSchemaMutationStorage::RepairAuditTargetRead;
        if (anchored_root.getDatabaseUUID() != paths.getDatabaseUUID())
            storageFail(StorageError::Code::InvalidConfiguration, "repair-audit root belongs to another Atomic database");
        const auto inventory = anchored_root.pinAuthorityInventory();
        const auto * rooted_leaf = inventory ? inventory->find(leaf.key) : nullptr;
        if (!rooted_leaf || *rooted_leaf != leaf)
            storageFail(StorageError::Code::InvalidConfiguration, "repair-audit target is absent from its anchored root");

        Read result;
        UInt64 retained_bytes = 0;
        const auto read_bounded = [&](std::string_view path, UInt64 artifact_maximum)
        {
            auto bytes = readOptionalFile(path, std::min(artifact_maximum, maximum_retained_bytes - retained_bytes));
            if (bytes)
            {
                if (bytes->size() > maximum_retained_bytes - retained_bytes)
                    storageFail(StorageError::Code::LimitExceeded, "repair-audit target exceeds its aggregate retained-byte limit");
                retained_bytes += bytes->size();
            }
            return bytes;
        };
        UInt64 authority_maximum = 0;
        switch (leaf.key.record_kind)
        {
            case AuthorityInventoryRecordKind::TypeDefinition: authority_maximum = limits.wal.definition_record.maximum_record_bytes; break;
            case AuthorityInventoryRecordKind::SidecarExpectation: authority_maximum = limits.wal.maximum_staged_artifact_bytes; break;
        }
        result.authority_record_bytes = read_bounded(paths.authorityRecordPath(leaf.key), authority_maximum);
        if (leaf.key.record_kind == AuthorityInventoryRecordKind::TypeDefinition)
            return result;

        const auto * expectation = anchored_root.findExpectationRecord(leaf.key.object_uuid);
        if (!expectation || expectation->object.database_uuid != anchored_root.getDatabaseUUID()
            || expectation->object.object_uuid != leaf.key.object_uuid || expectation->object_schema_revision != leaf.object_revision
            || computeSidecarExpectationRecordHash(*expectation) != leaf.canonical_record_hash)
        {
            storageFail(StorageError::Code::CorruptDurableState, "repair-audit rooted expectation differs from its inventory leaf");
        }

        if (isOrdinaryDependentObjectKind(expectation->object.kind))
        {
            result.persisted_references_bytes
                = read_bounded(paths.tableReferencesPath(expectation->object), limits.wal.maximum_staged_artifact_bytes);
            /// Metadata is authoritative only through the exact rooted
            /// installation-record hash. An absent hash deliberately means
            /// there is no installation/metadata audit target; unrelated
            /// bytes at those physical paths must not consume this leaf's
            /// retained budget or manufacture a repair finding.
            if (!expectation->installation_record_hash)
                return result;
            const UInt64 installation_maximum
                = std::min(limits.wal.maximum_staged_artifact_bytes, limits.wal.installation_record.maximum_encoded_bytes);
            result.installation_record_bytes
                = read_bounded(paths.metadataInstallationRecordPath(expectation->object), installation_maximum);
            if (result.installation_record_bytes)
            {
                try
                {
                    const auto installation = decodeDependentObjectMetadataInstallationRecord(
                        *result.installation_record_bytes, limits.wal.installation_record);
                    if (installation.object == expectation->object
                        && installation.object_schema_revision == expectation->object_schema_revision
                        && computeDependentObjectMetadataInstallationRecordHash(installation, limits.wal.installation_record)
                            == *expectation->installation_record_hash)
                    {
                        result.metadata_bytes
                            = read_bounded(paths.tableMetadataPath(installation.object_name), limits.wal.maximum_staged_artifact_bytes);
                    }
                }
                catch (const DependentObjectMetadataInstallationRecordError &)
                {
                }
            }
            return result;
        }
        if (expectation->object.kind != SchemaObjectKind::SyntheticTestObject)
            storageFail(StorageError::Code::InvalidConfiguration, "repair-audit target has no registered durable object adapter");
        result.persisted_references_bytes = read_bounded(
            paths.canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar, expectation->object),
            limits.wal.maximum_staged_artifact_bytes);
        result.metadata_bytes = read_bounded(
            paths.canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, expectation->object),
            limits.wal.maximum_staged_artifact_bytes);
        return result;
    }

    std::vector<AuthorityVerificationTarget> snapshotAuthorityVerificationTargets(
        const AuthorityRoot & anchored_root,
        const AuthorityVerificationScheduleLimits & schedule_limits,
        const AuthorityVerificationBatchExecutorLimits & executor_limits,
        UInt64 begin_target,
        UInt64 maximum_targets,
        std::span<const AuthorityVerificationTargetHistory> history,
        const AuthorityVerificationPassBudget & pass_budget,
        UInt64 * consumed_work_items) const
    {
        if (consumed_work_items)
            *consumed_work_items = 0;
        if (pass_budget.maximum_work_items == 0 || verificationPassBudgetExpired(pass_budget))
            return {};
        const auto inventory = anchored_root.pinAuthorityInventory();
        const auto graph = anchored_root.pinSchemaObjectDependencyGraph();
        if (!inventory || !graph || inventory->getSummary() != anchored_root.getInventorySummary()
            || inventory->getSummary().leaf_count > schedule_limits.maximum_snapshot_targets)
            storageFail(StorageError::Code::LimitExceeded, "authority verification target snapshot exceeds its rooted limits");

        const UInt64 leaf_count = inventory->getSummary().leaf_count;
        if (maximum_targets == 0 || begin_target > leaf_count || !std::in_range<size_t>(begin_target)
            || !std::in_range<size_t>(maximum_targets))
            storageFail(StorageError::Code::InvalidConfiguration, "authority verification snapshot range is invalid");
        std::vector<AuthorityVerificationTarget> result;
        const size_t begin = static_cast<size_t>(begin_target);
        const size_t maximum = static_cast<size_t>(maximum_targets);
        AuthorityVerificationTargetCost aggregate_cost;
        auto history_it = history.begin();
        bool history_seeded = false;
        UInt64 pass_work_items = 0;
        for (size_t index = begin; index < static_cast<size_t>(leaf_count) && result.size() < maximum; ++index)
        {
            if (pass_work_items >= pass_budget.maximum_work_items || verificationPassBudgetExpired(pass_budget))
                break;
            ++pass_work_items;
            if (result.empty())
            {
                result.reserve(
                    std::min({maximum, static_cast<size_t>(pass_budget.maximum_work_items), static_cast<size_t>(leaf_count) - begin}));
            }
            const auto * leaf_ptr = inventory->getLeafByCanonicalIndex(static_cast<UInt64>(index));
            if (!leaf_ptr)
                storageFail(StorageError::Code::CorruptDurableState, "authority verification snapshot lost its canonical inventory cursor");
            const auto & leaf = *leaf_ptr;
            const bool first_target = result.empty();
            const UInt64 canonical_limit
                = first_target ? schedule_limits.maximum_rooted_target_canonical_bytes : schedule_limits.maximum_canonical_bytes_per_batch;
            const UInt64 work_limit = first_target ? schedule_limits.maximum_rooted_target_verification_work_units
                                                   : schedule_limits.maximum_verification_work_units_per_batch;
            const UInt64 transient_limit
                = first_target ? schedule_limits.maximum_rooted_target_transient_bytes : schedule_limits.maximum_transient_bytes_per_batch;
            const UInt64 io_limit
                = first_target ? schedule_limits.maximum_rooted_target_io_bytes : schedule_limits.maximum_io_bytes_per_batch;
            if (aggregate_cost.canonical_bytes >= canonical_limit || aggregate_cost.work_units >= work_limit
                || aggregate_cost.transient_bytes > transient_limit || aggregate_cost.io_bytes >= io_limit)
                break;
            const AuthorityVerificationTargetCost probe_cost{
                .canonical_bytes = canonical_limit - aggregate_cost.canonical_bytes,
                .work_units = work_limit - aggregate_cost.work_units,
                .transient_bytes = transient_limit,
                .io_bytes = io_limit - aggregate_cost.io_bytes,
            };
            ScheduledAuthorityVerificationTarget probe{
                .leaf = leaf,
                .cost = probe_cost,
                .reasons = authorityVerificationSelectionReasonMask(AuthorityVerificationSelectionReason::Rotation),
            };
            AtomicDatabaseSchemaMutationStorage::VerificationTargetRead artifact;
            bool target_exceeded_probe = false;
            try
            {
                artifact = readAuthorityVerificationTarget(anchored_root, probe, true);
            }
            catch (const VerificationTargetBudgetMismatch &)
            {
                if (!first_target)
                    break;
                /// The exact rooted artifact grew beyond its admitted size.
                /// Do not read or parse beyond the rooted cap; schedule this
                /// leaf alone so ordinary execution emits terminal damage.
                target_exceeded_probe = true;
            }
            const AuthorityVerificationTargetArtifactView view{
                .authority_record
                = artifact.authority_record_bytes ? std::optional<std::string_view>(*artifact.authority_record_bytes) : std::nullopt,
                .installation_record
                = artifact.installation_record_bytes ? std::optional<std::string_view>(*artifact.installation_record_bytes) : std::nullopt,
                .persisted_references = artifact.persisted_references_bytes
                    ? std::optional<std::string_view>(*artifact.persisted_references_bytes)
                    : std::nullopt,
                .metadata = artifact.metadata_bytes ? std::optional<std::string_view>(*artifact.metadata_bytes) : std::nullopt,
                .retained_bytes = artifact.retained_bytes,
                .source_cost = artifact.charged_cost,
            };
            SchemaObjectID object;
            if (leaf.key.record_kind == AuthorityInventoryRecordKind::TypeDefinition)
            {
                object = {
                    .kind = SchemaObjectKind::TypeDefinition,
                    .database_uuid = anchored_root.getDatabaseUUID(),
                    .object_uuid = leaf.key.object_uuid,
                };
            }
            else
            {
                const auto * expectation = anchored_root.findExpectationRecord(leaf.key.object_uuid);
                if (!expectation || expectation->object_schema_revision != leaf.object_revision)
                    storageFail(StorageError::Code::CorruptDurableState, "rooted verification expectation inventory is inconsistent");
                object = expectation->object;
            }
            if (!history_seeded)
            {
                history_it = std::lower_bound(
                    history.begin(),
                    history.end(),
                    leaf.key,
                    [](const AuthorityVerificationTargetHistory & candidate, const AuthorityInventoryKey & key)
                    { return authorityInventoryKeyLess(candidate.leaf.key, key); });
                history_seeded = true;
            }
            else
            {
                while (history_it != history.end() && authorityInventoryKeyLess(history_it->leaf.key, leaf.key))
                    ++history_it;
            }
            const bool unchanged = history_it != history.end() && history_it->leaf == leaf;
            AuthorityVerificationTarget target{
                .leaf = leaf,
                .last_changed_catalog_epoch = unchanged ? history_it->last_changed_catalog_epoch : anchored_root.getDatabaseCatalogEpoch(),
                .last_periodic_verification_sequence = unchanged ? history_it->last_periodic_verification_sequence : 0,
                .reverse_dependency_count = graph->getDependentCount(object),
                .cost = target_exceeded_probe
                    ? probe_cost
                    : AuthorityVerificationBatchExecutor::estimateTrustedTargetCost(anchored_root, leaf, view, executor_limits),
            };
            const bool fits = target.cost.canonical_bytes <= probe_cost.canonical_bytes && target.cost.work_units <= probe_cost.work_units
                && target.cost.transient_bytes <= probe_cost.transient_bytes && target.cost.io_bytes <= probe_cost.io_bytes;
            if (!fits)
            {
                if (result.empty())
                    storageFail(StorageError::Code::LimitExceeded, "one authority verification target exceeds the full batch budget");
                break;
            }
            aggregate_cost.canonical_bytes += target.cost.canonical_bytes;
            aggregate_cost.work_units += target.cost.work_units;
            aggregate_cost.transient_bytes = std::max(aggregate_cost.transient_bytes, target.cost.transient_bytes);
            aggregate_cost.io_bytes += target.cost.io_bytes;
            result.push_back(std::move(target));
            if (aggregate_cost.transient_bytes > schedule_limits.maximum_transient_bytes_per_batch
                || aggregate_cost.canonical_bytes >= schedule_limits.maximum_canonical_bytes_per_batch
                || aggregate_cost.work_units >= schedule_limits.maximum_verification_work_units_per_batch
                || aggregate_cost.io_bytes >= schedule_limits.maximum_io_bytes_per_batch)
                break;
        }
        if (consumed_work_items)
            *consumed_work_items = pass_work_items;
        return result;
    }

    void writeFreshFile(std::string_view path, std::string_view bytes) const
    {
        ensureRegularFileOrAbsent(path);
        if (const auto existing = readOptionalFile(path, bytes.size()))
        {
            if (*existing != bytes)
                replayConflict("schema-mutation temporary-file replay differs");
            return;
        }
        fiu_do_on(DB::FailPoints::udt_schema_storage_temp_write_failure, {
            storageFail(StorageError::Code::FaultInjected, "fault injected before UDT schema temporary-file write");
        });
        auto output = disk->writeFile(String(path), std::max<size_t>(bytes.size(), 1), WriteMode::Rewrite, WriteSettings{});
        writeString(bytes, *output);
        output->finalize();
        fiu_do_on(DB::FailPoints::udt_schema_storage_temp_sync_failure, {
            storageFail(StorageError::Code::FaultInjected, "fault injected before UDT schema temporary-file sync");
        });
        output->sync();
    }

    void installImmutableFile(
        std::string_view path, std::string_view bytes, std::string_view temporary_suffix, bool repair_temporary = false) const
    {
        const String parent = parentPath(path);
        ensureDirectory(parent);
        const String temporary = String(path) + "." + String(temporary_suffix) + ".tmp";
        if (const auto existing = readOptionalFile(path, std::max<UInt64>(bytes.size(), 1)))
        {
            if (*existing != bytes)
                replayConflict("durable schema-mutation keyed record replay differs");
            if (repair_temporary && disk->existsFileOrDirectory(temporary))
            {
                ensureRegularFileOrAbsent(temporary);
                disk->removeFile(temporary);
                syncDirectory(parent);
            }
            return;
        }
        if (repair_temporary)
        {
            if (const auto existing = readOptionalFile(temporary, std::max<UInt64>(bytes.size(), 1)); existing && *existing != bytes)
            {
                disk->removeFile(temporary);
                syncDirectory(parent);
            }
        }
        writeFreshFile(temporary, bytes);
        ensureRegularFileOrAbsent(path);
        if (disk->existsFile(String(path)))
        {
            const auto raced = readRequiredFile(path, std::max<UInt64>(bytes.size(), 1));
            if (raced != bytes)
                replayConflict("durable schema-mutation keyed record appeared with different bytes");
            return;
        }
        fiu_do_on(DB::FailPoints::udt_schema_storage_temp_rename_failure, {
            storageFail(StorageError::Code::FaultInjected, "fault injected before UDT schema temporary-file rename");
        });
        disk->moveFile(temporary, String(path));
        syncDirectory(parent);
    }

    void replaceMutableFile(std::string_view path, std::string_view bytes, std::string_view temporary_suffix) const
    {
        const String parent = parentPath(path);
        ensureDirectory(parent);
        const String temporary = String(path) + "." + String(temporary_suffix) + ".tmp";
        if (const auto existing = readOptionalFile(temporary, std::max<UInt64>(bytes.size(), 1)); existing && *existing != bytes)
        {
            disk->removeFile(temporary);
            syncDirectory(parent);
        }
        writeFreshFile(temporary, bytes);
        ensureRegularFileOrAbsent(path);
        fiu_do_on(DB::FailPoints::udt_schema_storage_temp_rename_failure, {
            storageFail(StorageError::Code::FaultInjected, "fault injected before UDT schema temporary-file replacement");
        });
        disk->replaceFile(temporary, String(path));
        syncDirectory(parent);
    }

    String activationMarkerTemporaryPath() const { return paths.activationMarkerTemporaryPath(); }

    std::optional<ActivationMarkerState> activationMarkerState() const
    {
        const auto bytes = readOptionalFile(paths.activationMarkerPath(), internal_record_overhead);
        if (!bytes)
            return std::nullopt;
        auto state = decodeActivationMarkerInternal(*bytes, paths.getDatabaseUUID());
        if (state.activation_transaction_id == 0)
            storageFail(StorageError::Code::CorruptDurableState, "Atomic authority activation transaction ID is zero");
        return state;
    }

    std::optional<UInt64> activationMarkerTransactionID() const
    {
        const auto state = activationMarkerState();
        return state ? std::optional<UInt64>{state->activation_transaction_id} : std::nullopt;
    }

    bool hasActivationMarkerTemporary() const
    {
        const String path = activationMarkerTemporaryPath();
        ensureRegularFileOrAbsent(path);
        if (!disk->existsFile(path))
            return false;
        if (disk->getFileSize(path) > internal_record_overhead)
            storageFail(StorageError::Code::LimitExceeded, "Atomic authority activation temporary exceeds its byte limit");
        return true;
    }

    std::optional<ActivationMarkerState> activationMarkerTemporaryState() const
    {
        const auto bytes = readOptionalFile(activationMarkerTemporaryPath(), internal_record_overhead);
        if (!bytes)
            return std::nullopt;
        auto state = decodeActivationMarkerInternal(*bytes, paths.getDatabaseUUID());
        if (state.activation_transaction_id == 0)
            storageFail(StorageError::Code::CorruptDurableState, "Atomic authority activation temporary transaction ID is zero");
        return state;
    }

    static bool isRecoverableActivationMarkerV2Upgrade(
        const std::optional<ActivationMarkerState> & durable, const std::optional<ActivationMarkerState> & temporary) noexcept
    {
        return durable && temporary && !durable->requires_combined_configuration && temporary->requires_combined_configuration
            && durable->activation_transaction_id == temporary->activation_transaction_id;
    }

    void persistActivationMarker(UInt64 activation_transaction_id)
    {
        if (const auto existing = activationMarkerState())
        {
            if (existing->activation_transaction_id != activation_transaction_id)
                storageFail(StorageError::Code::CorruptDurableState, "first Atomic authority Commit has another activation identity");
            const String encoded = existing->requires_combined_configuration
                ? encodeActivationMarkerV2Internal(paths.getDatabaseUUID(), activation_transaction_id)
                : encodeAuthorityActivationMarker(paths.getDatabaseUUID(), activation_transaction_id);
            installImmutableFile(paths.activationMarkerPath(), encoded, "activation", true);
            return;
        }
        const String encoded = encodeActivationMarkerV2Internal(paths.getDatabaseUUID(), activation_transaction_id);
        installImmutableFile(paths.activationMarkerPath(), encoded, "activation", true);
    }

    void upgradeActivationMarkerToRequireCombinedConfiguration(const ActivationMarkerState & current)
    {
        if (current.requires_combined_configuration)
            return;
        const String encoded = encodeActivationMarkerV2Internal(paths.getDatabaseUUID(), current.activation_transaction_id);
        replaceMutableFile(paths.activationMarkerPath(), encoded, "activation");
    }

    void removeFirstActivationMarker(UInt64 activation_transaction_id)
    {
        if (const auto existing = activationMarkerTransactionID(); existing && *existing != activation_transaction_id)
            storageFail(StorageError::Code::CorruptDurableState, "first-activation rollback found another activation identity");

        bool removed = false;
        for (const String & path : {activationMarkerTemporaryPath(), paths.activationMarkerPath()})
        {
            ensureRegularFileOrAbsent(path);
            if (!disk->existsFile(path))
                continue;
            disk->removeFile(path);
            removed = true;
        }
        if (removed)
            syncDirectory(paths.getMetadataRoot());
    }

    std::optional<AuthorityVerificationScheduleCursor> loadAuthorityVerificationCursor()
    {
        constexpr UInt64 maximum_cursor_bytes = 256;
        const String durable_path = paths.verificationCursorPath();
        const String temporary_path = paths.verificationCursorTemporaryPath();
        const bool has_head = hasBoundedDurableAuthorityHead();
        const auto durable_bytes = readOptionalFile(durable_path, maximum_cursor_bytes);
        const auto temporary_bytes = readOptionalFile(temporary_path, maximum_cursor_bytes);
        if (!has_head)
        {
            if (durable_bytes || temporary_bytes)
                storageFail(StorageError::Code::CorruptDurableState, "authority verification cursor exists without an active authority");
            return std::nullopt;
        }

        std::optional<AuthorityVerificationScheduleCursor> durable;
        if (durable_bytes)
            durable = decodeAuthorityVerificationScheduleCursor(*durable_bytes, paths.getDatabaseUUID());
        if (!temporary_bytes)
            return durable;

        /// replaceMutableFile() synchronizes the complete temporary image
        /// before its atomic rename. Therefore an interrupted replacement is
        /// already valid completed progress and can be promoted exactly once.
        const auto recovered = decodeAuthorityVerificationScheduleCursor(*temporary_bytes, paths.getDatabaseUUID());
        ensureRegularFileOrAbsent(durable_path);
        disk->replaceFile(temporary_path, durable_path);
        syncDirectory(paths.getMetadataRoot());
        return recovered;
    }

    void persistAuthorityVerificationCursor(const AuthorityVerificationScheduleCursor & cursor)
    {
        if (!hasBoundedDurableAuthorityHead())
            storageFail(StorageError::Code::CorruptDurableState, "cannot persist verification progress without an active authority");
        const String encoded = encodeAuthorityVerificationScheduleCursor(cursor);
        replaceMutableFile(paths.verificationCursorPath(), encoded, "verification");
    }

    void validateVerificationSchedulerOverride(std::string_view bytes) const
    {
        try
        {
            static_cast<void>(decodeAuthorityVerificationSchedulerOverrideV2(bytes, paths.getDatabaseUUID()));
        }
        catch (const AuthorityVerificationScheduleError &)
        {
            storageFail(
                StorageError::Code::CorruptDurableState,
                "Atomic authority verification scheduler override V2 is malformed or belongs to another database");
        }
    }

    void validateResourceQuotaOverride(std::string_view bytes) const
    {
        try
        {
            static_cast<void>(decodeDatabaseResourceQuotaOverrideV2(bytes, paths.getDatabaseUUID()));
        }
        catch (const DatabaseResourceQuotaSettingsError &)
        {
            storageFail(
                StorageError::Code::CorruptDurableState,
                "Atomic authority database resource quota override V2 is malformed or belongs to another database");
        }
    }

    std::optional<String> readLegacyConfigurationComponentForMigration(
        std::string_view durable_path, std::string_view temporary_path, bool scheduler_component, std::string_view description)
    {
        auto durable = readOptionalFile(durable_path, maximum_udt_configuration_component_bytes);
        const auto temporary = readOptionalFile(temporary_path, maximum_udt_configuration_component_bytes);
        const auto validate = [&](std::string_view bytes)
        {
            if (scheduler_component)
                validateVerificationSchedulerOverride(bytes);
            else
                validateResourceQuotaOverride(bytes);
        };
        if (durable)
            validate(*durable);
        if (temporary)
        {
            validate(*temporary);
            if (!durable || *durable != *temporary)
            {
                storageFail(
                    StorageError::Code::CorruptDurableState,
                    String("active Atomic authority has a partial or conflicting legacy ") + String(description));
            }
            disk->removeFile(String(temporary_path));
            syncDirectory(paths.getMetadataRoot());
        }
        return durable;
    }

    AtomicDatabaseUDTPersistedConfigurationV2 readLegacyConfigurationForMigration()
    {
        return {
            .verification_scheduler_override = readLegacyConfigurationComponentForMigration(
                paths.verificationSchedulerOverrideV2Path(),
                paths.verificationSchedulerOverrideV2TemporaryPath(),
                true,
                "verification scheduler override"),
            .resource_quota_override = readLegacyConfigurationComponentForMigration(
                paths.resourceQuotaOverrideV2Path(), paths.resourceQuotaOverrideV2TemporaryPath(), false, "resource quota override"),
        };
    }

    void removeExactLegacyConfigurationAfterMigration(const AtomicDatabaseUDTPersistedConfigurationGenerationV2 & combined)
    {
        const auto scheduler_temporary
            = readOptionalFile(paths.verificationSchedulerOverrideV2TemporaryPath(), maximum_udt_configuration_component_bytes);
        const auto quota_temporary
            = readOptionalFile(paths.resourceQuotaOverrideV2TemporaryPath(), maximum_udt_configuration_component_bytes);
        if (scheduler_temporary || quota_temporary)
        {
            storageFail(StorageError::Code::CorruptDurableState, "combined Atomic UDT configuration retained a legacy temporary record");
        }

        const auto scheduler = readOptionalFile(paths.verificationSchedulerOverrideV2Path(), maximum_udt_configuration_component_bytes);
        const auto quota = readOptionalFile(paths.resourceQuotaOverrideV2Path(), maximum_udt_configuration_component_bytes);
        if (!scheduler && !quota)
            return;
        if (combined.generation != 1)
            storageFail(StorageError::Code::CorruptDurableState, "updated Atomic UDT configuration retained legacy V2 records");
        if (scheduler)
            validateVerificationSchedulerOverride(*scheduler);
        if (quota)
            validateResourceQuotaOverride(*quota);
        if ((scheduler
             && (!combined.configuration.verification_scheduler_override
                 || *scheduler != *combined.configuration.verification_scheduler_override))
            || (quota && (!combined.configuration.resource_quota_override || *quota != *combined.configuration.resource_quota_override)))
        {
            storageFail(
                StorageError::Code::CorruptDurableState, "combined Atomic UDT configuration conflicts with its legacy migration residue");
        }

        if (scheduler)
            disk->removeFile(paths.verificationSchedulerOverrideV2Path());
        if (quota)
            disk->removeFile(paths.resourceQuotaOverrideV2Path());
        syncDirectory(paths.getMetadataRoot());
    }

    AtomicDatabaseUDTPersistedConfigurationGenerationV2 readOrMigrateCombinedConfiguration()
    {
        const auto activation_marker = activationMarkerState();
        if (!activation_marker)
            storageFail(StorageError::Code::CorruptDurableState, "Atomic UDT configuration has no activation marker");
        const String durable_path = paths.udtConfigurationV2Path();
        const String temporary_path = paths.udtConfigurationV2TemporaryPath();
        const auto durable_bytes = readOptionalFile(durable_path, maximum_udt_configuration_record_bytes);
        const auto temporary_bytes = readOptionalFile(temporary_path, maximum_udt_configuration_record_bytes);
        std::optional<AtomicDatabaseUDTPersistedConfigurationGenerationV2> durable;
        std::optional<AtomicDatabaseUDTPersistedConfigurationGenerationV2> temporary;
        if (durable_bytes)
            durable = decodeAtomicDatabaseUDTConfigurationGenerationV2(*durable_bytes, paths.getDatabaseUUID());
        if (temporary_bytes)
            temporary = decodeAtomicDatabaseUDTConfigurationGenerationV2(*temporary_bytes, paths.getDatabaseUUID());

        if (!durable && activation_marker->requires_combined_configuration)
        {
            storageFail(StorageError::Code::CorruptDurableState, "active Atomic authority lost its required combined UDT configuration");
        }
        if (!durable)
        {
            AtomicDatabaseUDTPersistedConfigurationGenerationV2 migrated{
                .database_uuid = paths.getDatabaseUUID(),
                .generation = 1,
                .configuration = readLegacyConfigurationForMigration(),
            };
            const String encoded = encodeAtomicDatabaseUDTConfigurationGenerationV2(migrated);
            if (temporary)
            {
                if (*temporary != migrated)
                {
                    storageFail(
                        StorageError::Code::CorruptDurableState,
                        "partial Atomic UDT combined configuration differs from the exact legacy V2 pair");
                }
                disk->replaceFile(temporary_path, durable_path);
                syncDirectory(paths.getMetadataRoot());
            }
            else
            {
                installImmutableFile(durable_path, encoded, "configuration-v2", true);
            }
            durable = std::move(migrated);
        }
        else if (temporary)
        {
            if (durable->generation == std::numeric_limits<UInt64>::max() || temporary->generation != durable->generation + 1
                || temporary->configuration == durable->configuration)
            {
                storageFail(
                    StorageError::Code::CorruptDurableState,
                    "Atomic UDT configuration temporary is not one canonical successor generation");
            }
        }

        upgradeActivationMarkerToRequireCombinedConfiguration(*activation_marker);
        removeExactLegacyConfigurationAfterMigration(*durable);
        return *durable;
    }

    void removeNeverActivatedConfigurationFiles()
    {
        bool removed = false;
        for (const String & path :
             {paths.udtConfigurationV2TemporaryPath(),
              paths.udtConfigurationV2Path(),
              paths.verificationSchedulerOverrideV2TemporaryPath(),
              paths.verificationSchedulerOverrideV2Path(),
              paths.resourceQuotaOverrideV2TemporaryPath(),
              paths.resourceQuotaOverrideV2Path()})
        {
            ensureRegularFileOrAbsent(path);
            if (!disk->existsFile(path))
                continue;
            disk->removeFile(path);
            removed = true;
        }
        if (removed)
            syncDirectory(paths.getMetadataRoot());
    }

    void prepareUDTConfigurationForFirstActivationV2(const AtomicDatabaseUDTPersistedConfigurationV2 & configured)
    {
        if (hasDurableAuthorityMarker())
            storageFail(StorageError::Code::CorruptDurableState, "cannot stage first-activation policy for an active Atomic authority");
        static_cast<void>(cleanupNeverEnabledScaffold());
        if (hasDurableAuthorityMarker())
            storageFail(StorageError::Code::CorruptDurableState, "Atomic authority became active while staging its V2 policy");

        try
        {
            validateConfigurationComponentPayloads(configured, paths.getDatabaseUUID(), StorageError::Code::CorruptDurableState);
            const AtomicDatabaseUDTPersistedConfigurationGenerationV2 initial{
                .database_uuid = paths.getDatabaseUUID(),
                .generation = 1,
                .configuration = configured,
            };
            installImmutableFile(
                paths.udtConfigurationV2Path(), encodeAtomicDatabaseUDTConfigurationGenerationV2(initial), "configuration-v2", true);
        }
        catch (...)
        {
            /// No activation marker can exist while the caller holds the schema
            /// serialization boundary. Remove the complete combined generation
            /// and any legacy residue before returning failure.
            removeNeverActivatedConfigurationFiles();
            throw;
        }
    }

    AtomicDatabaseUDTPersistedConfigurationV2
    reconcileUDTConfigurationForActiveStartupV2(const AtomicDatabaseUDTPersistedConfigurationV2 & configured)
    {
        if (!hasBoundedDurableAuthorityHead())
            storageFail(StorageError::Code::CorruptDurableState, "cannot reconcile V2 policy without an active Atomic authority");
        validateConfigurationComponentPayloads(configured, paths.getDatabaseUUID(), StorageError::Code::CorruptDurableState);

        auto current = readOrMigrateCombinedConfiguration();
        auto replacement_configuration = current.configuration;
        if (configured.verification_scheduler_override)
            replacement_configuration.verification_scheduler_override = configured.verification_scheduler_override;
        if (configured.resource_quota_override)
            replacement_configuration.resource_quota_override = configured.resource_quota_override;

        const String temporary_path = paths.udtConfigurationV2TemporaryPath();
        const auto temporary_bytes = readOptionalFile(temporary_path, maximum_udt_configuration_record_bytes);
        std::optional<AtomicDatabaseUDTPersistedConfigurationGenerationV2> temporary;
        if (temporary_bytes)
            temporary = decodeAtomicDatabaseUDTConfigurationGenerationV2(*temporary_bytes, paths.getDatabaseUUID());
        if (replacement_configuration == current.configuration)
        {
            if (temporary)
            {
                disk->removeFile(temporary_path);
                syncDirectory(paths.getMetadataRoot());
            }
            return current.configuration;
        }
        if (current.generation == std::numeric_limits<UInt64>::max())
            storageFail(StorageError::Code::LimitExceeded, "Atomic UDT configuration generation overflows UInt64");

        AtomicDatabaseUDTPersistedConfigurationGenerationV2 replacement{
            .database_uuid = paths.getDatabaseUUID(),
            .generation = current.generation + 1,
            .configuration = std::move(replacement_configuration),
        };
        const String encoded = encodeAtomicDatabaseUDTConfigurationGenerationV2(replacement);
        if (temporary && *temporary != replacement)
        {
            disk->removeFile(temporary_path);
            syncDirectory(paths.getMetadataRoot());
        }
        replaceMutableFile(paths.udtConfigurationV2Path(), encoded, "configuration-v2");
        const auto committed_bytes = readRequiredFile(paths.udtConfigurationV2Path(), maximum_udt_configuration_record_bytes);
        const auto committed = decodeAtomicDatabaseUDTConfigurationGenerationV2(committed_bytes, paths.getDatabaseUUID());
        if (committed != replacement)
            storageFail(StorageError::Code::CorruptDurableState, "Atomic UDT configuration replacement did not commit exactly");
        return committed.configuration;
    }

    std::vector<String> listNames(std::string_view directory, UInt64 requested_maximum_entries = 0) const
    {
        requireNotSymlink(directory);
        if (!disk->existsFileOrDirectory(String(directory)))
            return {};
        if (!disk->existsDirectory(String(directory)))
            storageFail(StorageError::Code::UnsafePath, "Atomic schema metadata directory is not a directory");
        std::vector<String> result;
        const UInt64 maximum_entries = requested_maximum_entries ? std::min(requested_maximum_entries, limits.maximum_directory_entries)
                                                                 : limits.maximum_directory_entries;
        auto iterator = disk->iterateDirectory(String(directory));
        while (iterator->isValid())
        {
            if (result.size() >= maximum_entries)
                storageFail(StorageError::Code::LimitExceeded, "Atomic schema metadata directory exceeds its entry limit");
            const String name = iterator->name();
            if (name.empty() || name == "." || name == ".." || name.find('/') != String::npos)
                storageFail(StorageError::Code::UnsafePath, "Atomic schema metadata directory contains an unsafe entry");
            result.push_back(name);
            iterator->next();
        }
        std::sort(result.begin(), result.end());
        return result;
    }

    void validateGuard(const DatabaseSchemaMutationGuard & guard) const
    {
        if (guard.getDatabaseUUID() != paths.getDatabaseUUID()
            || guard.getOpaqueIdentity() != current_guard_identity.load(std::memory_order_acquire))
            replayConflict("schema-mutation guard token is stale or foreign");
    }

    void requireActiveTransaction(UInt64 transaction_id) const
    {
        if (!active_transaction_id || *active_transaction_id != transaction_id)
            replayConflict("schema-mutation storage call belongs to another active transaction");
    }

    void requireActiveDatabase(UUID database_uuid, UInt64 transaction_id) const
    {
        if (database_uuid != paths.getDatabaseUUID())
            replayConflict("schema-mutation storage call belongs to another database");
        requireActiveTransaction(transaction_id);
    }

    StagedEnvelope
    loadStagedEnvelope(UInt64 transaction_id, UInt64 ordinal, UInt64 maximum_canonical_bytes = std::numeric_limits<UInt64>::max()) const
    {
        const auto path = paths.stagedArtifactPath(transaction_id, ordinal);
        const UInt64 canonical_maximum = std::min(limits.wal.maximum_staged_artifact_bytes, maximum_canonical_bytes);
        const UInt64 maximum = canonical_maximum > std::numeric_limits<UInt64>::max() - internal_record_overhead
            ? std::numeric_limits<UInt64>::max()
            : canonical_maximum + internal_record_overhead;
        auto result = decodeStagedEnvelope(readRequiredFile(path, maximum), canonical_maximum);
        const auto expected = makeDatabaseSchemaWALStagedArtifactLocator(paths.getDatabaseUUID(), transaction_id, ordinal);
        if (result.locator != expected)
            storageFail(StorageError::Code::CorruptDurableState, "staged artifact envelope is installed at another locator");
        return result;
    }

    void validateStagingTransaction(UInt64 transaction_id) const
    {
        const String directory = paths.stagingTransactionDirectory(transaction_id);
        requireNotSymlink(directory);
        if (!disk->existsDirectory(directory))
            storageFail(StorageError::Code::UnsafePath, "staging transaction entry is not a directory");
        std::map<UInt64, String> envelopes;
        UInt64 total_artifact_bytes = 0;
        const UInt64 maximum = limits.wal.maximum_staged_artifact_bytes + internal_record_overhead;
        for (const auto & name : listNames(directory))
        {
            const auto parsed = parseStagedArtifactName(name);
            if (!parsed || parsed->ordinal >= limits.wal.maximum_staged_artifacts)
                storageFail(StorageError::Code::UnsafePath, "staging transaction contains a noncanonical artifact name");
            const String bytes = readRequiredFile(joinPath(directory, name), maximum);
            const auto envelope = decodeStagedEnvelope(bytes, limits.wal.maximum_staged_artifact_bytes);
            const auto expected = makeDatabaseSchemaWALStagedArtifactLocator(paths.getDatabaseUUID(), transaction_id, parsed->ordinal);
            if (envelope.locator != expected)
                storageFail(StorageError::Code::CorruptDurableState, "staged artifact filename and durable locator differ");
            const auto [it, inserted] = envelopes.emplace(parsed->ordinal, bytes);
            if (!inserted && it->second != bytes)
                storageFail(StorageError::Code::CorruptDurableState, "durable and temporary staged images differ");
            if (inserted)
            {
                if (envelope.canonical_bytes.size() > limits.wal.maximum_total_staged_artifact_bytes - total_artifact_bytes)
                    storageFail(StorageError::Code::LimitExceeded, "staging transaction exceeds its aggregate artifact byte limit");
                total_artifact_bytes += envelope.canonical_bytes.size();
            }
        }
    }

    std::vector<String> loadValidatedStagingManifest(
        UInt64 transaction_id,
        const DatabaseSchemaWALPrepare & prepare,
        bool proposed_prepare = false,
        UInt64 maximum_total_canonical_bytes = std::numeric_limits<UInt64>::max(),
        UInt64 maximum_control_bytes = 1ULL << 30) const
    {
        maximum_control_bytes = std::min(maximum_control_bytes, limits.wal.maximum_decode_control_bytes);
        if (maximum_control_bytes == 0)
            storageFail(StorageError::Code::InvalidConfiguration, "staging manifest control byte limit is invalid");
        const UInt64 artifact_count = static_cast<UInt64>(prepare.staged_artifacts.size());
        {
            /// Validate the exact 0..N-1 namespace as a streaming pass. The
            /// old two-set comparison retained O(N) String nodes and names at
            /// the same time as the returned manifest.
            constexpr UInt64 maximum_in_flight_directory_name_bytes = 256;
            const UInt64 namespace_control_bytes = checkedConfigurationSizeAdd(
                artifact_count,
                sizeof(String) + maximum_in_flight_directory_name_bytes,
                StorageError::Code::LimitExceeded,
                "staging manifest namespace control byte count overflows UInt64");
            if (namespace_control_bytes > maximum_control_bytes)
                storageFail(StorageError::Code::LimitExceeded, "staging manifest namespace exceeds its control byte limit");

            std::vector<UInt8> seen(static_cast<size_t>(artifact_count), 0);
            const String directory = paths.stagingTransactionDirectory(transaction_id);
            requireNotSymlink(directory);
            UInt64 entries = 0;
            if (disk->existsFileOrDirectory(directory))
            {
                if (!disk->existsDirectory(directory))
                    storageFail(StorageError::Code::UnsafePath, "staging transaction namespace is not a directory");
                auto iterator = disk->iterateDirectory(directory);
                while (iterator->isValid())
                {
                    if (entries >= artifact_count)
                    {
                        if (proposed_prepare)
                            replayConflict("Prepare manifest differs from the exact staged artifact set");
                        storageFail(
                            StorageError::Code::CorruptDurableState, "staging directory contains an artifact outside its Prepare manifest");
                    }
                    const String name = iterator->name();
                    if (name.empty() || name == "." || name == ".." || name.find('/') != String::npos)
                        storageFail(StorageError::Code::UnsafePath, "staging directory contains an unsafe entry");
                    if (name.size() > maximum_in_flight_directory_name_bytes)
                        storageFail(StorageError::Code::LimitExceeded, "staging directory entry exceeds its control byte limit");
                    const auto parsed = parseStagedArtifactName(name);
                    if (!parsed || parsed->temporary || parsed->ordinal >= artifact_count || seen[parsed->ordinal])
                    {
                        if (proposed_prepare)
                            replayConflict("Prepare manifest differs from the exact staged artifact set");
                        storageFail(
                            StorageError::Code::CorruptDurableState, "staging directory contains an artifact outside its Prepare manifest");
                    }
                    seen[parsed->ordinal] = 1;
                    ++entries;
                    iterator->next();
                }
            }
            if (entries != artifact_count)
            {
                if (proposed_prepare)
                    replayConflict("Prepare manifest differs from the exact staged artifact set");
                storageFail(StorageError::Code::CorruptDurableState, "staging directory differs from its Prepare manifest");
            }
        }

        const UInt64 manifest_control_bytes = checkedConfigurationSizeAdd(
            checkedStorageSizeMultiply(artifact_count, sizeof(String), "staging manifest String control byte count overflows UInt64"),
            sizeof(StagedEnvelope) + sizeof(String),
            StorageError::Code::LimitExceeded,
            "staging manifest control byte count overflows UInt64");
        if (manifest_control_bytes > maximum_control_bytes)
        {
            storageFail(StorageError::Code::LimitExceeded, "staging manifest exceeds its control byte limit");
        }

        std::vector<String> bytes;
        bytes.reserve(prepare.staged_artifacts.size());
        const UInt64 aggregate_maximum = std::min(limits.wal.maximum_total_staged_artifact_bytes, maximum_total_canonical_bytes);
        UInt64 retained_bytes = 0;
        for (size_t ordinal = 0; ordinal < prepare.staged_artifacts.size(); ++ordinal)
        {
            auto staged = loadStagedEnvelope(transaction_id, ordinal, aggregate_maximum - retained_bytes);
            if (staged.artifact != prepare.staged_artifacts[ordinal])
            {
                if (proposed_prepare)
                    replayConflict("Prepare manifest differs from an exact staged artifact envelope");
                storageFail(StorageError::Code::CorruptDurableState, "staging directory differs from its Prepare manifest");
            }
            if (staged.canonical_bytes.size() > aggregate_maximum - retained_bytes)
                storageFail(StorageError::Code::LimitExceeded, "staging manifest exceeds its requested aggregate byte limit");
            retained_bytes += staged.canonical_bytes.size();
            bytes.push_back(std::move(staged.canonical_bytes));
        }
        return bytes;
    }

    void validateMappedTableStagingPreconditions(UInt64 transaction_id) const
    {
        struct TableImage
        {
            bool seen = false;
            std::optional<StagedEnvelope> metadata;
            std::optional<DependentObjectMetadataInstallationRecord> installation;
        };
        struct TableTransition
        {
            TableImage before;
            TableImage after;
        };

        std::map<SchemaObjectID, TableTransition> tables;
        std::set<UInt64> ordinals;
        for (const auto & name : listNames(paths.stagingTransactionDirectory(transaction_id)))
        {
            const auto parsed = parseStagedArtifactName(name);
            if (!parsed || parsed->temporary || !ordinals.insert(parsed->ordinal).second)
                continue;
            auto envelope = loadStagedEnvelope(transaction_id, parsed->ordinal);
            if (!isOrdinaryDependentObjectKind(envelope.artifact.object.kind))
                continue;
            if (envelope.artifact.kind != DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata
                && envelope.artifact.kind != DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord)
                continue;

            auto & transition = tables[envelope.artifact.object];
            auto & image = envelope.artifact.image == DatabaseSchemaWALStagedArtifactImage::Before ? transition.before : transition.after;
            switch (envelope.artifact.kind)
            {
                case DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata:
                    image.seen = true;
                    if (image.metadata)
                        replayConflict("mapped table staging contains duplicate metadata images");
                    image.metadata = std::move(envelope);
                    break;
                case DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord:
                    image.seen = true;
                    if (image.installation)
                        replayConflict("mapped table staging contains duplicate installation records");
                    image.installation = decodeTableInstallationEnvelope(envelope);
                    break;
                case DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord:
                case DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord:
                case DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar: UNREACHABLE();
            }
        }

        const auto validate_image = [&](const SchemaObjectID & object, const TableImage & image) -> std::optional<String>
        {
            if (!image.seen)
                return std::nullopt;
            if (!image.metadata)
                replayConflict("table staging image lacks its exact metadata artifact");
            if (!image.installation)
                return std::nullopt;
            if (image.installation->object != object || image.installation->object_schema_revision != image.metadata->artifact.revision
                || image.installation->metadata_artifact_hash != image.metadata->artifact.content_hash)
                replayConflict("table staging mapping does not address its exact metadata image");
            return paths.tableMetadataPath(image.installation->object_name);
        };

        std::map<String, SchemaObjectID> before_targets;
        std::map<String, SchemaObjectID> after_targets;
        const auto register_target = [](const String & target, const SchemaObjectID & object, std::map<String, SchemaObjectID> & targets)
        {
            const auto [it, inserted] = targets.emplace(target, object);
            if (!inserted && it->second != object)
                replayConflict("two table staging images map to the same ordinary metadata path");
        };
        for (const auto & [object, transition] : tables)
        {
            const auto before_target = validate_image(object, transition.before);
            const auto after_target = validate_image(object, transition.after);
            if (!before_target && !after_target)
                replayConflict("table staging transition has no mapped logical image");

            /// Initial ALTER admission has an ordinary physical Before image
            /// with no installation record. Its exact path is nevertheless
            /// unambiguous: the logical After installation retains the same
            /// table name. This is distinct from CREATE (which has no Before
            /// metadata at all).
            const std::optional<String> effective_before_target
                = before_target ? before_target : (transition.before.seen ? after_target : std::nullopt);

            if (effective_before_target)
                register_target(*effective_before_target, object, before_targets);
            if (after_target)
                register_target(*after_target, object, after_targets);
            if (transition.before.seen)
            {
                if (!effective_before_target)
                    replayConflict("ordinary table Before image has no durable metadata target");
                const auto existing = readOptionalFile(*effective_before_target, limits.wal.maximum_staged_artifact_bytes);
                if (!existing || *existing != transition.before.metadata->canonical_bytes)
                    replayConflict("ordinary table metadata does not match the durable Before image");
                if (after_target && *after_target != *effective_before_target
                    && readOptionalFile(*after_target, limits.wal.maximum_staged_artifact_bytes))
                    replayConflict("ordinary table RENAME target is already occupied before durable mutation");
            }
            else if (readOptionalFile(*after_target, limits.wal.maximum_staged_artifact_bytes))
            {
                replayConflict("ordinary table metadata target is already occupied before mapped table CREATE");
            }
        }
    }

    std::optional<DatabaseSchemaWALPrepare> readPrepareFromDirectory(
        UInt64 transaction_id, std::string_view transaction_directory, UInt64 maximum_staged_artifacts, UInt64 maximum_control_bytes) const
    {
        if (maximum_staged_artifacts == 0 || maximum_control_bytes < database_schema_wal_prepare_minimum_decode_control_bytes)
            storageFail(StorageError::Code::LimitExceeded, "Prepare marker exceeds its prospective recovery control limits");
        /// Encoded WAL bytes are the independently bounded input/payload
        /// domain. They coexist with, but do not consume, the caller's decoded
        /// retained-control allowance.
        const auto bytes = readOptionalFile(joinPath(transaction_directory, "prepare.wal"), limits.wal.maximum_encoded_bytes);
        if (!bytes)
            return std::nullopt;
        auto decode_limits = limits.wal;
        decode_limits.maximum_staged_artifacts = std::min(decode_limits.maximum_staged_artifacts, maximum_staged_artifacts);
        decode_limits.maximum_decode_control_bytes = std::min(decode_limits.maximum_decode_control_bytes, maximum_control_bytes);
        auto prepare = decodeDatabaseSchemaWALPrepare(*bytes, decode_limits);
        if (prepare.transaction_id != transaction_id || prepare.after_authority_state.database_uuid != paths.getDatabaseUUID())
            storageFail(StorageError::Code::CorruptDurableState, "Prepare marker is stored under another transaction identity");
        return prepare;
    }

    std::optional<DatabaseSchemaWALPrepare> readPrepareFromDirectory(UInt64 transaction_id, std::string_view transaction_directory) const
    {
        return readPrepareFromDirectory(
            transaction_id, transaction_directory, limits.wal.maximum_staged_artifacts, limits.wal.maximum_decode_control_bytes);
    }

    std::optional<DatabaseSchemaWALPrepare>
    readPrepare(UInt64 transaction_id, UInt64 maximum_staged_artifacts, UInt64 maximum_control_bytes) const
    {
        return readPrepareFromDirectory(
            transaction_id, paths.walTransactionDirectory(transaction_id), maximum_staged_artifacts, maximum_control_bytes);
    }

    std::optional<DatabaseSchemaWALPrepare> readPrepare(UInt64 transaction_id) const
    {
        return readPrepareFromDirectory(transaction_id, paths.walTransactionDirectory(transaction_id));
    }

    std::optional<DatabaseSchemaWALCommit> readCommitFromDirectory(
        const DatabaseSchemaWALPrepare & prepare, std::string_view transaction_directory, UInt64 maximum_control_bytes) const
    {
        static_cast<void>(maximum_control_bytes);
        const auto bytes = readOptionalFile(joinPath(transaction_directory, "commit.wal"), limits.wal.maximum_encoded_bytes);
        if (!bytes)
            return std::nullopt;
        auto commit = decodeDatabaseSchemaWALCommit(*bytes, limits.wal);
        if (!commitMatchesPrepare(commit, prepare))
            storageFail(StorageError::Code::CorruptDurableState, "Commit marker does not bind its durable Prepare marker");
        return commit;
    }

    std::optional<DatabaseSchemaWALCommit>
    readCommitFromDirectory(const DatabaseSchemaWALPrepare & prepare, std::string_view transaction_directory) const
    {
        return readCommitFromDirectory(prepare, transaction_directory, limits.wal.maximum_decode_control_bytes);
    }

    std::optional<DatabaseSchemaWALCommit>
    readCommit(UInt64 transaction_id, const DatabaseSchemaWALPrepare & prepare, UInt64 maximum_control_bytes) const
    {
        return readCommitFromDirectory(prepare, paths.walTransactionDirectory(transaction_id), maximum_control_bytes);
    }

    std::optional<DatabaseSchemaWALCommit> readCommit(UInt64 transaction_id, const DatabaseSchemaWALPrepare & prepare) const
    {
        return readCommitFromDirectory(prepare, paths.walTransactionDirectory(transaction_id));
    }

    std::optional<DatabaseSchemaWALCommit>
    readStandaloneCommitFromDirectory(UInt64 transaction_id, std::string_view transaction_directory) const
    {
        const auto bytes = readOptionalFile(joinPath(transaction_directory, "commit.wal"), limits.wal.maximum_encoded_bytes);
        if (!bytes)
            return std::nullopt;
        auto commit = decodeDatabaseSchemaWALCommit(*bytes, limits.wal);
        if (commit.transaction_id != transaction_id || commit.database_uuid != paths.getDatabaseUUID())
            storageFail(StorageError::Code::CorruptDurableState, "Commit marker is stored under another transaction identity");
        return commit;
    }

    std::optional<DatabaseSchemaWALRecoveryDecision> readRecoveryDecisionFromDirectory(
        UInt64 transaction_id,
        const DatabaseSchemaWALPrepare & prepare,
        std::string_view transaction_directory,
        UInt64 maximum_control_bytes) const
    {
        static_cast<void>(maximum_control_bytes);
        const auto bytes = readOptionalFile(joinPath(transaction_directory, "recovery.bin"), internal_record_overhead);
        if (!bytes)
            return std::nullopt;
        return decodeAtomicDatabaseSchemaMutationRecoveryDecision(*bytes, paths.getDatabaseUUID(), transaction_id, prepare.prepare_hash);
    }

    std::optional<DatabaseSchemaWALRecoveryDecision> readRecoveryDecisionFromDirectory(
        UInt64 transaction_id, const DatabaseSchemaWALPrepare & prepare, std::string_view transaction_directory) const
    {
        return readRecoveryDecisionFromDirectory(transaction_id, prepare, transaction_directory, limits.wal.maximum_decode_control_bytes);
    }

    std::optional<DatabaseSchemaWALRecoveryDecision>
    readRecoveryDecision(UInt64 transaction_id, const DatabaseSchemaWALPrepare & prepare, UInt64 maximum_control_bytes) const
    {
        return readRecoveryDecisionFromDirectory(
            transaction_id, prepare, paths.walTransactionDirectory(transaction_id), maximum_control_bytes);
    }

    std::optional<DatabaseSchemaWALRecoveryDecision>
    readRecoveryDecision(UInt64 transaction_id, const DatabaseSchemaWALPrepare & prepare) const
    {
        return readRecoveryDecisionFromDirectory(transaction_id, prepare, paths.walTransactionDirectory(transaction_id));
    }

    AtomicDatabaseSchemaMutationRecoveryTransaction loadTransaction(
        UInt64 transaction_id,
        UInt64 maximum_total_staged_artifact_bytes = std::numeric_limits<UInt64>::max(),
        UInt64 maximum_staged_artifacts = std::numeric_limits<UInt64>::max(),
        UInt64 maximum_control_bytes = atomic_database_schema_mutation_default_recovery_control_bytes) const
    {
        if (maximum_control_bytes == 0)
            storageFail(StorageError::Code::InvalidConfiguration, "schema-mutation recovery control byte limit is invalid");
        constexpr UInt64 transaction_fixed_control_bytes
            = sizeof(AtomicDatabaseSchemaMutationRecoveryTransaction) - sizeof(DatabaseSchemaWALPrepare);
        if (transaction_fixed_control_bytes >= maximum_control_bytes)
            storageFail(StorageError::Code::LimitExceeded, "schema-mutation recovery transaction exceeds its control byte limit");
        auto prepare = readPrepare(transaction_id, maximum_staged_artifacts, maximum_control_bytes - transaction_fixed_control_bytes);
        if (!prepare)
            replayConflict("schema-mutation recovery has no durable Prepare marker");
        UInt64 retained_control_bytes = checkedConfigurationSizeAdd(
            transaction_fixed_control_bytes,
            getDatabaseSchemaWALPrepareDecodedControlBytes(*prepare),
            StorageError::Code::LimitExceeded,
            "schema-mutation recovery retained control byte count overflows UInt64");
        if (retained_control_bytes > maximum_control_bytes)
            storageFail(StorageError::Code::LimitExceeded, "schema-mutation recovery exceeds its retained control byte limit");
        const auto recovery_decision = readRecoveryDecision(transaction_id, *prepare, maximum_control_bytes - retained_control_bytes);
        std::vector<String> bytes;
        if (recovery_decision != DatabaseSchemaWALRecoveryDecision::RollBackPrepared)
        {
            bytes = loadValidatedStagingManifest(
                transaction_id, *prepare, false, maximum_total_staged_artifact_bytes, maximum_control_bytes - retained_control_bytes);
            retained_control_bytes = checkedConfigurationSizeAdd(
                retained_control_bytes,
                checkedStorageSizeMultiply(
                    static_cast<UInt64>(bytes.capacity()),
                    sizeof(String),
                    "schema-mutation recovery manifest control byte count overflows UInt64"),
                StorageError::Code::LimitExceeded,
                "schema-mutation recovery retained control byte count overflows UInt64");
        }
        if (retained_control_bytes > maximum_control_bytes)
            storageFail(StorageError::Code::LimitExceeded, "schema-mutation recovery exceeds its retained control byte limit");
        auto commit = readCommit(transaction_id, *prepare, maximum_control_bytes - retained_control_bytes);
        return {
            .prepare = std::move(*prepare),
            .commit = std::move(commit),
            .recovery_decision = recovery_decision,
            .staged_artifact_bytes = std::move(bytes),
        };
    }

    std::optional<AtomicDatabaseSchemaMutationRecoveryTransaction> loadCommittedTransactionForAuthorityRepair(
        UInt64 transaction_id,
        UInt64 maximum_total_staged_artifact_bytes,
        UInt64 maximum_staged_artifacts,
        UInt64 maximum_control_bytes) const
    {
        if (maximum_control_bytes == 0)
            storageFail(StorageError::Code::InvalidConfiguration, "authority-repair WAL control byte limit is invalid");
        const String directory = paths.walTransactionDirectory(transaction_id);
        const UInt8 files = validateTransactionDirectory(directory);
        if (!(files & transaction_prepare_file_bit))
            storageFail(StorageError::Code::CorruptDurableState, "authority-repair WAL transaction has no Prepare marker");

        /// The overwhelmingly common incomplete transaction has only Prepare.
        /// Prove the absence of Commit before decoding Prepare or touching its
        /// staging manifest, so it cannot hide an older committed exact source.
        if (!(files & transaction_commit_file_bit) && !(files & transaction_recovery_file_bit))
            return std::nullopt;

        constexpr UInt64 transaction_fixed_control_bytes
            = sizeof(AtomicDatabaseSchemaMutationRecoveryTransaction) - sizeof(DatabaseSchemaWALPrepare);
        if (transaction_fixed_control_bytes >= maximum_control_bytes)
            storageFail(StorageError::Code::LimitExceeded, "authority-repair WAL transaction exceeds its control byte limit");
        auto prepare = readPrepare(transaction_id, maximum_staged_artifacts, maximum_control_bytes - transaction_fixed_control_bytes);
        if (!prepare)
            storageFail(StorageError::Code::CorruptDurableState, "authority-repair WAL transaction lost its Prepare marker");
        UInt64 retained_control_bytes = checkedConfigurationSizeAdd(
            transaction_fixed_control_bytes,
            getDatabaseSchemaWALPrepareDecodedControlBytes(*prepare),
            StorageError::Code::LimitExceeded,
            "authority-repair WAL retained control byte count overflows UInt64");
        if (retained_control_bytes > maximum_control_bytes)
            storageFail(StorageError::Code::LimitExceeded, "authority-repair WAL transaction exceeds its control byte limit");

        const auto recovery_decision = readRecoveryDecision(transaction_id, *prepare, maximum_control_bytes - retained_control_bytes);
        if (recovery_decision == DatabaseSchemaWALRecoveryDecision::RollBackPrepared)
        {
            if (files & transaction_commit_file_bit)
                storageFail(StorageError::Code::CorruptDurableState, "rolled-back authority-repair WAL transaction also has Commit");
            return std::nullopt;
        }
        if (!(files & transaction_commit_file_bit))
        {
            if (recovery_decision == DatabaseSchemaWALRecoveryDecision::CompleteCommitted)
                storageFail(StorageError::Code::CorruptDurableState, "completed authority-repair WAL transaction has no Commit");
            return std::nullopt;
        }

        auto commit = readCommit(transaction_id, *prepare, maximum_control_bytes - retained_control_bytes);
        if (!commit)
            storageFail(StorageError::Code::CorruptDurableState, "authority-repair WAL transaction lost its Commit marker");
        auto bytes = loadValidatedStagingManifest(
            transaction_id, *prepare, false, maximum_total_staged_artifact_bytes, maximum_control_bytes - retained_control_bytes);
        retained_control_bytes = checkedConfigurationSizeAdd(
            retained_control_bytes,
            checkedStorageSizeMultiply(
                static_cast<UInt64>(bytes.capacity()), sizeof(String), "authority-repair WAL manifest control byte count overflows UInt64"),
            StorageError::Code::LimitExceeded,
            "authority-repair WAL retained control byte count overflows UInt64");
        if (retained_control_bytes > maximum_control_bytes)
            storageFail(StorageError::Code::LimitExceeded, "authority-repair WAL transaction exceeds its retained control byte limit");
        return AtomicDatabaseSchemaMutationRecoveryTransaction{
            .prepare = std::move(*prepare),
            .commit = std::move(commit),
            .recovery_decision = recovery_decision,
            .staged_artifact_bytes = std::move(bytes),
        };
    }

    void validateUnpreparedWALTransaction(UInt64 transaction_id) const
    {
        const String directory = paths.walTransactionDirectory(transaction_id);
        const UInt8 files = validateTransactionDirectory(directory);
        if (files & transaction_prepare_file_bit)
            replayConflict("unprepared WAL cleanup found a durable Prepare marker");
        if (files & ~transaction_prepare_temporary_file_bit)
            storageFail(StorageError::Code::CorruptDurableState, "schema-WAL transaction has a marker without Prepare");
        if (files & transaction_prepare_temporary_file_bit)
        {
            if (disk->getFileSize(joinPath(directory, "prepare.wal.prepare.tmp")) > limits.wal.maximum_encoded_bytes)
                storageFail(StorageError::Code::LimitExceeded, "temporary Prepare marker exceeds its byte limit");
        }
    }

    std::vector<UInt64> unpreparedWALTransactionIDs() const
    {
        std::vector<UInt64> result;
        for (const UInt64 transaction_id : numericDirectoryIDs(paths.walDirectory(), "schema-WAL namespace"))
        {
            if (readPrepare(transaction_id))
                continue;
            validateUnpreparedWALTransaction(transaction_id);
            result.push_back(transaction_id);
        }
        return result;
    }

    std::vector<UInt64> transactionIDs(UInt64 maximum_transactions = std::numeric_limits<UInt64>::max()) const
    {
        if (maximum_transactions == 0)
            storageFail(StorageError::Code::InvalidConfiguration, "schema-WAL transaction enumeration limit must be nonzero");
        std::vector<UInt64> result;
        for (const auto & name : listNames(paths.walDirectory(), maximum_transactions))
        {
            const auto id = parseFixedWidthID(name);
            if (!id)
                storageFail(StorageError::Code::UnsafePath, "schema-WAL directory contains a noncanonical transaction name");
            const String path = paths.walTransactionDirectory(*id);
            requireNotSymlink(path);
            if (!disk->existsDirectory(path))
                storageFail(StorageError::Code::UnsafePath, "schema-WAL transaction entry is not a directory");
            validateTransactionDirectory(path);
            if (readPrepare(*id))
                result.push_back(*id);
            else
                validateUnpreparedWALTransaction(*id);
        }
        return result;
    }

    std::optional<std::vector<UInt64>> resumeLightweightTransactionIDs(
        AtomicDatabaseSchemaMutationDurableTransactionDiscovery::Impl & progress,
        const AuthorityRootGraphIdentity & root,
        UInt64 maximum_transactions,
        UInt64 maximum_control_bytes,
        const AuthorityVerificationPassBudget & pass_budget) const
    {
        using Discovery = AtomicDatabaseSchemaMutationDurableTransactionDiscovery::Impl;
        if (maximum_transactions == 0 || maximum_control_bytes == 0 || pass_budget.maximum_work_items == 0)
            storageFail(StorageError::Code::InvalidConfiguration, "schema-WAL resumable discovery limits must be nonzero");
        maximum_transactions = std::min(maximum_transactions, limits.maximum_directory_entries);
        if (root.authority_root.database_uuid != paths.getDatabaseUUID())
            storageFail(StorageError::Code::InvalidConfiguration, "schema-WAL discovery root belongs to another database");
        if (verificationPassBudgetExpired(pass_budget))
            return std::nullopt;
        const UInt64 required_control_bytes = checkedConfigurationSizeAdd(
            atomic_database_schema_mutation_discovery_fixed_control_bytes,
            checkedStorageSizeMultiply(
                maximum_transactions,
                atomic_database_schema_mutation_discovery_control_bytes_per_transaction,
                "schema-WAL discovery control byte count overflows UInt64"),
            StorageError::Code::LimitExceeded,
            "schema-WAL discovery control byte count overflows UInt64");
        if (required_control_bytes > maximum_control_bytes)
            storageFail(StorageError::Code::LimitExceeded, "schema-WAL discovery exceeds its prospective control byte limit");

        const String directory = paths.walDirectory();
        const auto open_namespace = [&]
        {
            requireNotSymlink(directory);
            if (!disk->existsDirectory(directory))
                storageFail(StorageError::Code::UnsafePath, "schema-WAL namespace is not a directory");
            return disk->iterateDirectory(directory);
        };
        if (!progress.storage_identity)
        {
            progress.storage_identity = this;
            progress.root = root;
            progress.maximum_transactions = maximum_transactions;
            progress.maximum_control_bytes = maximum_control_bytes;
            progress.collected.reserve(static_cast<size_t>(maximum_transactions));
            if (progress.collected.capacity() > maximum_transactions)
                storageFail(StorageError::Code::LimitExceeded, "schema-WAL discovery allocator exceeded its admitted capacity");
            progress.iterator = open_namespace();
        }
        else if (
            progress.storage_identity != this || !progress.root || *progress.root != root
            || progress.maximum_transactions != maximum_transactions || progress.maximum_control_bytes != maximum_control_bytes)
        {
            storageFail(StorageError::Code::CorruptDurableState, "schema-WAL discovery continuation changed its sealed root or limits");
        }
        if (progress.phase == Discovery::Phase::Complete)
            storageFail(StorageError::Code::CorruptDurableState, "schema-WAL discovery continuation was consumed twice");

        const auto inspect_entry = [&](const String & name)
        {
            if (name.empty() || name.size() > 256 || name == "." || name == ".." || name.find('/') != String::npos)
                storageFail(StorageError::Code::UnsafePath, "schema-WAL namespace contains an unsafe entry");
            const auto id = parseFixedWidthID(name);
            if (!id)
                storageFail(StorageError::Code::UnsafePath, "schema-WAL directory contains a noncanonical transaction name");
            const String path = paths.walTransactionDirectory(*id);
            requireNotSymlink(path);
            if (!disk->existsDirectory(path))
                storageFail(StorageError::Code::UnsafePath, "schema-WAL transaction entry is not a directory");
            const UInt8 files = validateTransactionDirectory(path);
            const bool prepared = (files & transaction_prepare_file_bit) != 0;
            if (!prepared)
                validateUnpreparedWALTransaction(*id);
            return Discovery::Entry{.transaction_id = *id, .prepared = prepared};
        };
        const auto entry_less
            = [](const Discovery::Entry & lhs, const Discovery::Entry & rhs) { return lhs.transaction_id < rhs.transaction_id; };
        const auto heap_less = [&](const Discovery::RunCursor & lhs, const Discovery::RunCursor & rhs)
        { return progress.collected[lhs.current].transaction_id > progress.collected[rhs.current].transaction_id; };

        UInt64 work_items = 0;
        while (work_items < pass_budget.maximum_work_items && !verificationPassBudgetExpired(pass_budget))
        {
            if (progress.phase == Discovery::Phase::Collect)
            {
                if (progress.iterator->isValid())
                {
                    if (progress.namespace_entries >= maximum_transactions)
                        storageFail(StorageError::Code::LimitExceeded, "schema-WAL transaction discovery exceeds its count limit");
                    progress.collected.push_back(inspect_entry(progress.iterator->name()));
                    ++progress.namespace_entries;
                    if (progress.collected.back().prepared)
                        ++progress.durable_entries;
                    progress.iterator->next();
                    if (progress.collected.size() % Discovery::sorted_run_size == 0)
                    {
                        std::sort(progress.collected.end() - Discovery::sorted_run_size, progress.collected.end(), entry_less);
                    }
                    ++work_items;
                    continue;
                }

                const size_t tail = progress.collected.size() % Discovery::sorted_run_size;
                if (tail)
                    std::sort(progress.collected.end() - tail, progress.collected.end(), entry_less);
                progress.iterator.reset();
                progress.sorted.reserve(progress.collected.size());
                const size_t run_count = (progress.collected.size() + Discovery::sorted_run_size - 1) / Discovery::sorted_run_size;
                progress.merge_heap.reserve(run_count);
                if (progress.sorted.capacity() > progress.collected.size() || progress.merge_heap.capacity() > run_count)
                    storageFail(StorageError::Code::LimitExceeded, "schema-WAL discovery allocator exceeded its admitted merge capacity");
                progress.phase = Discovery::Phase::PrepareMerge;
                continue;
            }

            if (progress.phase == Discovery::Phase::PrepareMerge)
            {
                if (progress.next_run_begin < progress.collected.size())
                {
                    const size_t end = std::min(progress.collected.size(), progress.next_run_begin + Discovery::sorted_run_size);
                    progress.merge_heap.push_back({.current = progress.next_run_begin, .end = end});
                    std::push_heap(progress.merge_heap.begin(), progress.merge_heap.end(), heap_less);
                    progress.next_run_begin = end;
                    ++work_items;
                    continue;
                }
                progress.phase = Discovery::Phase::Merge;
                continue;
            }

            if (progress.phase == Discovery::Phase::Merge)
            {
                if (!progress.merge_heap.empty())
                {
                    std::pop_heap(progress.merge_heap.begin(), progress.merge_heap.end(), heap_less);
                    auto run = progress.merge_heap.back();
                    progress.merge_heap.pop_back();
                    const auto entry = progress.collected[run.current++];
                    if (!progress.sorted.empty() && progress.sorted.back().transaction_id == entry.transaction_id)
                        storageFail(StorageError::Code::CorruptDurableState, "schema-WAL namespace contains duplicate transaction IDs");
                    progress.sorted.push_back(entry);
                    if (run.current != run.end)
                    {
                        progress.merge_heap.push_back(run);
                        std::push_heap(progress.merge_heap.begin(), progress.merge_heap.end(), heap_less);
                    }
                    ++work_items;
                    continue;
                }
                std::vector<Discovery::Entry>().swap(progress.collected);
                std::vector<Discovery::RunCursor>().swap(progress.merge_heap);
                progress.iterator = open_namespace();
                progress.phase = Discovery::Phase::Validate;
                continue;
            }

            if (progress.phase == Discovery::Phase::Validate)
            {
                if (progress.iterator->isValid())
                {
                    if (progress.validation_entries >= progress.sorted.size())
                        storageFail(StorageError::Code::CorruptDurableState, "schema-WAL namespace changed during discovery");
                    const auto entry = inspect_entry(progress.iterator->name());
                    const auto expected = std::lower_bound(
                        progress.sorted.begin(),
                        progress.sorted.end(),
                        entry.transaction_id,
                        [](const Discovery::Entry & item, UInt64 id) { return item.transaction_id < id; });
                    if (expected == progress.sorted.end() || expected->transaction_id != entry.transaction_id
                        || expected->prepared != entry.prepared)
                    {
                        storageFail(StorageError::Code::CorruptDurableState, "schema-WAL namespace changed during discovery");
                    }
                    ++progress.validation_entries;
                    progress.iterator->next();
                    ++work_items;
                    continue;
                }
                if (progress.validation_entries != progress.sorted.size())
                    storageFail(StorageError::Code::CorruptDurableState, "schema-WAL namespace changed during discovery");
                progress.iterator.reset();
                progress.durable_ids.reserve(static_cast<size_t>(progress.durable_entries));
                if (progress.durable_ids.capacity() > progress.durable_entries)
                    storageFail(StorageError::Code::LimitExceeded, "schema-WAL discovery result allocator exceeded its admitted capacity");
                progress.phase = Discovery::Phase::BuildResult;
                continue;
            }

            if (progress.phase == Discovery::Phase::BuildResult)
            {
                if (progress.build_result_index < progress.sorted.size())
                {
                    const auto & entry = progress.sorted[progress.build_result_index++];
                    if (entry.prepared)
                        progress.durable_ids.push_back(entry.transaction_id);
                    ++work_items;
                    continue;
                }
                std::vector<Discovery::Entry>().swap(progress.sorted);
                progress.phase = Discovery::Phase::Complete;
                return std::move(progress.durable_ids);
            }
        }
        return std::nullopt;
    }

    std::vector<UInt64> numericDirectoryIDs(std::string_view directory, std::string_view description) const
    {
        std::vector<UInt64> result;
        for (const auto & name : listNames(directory))
        {
            const auto id = parseFixedWidthID(name);
            if (!id)
                storageFail(StorageError::Code::UnsafePath, String(description) + " contains a noncanonical identifier");
            const String path = joinPath(directory, name);
            requireNotSymlink(path);
            if (!disk->existsDirectory(path))
                storageFail(StorageError::Code::UnsafePath, String(description) + " contains a non-directory entry");
            result.push_back(*id);
        }
        return result;
    }

    struct CheckpointDirectoryState
    {
        UInt64 checkpoint_id = 0;
        std::optional<DatabaseSchemaWALCheckpoint> checkpoint;
    };

    DatabaseSchemaWALCheckpoint readCheckpointRecordFromDirectory(UInt64 checkpoint_id, std::string_view directory) const
    {
        const String record_bytes = readRequiredFile(
            joinPath(directory, "checkpoint.wal"), std::min(limits.wal.maximum_encoded_bytes, maximum_checkpoint_record_bytes));
        auto checkpoint = decodeDatabaseSchemaWALCheckpoint(record_bytes, limits.wal);
        if (checkpoint.checkpoint_id != checkpoint_id || checkpoint.authority_state.database_uuid != paths.getDatabaseUUID())
            storageFail(StorageError::Code::CorruptDurableState, "schema-WAL checkpoint is stored under another identity");
        return checkpoint;
    }

    DatabaseSchemaWALCheckpoint readCheckpointRecord(UInt64 checkpoint_id) const
    {
        return readCheckpointRecordFromDirectory(checkpoint_id, paths.checkpointDirectory(checkpoint_id));
    }

    std::vector<CheckpointDirectoryState> checkpointDirectoryStates() const
    {
        const std::set<String> allowed_names{
            "checkpoint.wal",
            "checkpoint.wal.checkpoint.tmp",
            "inventory.snapshot",
            "inventory.snapshot.inventory.tmp",
            "schema_graph.snapshot",
            "schema_graph.snapshot.graph.tmp",
        };
        const std::set<String> durable_names{"checkpoint.wal", "inventory.snapshot", "schema_graph.snapshot"};

        std::vector<CheckpointDirectoryState> result;
        for (const auto & name : listNames(paths.checkpointsDirectory(), limits.maximum_checkpoint_namespace_entries))
        {
            const auto checkpoint_id = parseFixedWidthID(name);
            if (!checkpoint_id)
                storageFail(StorageError::Code::UnsafePath, "checkpoint directory contains a noncanonical checkpoint name");
            const String directory = paths.checkpointDirectory(*checkpoint_id);
            requireNotSymlink(directory);
            if (!disk->existsDirectory(directory))
                storageFail(StorageError::Code::UnsafePath, "checkpoint entry is not a directory");

            const auto names = validateFlatDirectory(directory, allowed_names);
            ensureRegularFileOrAbsent(paths.checkpointRecordPath(*checkpoint_id));
            CheckpointDirectoryState state{
                .checkpoint_id = *checkpoint_id,
                .checkpoint = std::nullopt,
            };
            if (disk->existsFile(paths.checkpointRecordPath(*checkpoint_id)))
            {
                if (std::set<String>(names.begin(), names.end()) != durable_names)
                    storageFail(StorageError::Code::CorruptDurableState, "durable checkpoint directory contains temporary artifacts");
                state.checkpoint = readCheckpointRecord(*checkpoint_id);
            }
            result.push_back(std::move(state));
        }
        return result;
    }

    struct RetiredCheckpointNamespaceEntries
    {
        std::vector<UInt64> transaction_checkpoint_ids;
        std::vector<UInt64> checkpoint_image_ids;
    };

    RetiredCheckpointNamespaceEntries retiredCheckpointNamespaceEntries() const
    {
        RetiredCheckpointNamespaceEntries result;
        for (const auto & name : listNames(paths.retiredCheckpointDirectory(), limits.maximum_checkpoint_namespace_entries))
        {
            const auto transaction_checkpoint_id = parseFixedWidthID(name);
            const auto checkpoint_image_id = parseRetiredCheckpointImageName(name);
            if (!transaction_checkpoint_id && !checkpoint_image_id)
                storageFail(StorageError::Code::UnsafePath, "retired checkpoint namespace contains a noncanonical entry");
            const String path = joinPath(paths.retiredCheckpointDirectory(), name);
            requireNotSymlink(path);
            if (!disk->existsDirectory(path))
                storageFail(StorageError::Code::UnsafePath, "retired checkpoint namespace contains a non-directory entry");
            if (transaction_checkpoint_id)
                result.transaction_checkpoint_ids.push_back(*transaction_checkpoint_id);
            else
                result.checkpoint_image_ids.push_back(*checkpoint_image_id);
        }
        return result;
    }

    AtomicDatabaseSchemaMutationCheckpointImage loadCheckpoint(UInt64 checkpoint_id) const
    {
        const String directory = paths.checkpointDirectory(checkpoint_id);
        const std::set<String> durable_names{"checkpoint.wal", "inventory.snapshot", "schema_graph.snapshot"};
        const auto names = validateFlatDirectory(directory, durable_names);
        if (std::set<String>(names.begin(), names.end()) != durable_names)
            storageFail(StorageError::Code::CorruptDurableState, "durable checkpoint directory differs from its exact record set");
        auto checkpoint = readCheckpointRecord(checkpoint_id);
        String inventory = readRequiredFile(paths.checkpointInventorySnapshotPath(checkpoint_id), limits.wal.maximum_encoded_bytes);
        String graph = readRequiredFile(paths.checkpointSchemaGraphSnapshotPath(checkpoint_id), limits.wal.maximum_encoded_bytes);
        static_cast<void>(DatabaseSchemaWALCheckpointBuilder::validateDecoded(checkpoint, inventory, graph, limits.wal));
        return {
            .checkpoint = std::move(checkpoint),
            .inventory_snapshot_bytes = std::move(inventory),
            .schema_graph_snapshot_bytes = std::move(graph),
        };
    }

    std::optional<DatabaseSchemaWALCheckpoint> latestCheckpointRecord() const
    {
        std::optional<DatabaseSchemaWALCheckpoint> latest_record;
        for (auto & state : checkpointDirectoryStates())
        {
            if (!state.checkpoint)
                continue;
            if (!latest_record
                || std::tie(state.checkpoint->covered_commit.transaction_id, state.checkpoint->checkpoint_id)
                    > std::tie(latest_record->covered_commit.transaction_id, latest_record->checkpoint_id))
                latest_record = std::move(state.checkpoint);
        }
        return latest_record;
    }

    std::optional<AtomicDatabaseSchemaMutationCheckpointImage> latestCheckpoint() const
    {
        auto latest_record = latestCheckpointRecord();
        if (!latest_record)
            return std::nullopt;
        return loadCheckpoint(latest_record->checkpoint_id);
    }

    struct CheckpointMaintenancePlan
    {
        std::optional<DatabaseSchemaWALCheckpoint> latest_checkpoint;
        std::optional<DatabaseSchemaWALCommit> checkpoint_commit;
        std::optional<DatabaseSchemaWALExactRepairProvenance> latest_exact_repair_provenance;
        bool resume_compaction = false;
        bool sweep_checkpoint_images = false;

        bool needsMaintenance() const noexcept { return checkpoint_commit || resume_compaction || sweep_checkpoint_images; }
    };

    CheckpointMaintenancePlan checkpointMaintenancePlan() const
    {
        if (recovery_required_transaction.load(std::memory_order_acquire))
            replayConflict("checkpoint maintenance cannot cross a schema-mutation recovery latch");

        CheckpointMaintenancePlan plan;
        plan.latest_checkpoint = latestCheckpointRecord();
        if (plan.latest_checkpoint)
            plan.latest_exact_repair_provenance = plan.latest_checkpoint->last_exact_repair_provenance;
        const UInt64 covered_transaction_id = plan.latest_checkpoint ? plan.latest_checkpoint->covered_commit.transaction_id : 0;
        UInt64 committed_tail = 0;
        for (const auto & name : listNames(paths.walDirectory()))
        {
            const auto transaction_id = parseFixedWidthID(name);
            if (!transaction_id)
                storageFail(StorageError::Code::UnsafePath, "schema-WAL directory contains a noncanonical transaction name");
            const String directory = paths.walTransactionDirectory(*transaction_id);
            requireNotSymlink(directory);
            if (!disk->existsDirectory(directory))
                storageFail(StorageError::Code::UnsafePath, "schema-WAL transaction entry is not a directory");
            validateTransactionDirectory(directory);

            const auto prepare = readPrepare(*transaction_id);
            if (!prepare)
            {
                validateUnpreparedWALTransaction(*transaction_id);
                continue;
            }

            const auto commit = readCommit(*transaction_id, *prepare);
            if (!commit)
                replayConflict("checkpoint maintenance encountered an unresolved prepared transaction");
            if (const auto recovery_bytes = readOptionalFile(joinPath(directory, "recovery.bin"), internal_record_overhead))
            {
                const auto recovery = decodeRecoveryDecisionEnvelopeInternal(*recovery_bytes);
                if (recovery.database_uuid != paths.getDatabaseUUID() || recovery.transaction_id != *transaction_id
                    || recovery.prepare_hash != commit->prepare_hash
                    || recovery.decision != DatabaseSchemaWALRecoveryDecision::CompleteCommitted)
                {
                    storageFail(
                        StorageError::Code::CorruptDurableState,
                        "checkpoint maintenance encountered an inconsistent committed recovery marker");
                }
            }

            if (*transaction_id <= covered_transaction_id)
            {
                plan.resume_compaction = true;
                continue;
            }
            /// A provenance-free exact repair is a valid legacy V1 record. It
            /// supersedes, rather than inherits, any older summary: reporting
            /// the preceding repair as the latest one after restart would be
            /// observably false and would survive the next compaction.
            if (prepare->exact_repair)
                plan.latest_exact_repair_provenance = prepare->exact_repair_provenance;
            ++committed_tail;
            plan.checkpoint_commit = *commit;
        }

        if (committed_tail < checkpoint_transaction_interval)
            plan.checkpoint_commit.reset();
        const auto retired = retiredCheckpointNamespaceEntries();
        if (!retired.transaction_checkpoint_ids.empty())
            plan.resume_compaction = true;
        const auto checkpoints = checkpointDirectoryStates();
        const size_t retained_checkpoint_count = plan.latest_checkpoint ? 1 : 0;
        plan.sweep_checkpoint_images = checkpoints.size() > retained_checkpoint_count || !retired.checkpoint_image_ids.empty();
        if (plan.resume_compaction && !plan.latest_checkpoint && !plan.checkpoint_commit)
            storageFail(StorageError::Code::CorruptDurableState, "checkpoint compaction progress has no durable checkpoint");
        return plan;
    }

    UInt64 durableHighWater() const
    {
        UInt64 result = 0;
        if (const auto bytes = readOptionalFile(paths.highWaterMarkPath(), internal_record_overhead))
            result = decodeAtomicDatabaseSchemaMutationHighWaterMark(*bytes, paths.getDatabaseUUID());
        if (const auto checkpoint = latestCheckpointRecord())
            result = std::max(result, checkpoint->covered_commit.transaction_id);
        for (const UInt64 transaction_id : transactionIDs())
            result = std::max(result, transaction_id);
        for (const UInt64 transaction_id : numericDirectoryIDs(paths.retiredRollbackDirectory(), "retired rollback namespace"))
        {
            static_cast<void>(validateRetiredRollback(transaction_id));
            result = std::max(result, transaction_id);
        }
        const auto retired_checkpoints = retiredCheckpointNamespaceEntries();
        for (const UInt64 checkpoint_id : retired_checkpoints.transaction_checkpoint_ids)
        {
            const auto checkpoint = readCheckpointRecord(checkpoint_id);
            const String directory = parentPath(paths.retiredCheckpointTransactionDirectory(checkpoint_id, 1));
            for (const UInt64 transaction_id : numericDirectoryIDs(directory, "retired checkpoint transaction namespace"))
            {
                static_cast<void>(validateRetiredCheckpointTransaction(checkpoint_id, transaction_id, checkpoint));
                result = std::max(result, transaction_id);
            }
        }
        for (const UInt64 checkpoint_id : retired_checkpoints.checkpoint_image_ids)
        {
            if (const auto checkpoint = validateRetiredCheckpointImage(checkpoint_id))
                result = std::max(result, checkpoint->covered_commit.transaction_id);
        }
        return result;
    }

    void persistHighWater(UInt64 requested)
    {
        UInt64 current = 0;
        if (const auto bytes = readOptionalFile(paths.highWaterMarkPath(), internal_record_overhead))
            current = decodeAtomicDatabaseSchemaMutationHighWaterMark(*bytes, paths.getDatabaseUUID());

        bool removed_temporary = false;
        const UInt64 cleanup_temporaries_through = current >= requested ? current : requested - 1;
        for (const auto & name : listNames(paths.authorityDirectory()))
        {
            const auto temporary_id = parseHighWaterTemporaryName(name);
            if (!temporary_id || *temporary_id > cleanup_temporaries_through)
                continue;
            const String temporary_path = joinPath(paths.authorityDirectory(), name);
            ensureRegularFileOrAbsent(temporary_path);
            disk->removeFile(temporary_path);
            removed_temporary = true;
        }
        if (removed_temporary)
            syncDirectory(paths.authorityDirectory());
        if (current >= requested)
            return;
        const String encoded = encodeAtomicDatabaseSchemaMutationHighWaterMark(paths.getDatabaseUUID(), requested);
        replaceMutableFile(paths.highWaterMarkPath(), encoded, fixedWidthID(requested));
    }

    std::optional<AuthorityState> currentAuthorityState() const
    {
        auto checkpoint = latestCheckpointRecord();
        UInt64 covered = 0;
        std::optional<AuthorityState> result;
        if (checkpoint)
        {
            covered = checkpoint->covered_commit.transaction_id;
            result = checkpoint->authority_state;
        }
        for (const UInt64 transaction_id : transactionIDs())
        {
            if (transaction_id <= covered)
                continue;
            auto transaction = loadTransaction(transaction_id);
            const auto decision = readRecoveryDecision(transaction_id, transaction.prepare);
            if (transaction.prepare.before_authority_state != result)
                storageFail(StorageError::Code::CorruptDurableState, "schema-WAL transaction chain has a stale preceding authority state");
            if (transaction.commit)
            {
                if (decision == DatabaseSchemaWALRecoveryDecision::RollBackPrepared)
                    storageFail(StorageError::Code::CorruptDurableState, "committed schema-WAL transaction is marked rolled back");
                result = transaction.prepare.after_authority_state;
            }
            else if (decision == DatabaseSchemaWALRecoveryDecision::CompleteCommitted)
            {
                storageFail(StorageError::Code::CorruptDurableState, "uncommitted schema-WAL transaction is marked complete");
            }
        }
        return result;
    }

    AtomicDatabaseSchemaMutationReconciliation reconcileAuthorityRecords(
        const AuthorityInventory & anchored_inventory,
        const SchemaObjectDependencyGraph & anchored_graph,
        std::span<const SchemaObjectID> retained_dependent_objects = {},
        bool retain_all_images = true) const
    {
        if (!hasDurableAuthorityMarker())
            storageFail(StorageError::Code::CorruptDurableState, "Atomic authority records have no durable authority marker");
        if (anchored_graph.getDatabaseUUID() != paths.getDatabaseUUID())
            storageFail(StorageError::Code::CorruptDurableState, "anchored schema graph belongs to another database");
        if (!retain_all_images)
        {
            if (retained_dependent_objects.empty())
                storageFail(StorageError::Code::InvalidConfiguration, "targeted authority reconciliation has an empty object scope");
            for (size_t index = 0; index < retained_dependent_objects.size(); ++index)
            {
                const auto & object = retained_dependent_objects[index];
                if (!object.isValid() || object.database_uuid != paths.getDatabaseUUID()
                    || (object.kind != SchemaObjectKind::SyntheticTestObject && object.kind != SchemaObjectKind::Table
                        && object.kind != SchemaObjectKind::View && object.kind != SchemaObjectKind::Dictionary))
                    storageFail(
                        StorageError::Code::InvalidConfiguration, "targeted authority reconciliation has an invalid object identity");
                if (index && !(retained_dependent_objects[index - 1] < object))
                    storageFail(
                        StorageError::Code::InvalidConfiguration,
                        "targeted authority reconciliation object identities are not in strict canonical order");
            }
        }

        const String authority_databases_directory = parentPath(paths.authorityDirectory());
        const String authority_namespace_directory = parentPath(authority_databases_directory);
        const std::set<String> expected_authority_namespace{"databases"};
        const auto authority_namespace_names = listNames(authority_namespace_directory);
        if (std::set<String>(authority_namespace_names.begin(), authority_namespace_names.end()) != expected_authority_namespace)
            storageFail(StorageError::Code::CorruptDurableState, "Atomic authority namespace contains an unknown format version");
        const std::set<String> expected_database_directory{uuidPathComponent(paths.getDatabaseUUID())};
        const auto database_names = listNames(authority_databases_directory);
        if (std::set<String>(database_names.begin(), database_names.end()) != expected_database_directory)
            storageFail(StorageError::Code::CorruptDurableState, "Atomic authority namespace contains another database identity");

        const std::set<String> allowed_authority_entries{
            "checkpoints",
            "expectations",
            "retired",
            "staging",
            "synthetic",
            "transaction_high_water.bin",
            "wal",
        };
        for (const auto & name : listNames(paths.authorityDirectory()))
        {
            if (!allowed_authority_entries.contains(name))
                storageFail(StorageError::Code::CorruptDurableState, "Atomic authority directory contains an unknown entry");
            const String path = joinPath(paths.authorityDirectory(), name);
            requireNotSymlink(path);
            const bool is_high_water = name == "transaction_high_water.bin";
            if ((is_high_water && !disk->existsFile(path)) || (!is_high_water && !disk->existsDirectory(path)))
                storageFail(StorageError::Code::UnsafePath, "Atomic authority directory entry has the wrong file type");
        }

        std::set<String> expected_type_names{".authority"};
        std::set<String> expected_expectation_names;
        std::set<String> expected_synthetic_names;
        std::set<UUID> expected_definition_uuids;
        std::set<UUID> expected_synthetic_uuids;
        std::set<UUID> expected_table_uuids;
        std::set<UUID> loaded_synthetic_metadata_uuids;
        std::set<UUID> loaded_table_metadata_uuids;
        std::set<String> mapped_table_names;
        std::set<String> mapped_table_metadata_paths;
        for (const auto & object : anchored_graph.getNodes())
        {
            if (object.database_uuid != paths.getDatabaseUUID())
                storageFail(StorageError::Code::CorruptDurableState, "anchored schema graph contains a foreign object");
            switch (object.kind)
            {
                case SchemaObjectKind::TypeDefinition: expected_definition_uuids.insert(object.object_uuid); break;
                case SchemaObjectKind::SyntheticTestObject:
                    expected_synthetic_uuids.insert(object.object_uuid);
                    expected_synthetic_names.insert(uuidPathComponent(object.object_uuid) + ".metadata");
                    break;
                case SchemaObjectKind::View:
                case SchemaObjectKind::Dictionary:
                case SchemaObjectKind::Table: expected_table_uuids.insert(object.object_uuid); break;
            }
        }
        std::set<UUID> actual_definition_uuids;
        AtomicDatabaseSchemaMutationReconciliation result;
        if (retain_all_images)
            result.authority_records.reserve(anchored_inventory.getLeaves().size());
        else
            result.dependent_objects.reserve(retained_dependent_objects.size());
        UInt64 total_authority_bytes = 0;
        UInt64 total_dependent_bytes = 0;
        const auto account_dependent_bytes = [&](size_t byte_count)
        {
            if (byte_count > limits.maximum_total_durable_dependent_object_bytes - total_dependent_bytes)
                storageFail(StorageError::Code::LimitExceeded, "Atomic dependent-object bytes exceed their aggregate limit");
            total_dependent_bytes += byte_count;
        };
        for (const auto & leaf : anchored_inventory.getLeaves())
        {
            const String path = paths.authorityRecordPath(leaf.key);
            const String name = std::filesystem::path(path).filename().generic_string();
            UInt64 maximum = 0;
            switch (leaf.key.record_kind)
            {
                case AuthorityInventoryRecordKind::TypeDefinition:
                    expected_type_names.insert(name);
                    actual_definition_uuids.insert(leaf.key.object_uuid);
                    maximum = limits.wal.definition_record.maximum_record_bytes;
                    break;
                case AuthorityInventoryRecordKind::SidecarExpectation:
                    expected_expectation_names.insert(name);
                    maximum = limits.wal.maximum_staged_artifact_bytes;
                    break;
            }
            String canonical_bytes = readRequiredFile(path, maximum);
            if (canonical_bytes.size() > limits.maximum_total_authority_record_bytes - total_authority_bytes)
                storageFail(StorageError::Code::LimitExceeded, "Atomic authority record bytes exceed their aggregate limit");
            total_authority_bytes += canonical_bytes.size();

            switch (leaf.key.record_kind)
            {
                /// Definition envelopes deliberately remain raw here.  The
                /// recovery decoder owns keyed RecordError classification, so
                /// malformed bytes become one precise INVALID diagnostic
                /// instead of escaping reconciliation or collapsing the whole
                /// catalog into an unkeyed INCOMPLETE state.
                case AuthorityInventoryRecordKind::TypeDefinition: break;
                case AuthorityInventoryRecordKind::SidecarExpectation: {
                    SidecarExpectationRecord expectation;
                    try
                    {
                        expectation = decodeSidecarExpectationRecord(canonical_bytes);
                    }
                    catch (const SidecarExpectationRecordError &)
                    {
                        storageFail(StorageError::Code::CorruptDurableState, "sidecar expectation record is not canonical supported bytes");
                    }
                    const bool synthetic = expectation.object.kind == SchemaObjectKind::SyntheticTestObject;
                    const bool durable_object = expectation.object.kind == SchemaObjectKind::Table
                        || expectation.object.kind == SchemaObjectKind::View || expectation.object.kind == SchemaObjectKind::Dictionary;
                    const bool expected_object = (synthetic && expected_synthetic_uuids.contains(expectation.object.object_uuid))
                        || (durable_object && expected_table_uuids.contains(expectation.object.object_uuid));
                    if ((!synthetic && !durable_object) || expectation.object.database_uuid != paths.getDatabaseUUID()
                        || expectation.object.object_uuid != leaf.key.object_uuid || !expected_object
                        || expectation.object_schema_revision != leaf.object_revision
                        || computeSidecarExpectationRecordHash(expectation) != leaf.canonical_record_hash)
                        storageFail(StorageError::Code::CorruptDurableState, "expectation record differs from its anchored inventory leaf");

                    const String uuid = uuidPathComponent(expectation.object.object_uuid);
                    String object_name;
                    String sidecar_path;
                    String metadata_path;
                    String installation_bytes;
                    std::optional<Digest> expected_metadata_hash;
                    if (synthetic)
                    {
                        if (expectation.installation_record_hash)
                            storageFail(
                                StorageError::Code::CorruptDurableState,
                                "synthetic expectation unexpectedly names a metadata installation record");
                        expected_synthetic_names.insert(uuid + ".metadata");
                        expected_synthetic_names.insert(uuid + ".references");
                        sidecar_path = joinPath(joinPath(paths.authorityDirectory(), "synthetic"), uuid + ".references");
                        metadata_path = joinPath(joinPath(paths.authorityDirectory(), "synthetic"), uuid + ".metadata");
                        loaded_synthetic_metadata_uuids.insert(expectation.object.object_uuid);
                    }
                    else
                    {
                        if (!expectation.installation_record_hash)
                            storageFail(
                                StorageError::Code::CorruptDurableState,
                                "ordinary dependent-object expectation has no metadata installation record hash");
                        expected_expectation_names.insert(uuid + ".references");
                        expected_expectation_names.insert(uuid + ".installation");
                        const String installation_path = paths.metadataInstallationRecordPath(expectation.object);
                        installation_bytes = readRequiredFile(
                            installation_path,
                            std::min(limits.wal.maximum_staged_artifact_bytes, limits.wal.installation_record.maximum_encoded_bytes));
                        account_dependent_bytes(installation_bytes.size());
                        DependentObjectMetadataInstallationRecord installation;
                        try
                        {
                            installation
                                = decodeDependentObjectMetadataInstallationRecord(installation_bytes, limits.wal.installation_record);
                        }
                        catch (const DependentObjectMetadataInstallationRecordError &)
                        {
                            storageFail(
                                StorageError::Code::CorruptDurableState,
                                "ordinary dependent-object metadata installation record is invalid");
                        }
                        if (installation.object != expectation.object
                            || installation.object_schema_revision != expectation.object_schema_revision
                            || computeDependentObjectMetadataInstallationRecordHash(installation, limits.wal.installation_record)
                                != *expectation.installation_record_hash)
                            storageFail(
                                StorageError::Code::CorruptDurableState,
                                "ordinary dependent-object metadata installation record differs from its anchored expectation");
                        object_name = installation.object_name;
                        expected_metadata_hash = installation.metadata_artifact_hash;
                        if (!mapped_table_names.insert(object_name).second)
                            storageFail(
                                StorageError::Code::CorruptDurableState,
                                "two anchored dependent objects map to the same ordinary metadata name");
                        sidecar_path = paths.tableReferencesPath(expectation.object);
                        metadata_path = paths.tableMetadataPath(object_name);
                        if (!mapped_table_metadata_paths.insert(metadata_path).second)
                            storageFail(
                                StorageError::Code::CorruptDurableState,
                                "two anchored dependent objects map to the same ordinary metadata path");
                        loaded_table_metadata_uuids.insert(expectation.object.object_uuid);
                    }

                    String sidecar_bytes = readRequiredFile(sidecar_path, limits.wal.maximum_staged_artifact_bytes);
                    account_dependent_bytes(sidecar_bytes.size());
                    PersistedTypeReferences references;
                    try
                    {
                        references = decodePersistedTypeReferences(sidecar_bytes, limits.wal.persisted_references);
                    }
                    catch (const PersistedTypeReferencesError &)
                    {
                        storageFail(StorageError::Code::CorruptDurableState, "dependent sidecar is not canonical supported bytes");
                    }
                    if (references.object != expectation.object || references.object_schema_revision != expectation.object_schema_revision
                        || references.physical_schema_fingerprint != expectation.physical_schema_fingerprint
                        || computePersistedTypeReferencesSidecarHash(references, limits.wal.persisted_references)
                            != expectation.sidecar_hash)
                        storageFail(StorageError::Code::CorruptDurableState, "dependent sidecar differs from its anchored expectation");

                    String metadata_bytes = readRequiredFile(metadata_path, limits.wal.maximum_staged_artifact_bytes);
                    account_dependent_bytes(metadata_bytes.size());
                    if (durable_object)
                    {
                        if (computeDatabaseSchemaWALStagedArtifactHash(
                                DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, metadata_bytes)
                            != *expected_metadata_hash)
                            storageFail(
                                StorageError::Code::CorruptDurableState,
                                "ordinary dependent-object metadata differs from its installed content address");
                    }
                    if (retain_all_images
                        || std::binary_search(retained_dependent_objects.begin(), retained_dependent_objects.end(), expectation.object))
                    {
                        result.dependent_objects.push_back({
                            .expectation = expectation,
                            .object_name = std::move(object_name),
                            .canonical_metadata_bytes = std::move(metadata_bytes),
                            .canonical_sidecar_bytes = std::move(sidecar_bytes),
                            .canonical_installation_record_bytes = std::move(installation_bytes),
                        });
                    }
                    break;
                }
            }
            if (retain_all_images)
                result.authority_records.push_back({.key = leaf.key, .canonical_bytes = std::move(canonical_bytes)});
        }
        if (actual_definition_uuids != expected_definition_uuids)
            storageFail(StorageError::Code::CorruptDurableState, "definition inventory and anchored schema graph differ");
        for (const UUID object_uuid : expected_synthetic_uuids)
        {
            if (loaded_synthetic_metadata_uuids.contains(object_uuid))
                continue;
            const String metadata_path
                = joinPath(joinPath(paths.authorityDirectory(), "synthetic"), uuidPathComponent(object_uuid) + ".metadata");
            const String metadata_bytes = readRequiredFile(metadata_path, limits.wal.maximum_staged_artifact_bytes);
            account_dependent_bytes(metadata_bytes.size());
        }
        if (loaded_table_metadata_uuids != expected_table_uuids)
            storageFail(
                StorageError::Code::CorruptDurableState, "anchored dependent-object graph and metadata-installation expectations differ");

        const auto type_names = listNames(paths.typesDirectory());
        if (std::set<String>(type_names.begin(), type_names.end()) != expected_type_names)
            storageFail(StorageError::Code::CorruptDurableState, "types directory differs from the anchored definition set");

        const String expectations_directory = joinPath(paths.authorityDirectory(), "expectations");
        const auto expectation_names = listNames(expectations_directory);
        if (std::set<String>(expectation_names.begin(), expectation_names.end()) != expected_expectation_names)
            storageFail(StorageError::Code::CorruptDurableState, "expectation directory differs from the anchored expectation set");

        const String synthetic_directory = joinPath(paths.authorityDirectory(), "synthetic");
        const auto synthetic_names = listNames(synthetic_directory);
        if (std::set<String>(synthetic_names.begin(), synthetic_names.end()) != expected_synthetic_names)
            storageFail(StorageError::Code::CorruptDurableState, "synthetic metadata directory differs from the anchored expectation set");
        if (!retain_all_images && result.dependent_objects.size() != retained_dependent_objects.size())
            storageFail(
                StorageError::Code::CorruptDurableState, "targeted authority reconciliation did not resolve its complete object scope");
        return result;
    }

    void removeEmptyOwnedDirectory(std::string_view directory)
    {
        requireNotSymlink(directory);
        if (!disk->existsFileOrDirectory(String(directory)))
            return;
        if (!disk->existsDirectory(String(directory)))
            storageFail(StorageError::Code::UnsafePath, "owned schema-mutation directory is occupied by a non-directory");
        if (!listNames(directory).empty())
            storageFail(StorageError::Code::CorruptDurableState, "owned schema-mutation directory is unexpectedly nonempty");
        disk->removeDirectory(String(directory));
        syncDirectory(parentPath(directory));
    }

    bool isExactEmptyNeverEnabledScaffold() const
    {
        const String authority_namespace = joinPath(paths.typesDirectory(), ".authority");
        const String authority_databases_directory = parentPath(paths.authorityDirectory());
        const auto validate_layer = [&](std::string_view directory, const std::set<String> & allowed_names)
        {
            requireNotSymlink(directory);
            if (!disk->existsFileOrDirectory(String(directory)))
                return false;
            if (!disk->existsDirectory(String(directory)))
                storageFail(StorageError::Code::UnsafePath, "never-enabled authority scaffold contains a non-directory layer");
            for (const auto & name : listNames(directory))
            {
                if (!allowed_names.contains(name))
                    storageFail(StorageError::Code::CorruptDurableState, "never-enabled authority scaffold contains an unknown entry");
            }
            return true;
        };

        if (!validate_layer(authority_namespace, {"databases"}))
            return true;
        if (!disk->existsFileOrDirectory(authority_databases_directory))
            return true;
        if (!validate_layer(authority_databases_directory, {uuidPathComponent(paths.getDatabaseUUID())}))
            return true;
        if (!disk->existsFileOrDirectory(paths.authorityDirectory()))
            return true;
        const std::set<String> allowed_authority_directories{"checkpoints", "expectations", "retired", "staging", "synthetic", "wal"};
        const auto authority_entries = listNames(paths.authorityDirectory());
        for (const auto & name : authority_entries)
        {
            if (name == "transaction_high_water.bin" || parseHighWaterTemporaryName(name))
                return false;
            if (!allowed_authority_directories.contains(name))
                storageFail(StorageError::Code::CorruptDurableState, "never-enabled authority scaffold contains an unknown entry");
            const String child = joinPath(paths.authorityDirectory(), name);
            requireNotSymlink(child);
            if (!disk->existsDirectory(child))
                storageFail(StorageError::Code::UnsafePath, "never-enabled authority scaffold child is not a directory");
            if (name == "retired")
            {
                const auto retired_entries = listNames(child);
                for (const auto & retired_name : retired_entries)
                {
                    if (retired_name != "checkpoint" && retired_name != "rollback")
                        storageFail(StorageError::Code::CorruptDurableState, "never-enabled retired scaffold contains an unknown entry");
                    const String retired_child = joinPath(child, retired_name);
                    requireNotSymlink(retired_child);
                    if (!disk->existsDirectory(retired_child))
                        storageFail(StorageError::Code::UnsafePath, "never-enabled retired scaffold child is not a directory");
                    if (!listNames(retired_child).empty())
                        return false;
                }
                continue;
            }
            if (!listNames(child).empty())
                return false;
        }
        return true;
    }

    bool cleanupNeverEnabledScaffold()
    {
        if (activationMarkerTransactionID() || hasActivationMarkerTemporary())
            return false;

        const String authority_namespace = joinPath(paths.typesDirectory(), ".authority");
        requireNotSymlink(authority_namespace);
        if (!disk->existsFileOrDirectory(authority_namespace))
        {
            requireNotSymlink(paths.typesDirectory());
            if (!disk->existsFileOrDirectory(paths.typesDirectory()))
            {
                bool had_configuration = false;
                for (const String & path :
                     {paths.udtConfigurationV2Path(),
                      paths.udtConfigurationV2TemporaryPath(),
                      paths.verificationSchedulerOverrideV2Path(),
                      paths.verificationSchedulerOverrideV2TemporaryPath(),
                      paths.resourceQuotaOverrideV2Path(),
                      paths.resourceQuotaOverrideV2TemporaryPath()})
                {
                    ensureRegularFileOrAbsent(path);
                    had_configuration = had_configuration || disk->existsFile(path);
                }
                removeNeverActivatedConfigurationFiles();
                return had_configuration;
            }
            if (!disk->existsDirectory(paths.typesDirectory()))
                storageFail(StorageError::Code::UnsafePath, "Atomic types namespace is not a directory");
            if (!listNames(paths.typesDirectory()).empty())
                return false;
            removeNeverActivatedConfigurationFiles();
            removeEmptyOwnedDirectory(paths.typesDirectory());
            return true;
        }
        if (!disk->existsDirectory(authority_namespace))
            storageFail(StorageError::Code::UnsafePath, "Atomic authority namespace is occupied by a non-directory");
        if (!isExactEmptyNeverEnabledScaffold())
            return false;

        removeNeverActivatedConfigurationFiles();
        const String authority_databases_directory = parentPath(paths.authorityDirectory());
        removeEmptyOwnedDirectory(paths.checkpointsDirectory());
        removeEmptyOwnedDirectory(paths.walDirectory());
        removeEmptyOwnedDirectory(paths.stagingDirectory());
        removeEmptyOwnedDirectory(joinPath(paths.authorityDirectory(), "expectations"));
        removeEmptyOwnedDirectory(joinPath(paths.authorityDirectory(), "synthetic"));
        removeEmptyOwnedDirectory(paths.retiredRollbackDirectory());
        removeEmptyOwnedDirectory(paths.retiredCheckpointDirectory());
        removeEmptyOwnedDirectory(paths.retiredDirectory());
        removeEmptyOwnedDirectory(paths.authorityDirectory());
        removeEmptyOwnedDirectory(authority_databases_directory);
        removeEmptyOwnedDirectory(authority_namespace);
        if (listNames(paths.typesDirectory()).empty())
            removeEmptyOwnedDirectory(paths.typesDirectory());
        return true;
    }

    bool hasDurableAuthorityMarker() const
    {
        String current;
        for (const auto & component : std::filesystem::path(paths.getMetadataRoot()))
        {
            current = joinPath(current, component.generic_string());
            requireNotSymlink(current);
            if (!disk->existsFileOrDirectory(current))
                return false;
            if (!disk->existsDirectory(current))
                storageFail(StorageError::Code::UnsafePath, "Atomic database metadata root contains a non-directory component");
        }

        const auto activation_marker = activationMarkerState();
        const auto activation_temporary = activationMarkerTemporaryState();
        const bool recoverable_marker_upgrade = isRecoverableActivationMarkerV2Upgrade(activation_marker, activation_temporary);
        ensureRegularFileOrAbsent(paths.udtConfigurationV2Path());
        ensureRegularFileOrAbsent(paths.udtConfigurationV2TemporaryPath());
        ensureRegularFileOrAbsent(paths.verificationSchedulerOverrideV2Path());
        ensureRegularFileOrAbsent(paths.verificationSchedulerOverrideV2TemporaryPath());
        ensureRegularFileOrAbsent(paths.resourceQuotaOverrideV2Path());
        ensureRegularFileOrAbsent(paths.resourceQuotaOverrideV2TemporaryPath());
        ensureRegularFileOrAbsent(paths.verificationCursorPath());
        ensureRegularFileOrAbsent(paths.verificationCursorTemporaryPath());
        const bool has_verification_cursor = disk->existsFile(paths.verificationCursorPath());
        const bool has_verification_cursor_temporary = disk->existsFile(paths.verificationCursorTemporaryPath());
        if (activation_marker && activation_temporary && !recoverable_marker_upgrade)
            storageFail(StorageError::Code::CorruptDurableState, "Atomic authority activation marker retained its temporary file");
        if ((has_verification_cursor || has_verification_cursor_temporary) && !activation_marker)
            storageFail(StorageError::Code::CorruptDurableState, "authority verification cursor exists without an activation marker");
        if ((activation_marker && activation_marker->requires_combined_configuration) || recoverable_marker_upgrade)
        {
            if (!disk->existsFile(paths.udtConfigurationV2Path()))
                storageFail(
                    StorageError::Code::CorruptDurableState,
                    "Atomic authority activation marker requires a durable combined UDT configuration");
        }

        const String types = paths.typesDirectory();
        requireNotSymlink(types);
        if (!disk->existsFileOrDirectory(types))
        {
            if (activation_marker || activation_temporary)
                storageFail(StorageError::Code::CorruptDurableState, "Atomic authority activation marker has no types namespace");
            return false;
        }
        if (!disk->existsDirectory(types))
            storageFail(StorageError::Code::UnsafePath, "Atomic types namespace is not a directory");

        const String authority_namespace = joinPath(types, ".authority");
        requireNotSymlink(authority_namespace);
        if (!disk->existsFileOrDirectory(authority_namespace))
        {
            if (activation_marker || activation_temporary)
                storageFail(StorageError::Code::CorruptDurableState, "Atomic authority activation marker has no authority namespace");
            return false;
        }
        if (!disk->existsDirectory(authority_namespace))
            storageFail(StorageError::Code::UnsafePath, "Atomic authority namespace is not a directory");
        if (isExactEmptyNeverEnabledScaffold())
        {
            if (activation_marker || activation_temporary)
                storageFail(StorageError::Code::CorruptDurableState, "Atomic authority activation marker has only an empty scaffold");
            return false;
        }

        const std::array required_directories{
            parentPath(parentPath(paths.authorityDirectory())),
            parentPath(paths.authorityDirectory()),
            paths.authorityDirectory(),
            paths.stagingDirectory(),
            paths.walDirectory(),
            paths.checkpointsDirectory(),
            paths.retiredDirectory(),
            paths.retiredRollbackDirectory(),
            paths.retiredCheckpointDirectory(),
        };
        for (const auto & directory : required_directories)
        {
            requireNotSymlink(directory);
            if (!disk->existsDirectory(directory))
                storageFail(StorageError::Code::CorruptDurableState, "Atomic authority namespace is present but incomplete");
        }

        const auto authority_namespace_names = listNames(parentPath(parentPath(paths.authorityDirectory())));
        if (authority_namespace_names != std::vector<String>{"databases"})
            storageFail(StorageError::Code::CorruptDurableState, "Atomic authority namespace has an unknown format version");
        const auto database_names = listNames(parentPath(paths.authorityDirectory()));
        if (database_names != std::vector<String>{uuidPathComponent(paths.getDatabaseUUID())})
            storageFail(StorageError::Code::CorruptDurableState, "Atomic authority namespace has an unknown database identity");

        const std::set<String> allowed_authority_directories{"checkpoints", "expectations", "retired", "staging", "synthetic", "wal"};
        std::vector<UInt64> high_water_temporary_ids;
        for (const auto & name : listNames(paths.authorityDirectory()))
        {
            const String path = joinPath(paths.authorityDirectory(), name);
            requireNotSymlink(path);
            if (allowed_authority_directories.contains(name))
            {
                if (!disk->existsDirectory(path))
                    storageFail(StorageError::Code::UnsafePath, "Atomic authority directory entry is not a directory");
                continue;
            }
            if (name == "transaction_high_water.bin")
            {
                if (!disk->existsFile(path))
                    storageFail(StorageError::Code::UnsafePath, "Atomic high-water entry is not a regular file");
                continue;
            }
            if (const auto transaction_id = parseHighWaterTemporaryName(name))
            {
                if (!disk->existsFile(path))
                    storageFail(StorageError::Code::UnsafePath, "Atomic high-water temporary entry is not a regular file");
                if (disk->getFileSize(path) > internal_record_overhead)
                    storageFail(StorageError::Code::LimitExceeded, "Atomic high-water temporary entry exceeds its byte limit");
                high_water_temporary_ids.push_back(*transaction_id);
                continue;
            }
            storageFail(StorageError::Code::CorruptDurableState, "Atomic authority directory contains an unknown entry");
        }
        const bool has_high_water = readOptionalFile(paths.highWaterMarkPath(), internal_record_overhead).has_value();
        const bool has_transaction = !transactionIDs().empty();
        const bool has_unprepared_wal = !unpreparedWALTransactionIDs().empty();
        const bool has_checkpoint = latestCheckpointRecord().has_value();
        const bool has_current_state = currentAuthorityState().has_value();
        const auto staged_transaction_ids = numericDirectoryIDs(paths.stagingDirectory(), "staging namespace");
        for (const UInt64 transaction_id : staged_transaction_ids)
            validateStagingTransaction(transaction_id);
        const bool has_staging = !staged_transaction_ids.empty();
        const auto retired_rollback_ids = numericDirectoryIDs(paths.retiredRollbackDirectory(), "retired rollback namespace");
        for (const UInt64 transaction_id : retired_rollback_ids)
            static_cast<void>(validateRetiredRollback(transaction_id));
        const auto retired_checkpoints = retiredCheckpointNamespaceEntries();
        for (const UInt64 checkpoint_id : retired_checkpoints.transaction_checkpoint_ids)
        {
            const auto checkpoint = readCheckpointRecord(checkpoint_id);
            const String directory = parentPath(paths.retiredCheckpointTransactionDirectory(checkpoint_id, 1));
            for (const UInt64 transaction_id : numericDirectoryIDs(directory, "retired checkpoint transaction namespace"))
                static_cast<void>(validateRetiredCheckpointTransaction(checkpoint_id, transaction_id, checkpoint));
        }
        for (const UInt64 checkpoint_id : retired_checkpoints.checkpoint_image_ids)
            static_cast<void>(validateRetiredCheckpointImage(checkpoint_id));
        const bool has_retired = !retired_rollback_ids.empty() || !retired_checkpoints.transaction_checkpoint_ids.empty()
            || !retired_checkpoints.checkpoint_image_ids.empty();
        const UInt64 durable_high_water = durableHighWater();
        if (activation_marker && activation_marker->activation_transaction_id > durable_high_water)
            storageFail(StorageError::Code::CorruptDurableState, "Atomic authority activation marker is ahead of durable history");
        if (has_current_state && (!activation_marker || (activation_temporary && !recoverable_marker_upgrade)))
            storageFail(StorageError::Code::CorruptDurableState, "committed Atomic authority state has no complete activation marker");
        if (std::any_of(
                high_water_temporary_ids.begin(),
                high_water_temporary_ids.end(),
                [&](UInt64 transaction_id) { return transaction_id > durable_high_water; }))
            storageFail(StorageError::Code::CorruptDurableState, "Atomic high-water temporary entry has no durable transaction");
        if (!has_high_water && !has_transaction && !has_unprepared_wal && !has_checkpoint && !has_current_state && !has_staging
            && !has_retired)
            storageFail(StorageError::Code::CorruptDurableState, "Atomic authority namespace has no durable authority evidence");
        return true;
    }

    bool hasCompleteDurableActivationMarker() const
    {
        /// Do not inspect the temporary marker when the durable image is
        /// absent. A valid temporary-only marker is expected after a crash in
        /// first activation and is owned by WAL rollback, not active-policy
        /// reconciliation.
        return activationMarkerState().has_value();
    }

    bool hasBoundedDurableAuthorityHead() const
    {
        String current;
        for (const auto & component : std::filesystem::path(paths.getMetadataRoot()))
        {
            current = joinPath(current, component.generic_string());
            requireNotSymlink(current);
            if (!disk->existsFileOrDirectory(current))
                return false;
            if (!disk->existsDirectory(current))
                storageFail(StorageError::Code::UnsafePath, "Atomic database metadata root contains a non-directory component");
        }

        const auto activation_marker = activationMarkerState();
        const auto activation_temporary = activationMarkerTemporaryState();
        const bool recoverable_marker_upgrade = isRecoverableActivationMarkerV2Upgrade(activation_marker, activation_temporary);
        if (activation_temporary && !recoverable_marker_upgrade)
        {
            storageFail(
                StorageError::Code::CorruptDurableState,
                activation_marker ? "Atomic authority activation marker retained its temporary file"
                                  : "Atomic authority activation marker is incomplete");
        }
        if (!activation_marker)
            return false;

        ensureRegularFileOrAbsent(paths.udtConfigurationV2Path());
        ensureRegularFileOrAbsent(paths.udtConfigurationV2TemporaryPath());
        ensureRegularFileOrAbsent(paths.verificationSchedulerOverrideV2Path());
        ensureRegularFileOrAbsent(paths.verificationSchedulerOverrideV2TemporaryPath());
        ensureRegularFileOrAbsent(paths.resourceQuotaOverrideV2Path());
        ensureRegularFileOrAbsent(paths.resourceQuotaOverrideV2TemporaryPath());
        if ((activation_marker->requires_combined_configuration || recoverable_marker_upgrade)
            && !disk->existsFile(paths.udtConfigurationV2Path()))
        {
            storageFail(
                StorageError::Code::CorruptDurableState,
                "Atomic authority activation marker requires a durable combined UDT configuration");
        }

        const std::array required_directories{
            paths.typesDirectory(),
            parentPath(parentPath(paths.authorityDirectory())),
            parentPath(paths.authorityDirectory()),
            paths.authorityDirectory(),
            paths.stagingDirectory(),
            paths.walDirectory(),
            paths.checkpointsDirectory(),
            paths.retiredDirectory(),
            paths.retiredRollbackDirectory(),
            paths.retiredCheckpointDirectory(),
        };
        for (const auto & directory : required_directories)
        {
            requireNotSymlink(directory);
            if (!disk->existsDirectory(directory))
                storageFail(StorageError::Code::CorruptDurableState, "Atomic authority durable head is incomplete");
        }
        return true;
    }

    bool hasUnresolvedPrepare() const
    {
        for (const UInt64 transaction_id : transactionIDs())
        {
            auto prepare = readPrepare(transaction_id);
            if (!prepare)
                continue;
            if (readCommit(transaction_id, *prepare))
                continue;
            if (readRecoveryDecision(transaction_id, *prepare) != DatabaseSchemaWALRecoveryDecision::RollBackPrepared)
                return true;
        }
        return false;
    }

    StagedEnvelope findSelectedImage(const DatabaseSchemaWALStagedArtifactRef & selected) const
    {
        if (!active_transaction_id)
            replayConflict("artifact installation has no active transaction");
        const auto prepare = readPrepare(*active_transaction_id);
        if (!prepare)
            replayConflict("artifact installation has no durable Prepare marker");
        std::optional<StagedEnvelope> result;
        for (size_t ordinal = 0; ordinal < prepare->staged_artifacts.size(); ++ordinal)
        {
            if (prepare->staged_artifacts[ordinal] != selected)
                continue;
            if (result)
                replayConflict("artifact installation selected a duplicate durable image");
            result = loadStagedEnvelope(*active_transaction_id, ordinal);
            if (result->artifact != selected)
                storageFail(StorageError::Code::CorruptDurableState, "selected staged image differs from its Prepare manifest");
        }
        if (!result)
            replayConflict("artifact installation is not selected by the durable Prepare manifest");
        return std::move(*result);
    }

    std::optional<StagedEnvelope> findOppositeImage(const DatabaseSchemaWALStagedArtifactRef & selected) const
    {
        if (!active_transaction_id)
            replayConflict("artifact installation has no active transaction");
        auto prepare = readPrepare(*active_transaction_id);
        if (!prepare)
            replayConflict("artifact installation has no durable Prepare marker");
        std::optional<StagedEnvelope> result;
        for (size_t ordinal = 0; ordinal < prepare->staged_artifacts.size(); ++ordinal)
        {
            const auto & candidate = prepare->staged_artifacts[ordinal];
            if (candidate.kind == selected.kind && candidate.object == selected.object && candidate.image != selected.image)
            {
                if (result)
                    replayConflict("artifact installation has duplicate opposite durable images");
                result = loadStagedEnvelope(*active_transaction_id, ordinal);
                if (result->artifact != candidate)
                    storageFail(StorageError::Code::CorruptDurableState, "opposite staged image differs from its Prepare manifest");
            }
        }
        return result;
    }

    std::optional<StagedEnvelope> findOnlyImage(DatabaseSchemaWALStagedArtifactKind kind, const SchemaObjectID & object) const
    {
        if (!active_transaction_id)
            replayConflict("artifact removal has no active transaction");
        auto prepare = readPrepare(*active_transaction_id);
        if (!prepare)
            replayConflict("artifact removal has no durable Prepare marker");
        std::optional<StagedEnvelope> result;
        for (size_t ordinal = 0; ordinal < prepare->staged_artifacts.size(); ++ordinal)
        {
            const auto & candidate = prepare->staged_artifacts[ordinal];
            if (candidate.kind != kind || candidate.object != object)
                continue;
            if (result)
                replayConflict("artifact removal selected an identity having two staged images");
            result = loadStagedEnvelope(*active_transaction_id, ordinal);
            if (result->artifact != candidate)
                storageFail(StorageError::Code::CorruptDurableState, "removed staged image differs from its Prepare manifest");
        }
        if (!result)
            replayConflict("artifact removal is not selected by the durable Prepare manifest");
        return result;
    }

    DependentObjectMetadataInstallationRecord decodeTableInstallationEnvelope(const StagedEnvelope & envelope) const
    {
        if (envelope.artifact.kind != DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord
            || !isOrdinaryDependentObjectKind(envelope.artifact.object.kind))
            replayConflict("dependent-object metadata installation selected a non-storage mapping artifact");

        DependentObjectMetadataInstallationRecord record;
        try
        {
            record = decodeDependentObjectMetadataInstallationRecord(envelope.canonical_bytes, limits.wal.installation_record);
        }
        catch (const DependentObjectMetadataInstallationRecordError &)
        {
            storageFail(StorageError::Code::CorruptDurableState, "durable table metadata installation record is invalid");
        }
        if (record.object != envelope.artifact.object || record.object_schema_revision != envelope.artifact.revision)
            replayConflict("table metadata installation record differs from its durable artifact identity");
        return record;
    }

    std::optional<DependentObjectMetadataInstallationRecord> findTableInstallationForImage(
        const DatabaseSchemaWALStagedArtifactRef & selected_metadata, DatabaseSchemaWALStagedArtifactImage image) const
    {
        if (selected_metadata.kind != DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata
            || !isOrdinaryDependentObjectKind(selected_metadata.object.kind))
            replayConflict("mapped dependent-object metadata path requires an ordinary metadata artifact");
        if (!active_transaction_id)
            replayConflict("mapped table metadata path has no active transaction");
        const auto prepare = readPrepare(*active_transaction_id);
        if (!prepare)
            replayConflict("mapped table metadata path has no durable Prepare marker");

        std::optional<StagedEnvelope> installation;
        std::optional<DatabaseSchemaWALStagedArtifactRef> image_metadata;
        for (size_t ordinal = 0; ordinal < prepare->staged_artifacts.size(); ++ordinal)
        {
            const auto & candidate = prepare->staged_artifacts[ordinal];
            if (candidate.object != selected_metadata.object || candidate.image != image)
                continue;
            if (candidate.kind == DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata)
            {
                if (image_metadata)
                    replayConflict("mapped table metadata has duplicate same-image metadata artifacts");
                image_metadata = candidate;
            }
            else if (candidate.kind == DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord)
            {
                if (installation)
                    replayConflict("mapped table metadata has duplicate same-image installation records");
                installation = loadStagedEnvelope(*active_transaction_id, ordinal);
                if (installation->artifact != candidate)
                    storageFail(
                        StorageError::Code::CorruptDurableState, "table metadata installation image differs from its Prepare manifest");
            }
        }
        if (!installation)
            return std::nullopt;
        if (!image_metadata)
            replayConflict("mapped table metadata installation has no same-image metadata artifact");
        if (image == selected_metadata.image && *image_metadata != selected_metadata)
            replayConflict("selected table metadata differs from its durable Prepare image");

        auto record = decodeTableInstallationEnvelope(*installation);
        if (record.object_schema_revision != image_metadata->revision || record.metadata_artifact_hash != image_metadata->content_hash)
            replayConflict("table metadata installation record does not address its same-image metadata artifact");
        return record;
    }

    DependentObjectMetadataInstallationRecord findTableInstallationForMetadata(const DatabaseSchemaWALStagedArtifactRef & metadata) const
    {
        const auto selected = findTableInstallationForImage(metadata, metadata.image);
        const auto opposite_image = metadata.image == DatabaseSchemaWALStagedArtifactImage::Before
            ? DatabaseSchemaWALStagedArtifactImage::After
            : DatabaseSchemaWALStagedArtifactImage::Before;
        const auto opposite = findTableInstallationForImage(metadata, opposite_image);
        if (!selected && !opposite)
            replayConflict("table metadata transition has no mapped logical image");
        return selected ? *selected : *opposite;
    }

    String canonicalArtifactTarget(const StagedEnvelope & envelope) const
    {
        if (!isOrdinaryDependentObjectKind(envelope.artifact.object.kind))
            return paths.canonicalArtifactPath(envelope.artifact.kind, envelope.artifact.object);
        if (envelope.artifact.kind == DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata)
            return paths.tableMetadataPath(findTableInstallationForMetadata(envelope.artifact).object_name);
        if (envelope.artifact.kind == DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord)
            static_cast<void>(decodeTableInstallationEnvelope(envelope));
        return paths.canonicalArtifactPath(envelope.artifact.kind, envelope.artifact.object);
    }

    std::vector<String> validateFlatDirectory(std::string_view directory, const std::set<String> & allowed_names) const
    {
        if (!disk->existsFileOrDirectory(String(directory)))
            return {};
        requireNotSymlink(directory);
        if (!disk->existsDirectory(String(directory)))
            storageFail(StorageError::Code::UnsafePath, "owned schema-mutation path is not a directory");
        const auto names = listNames(directory, std::max<UInt64>(1, allowed_names.size()));
        for (const auto & name : names)
        {
            if (!allowed_names.contains(name))
                storageFail(StorageError::Code::UnsafePath, "owned schema-mutation directory contains an unknown entry");
            const String path = joinPath(directory, name);
            requireNotSymlink(path);
            if (!disk->existsFile(path))
                storageFail(StorageError::Code::UnsafePath, "owned schema-mutation directory contains a non-file entry");
        }
        return names;
    }

    void removeValidatedFlatDirectory(std::string_view directory, const std::set<String> & allowed_names)
    {
        const auto names = validateFlatDirectory(directory, allowed_names);
        if (!disk->existsFileOrDirectory(String(directory)))
            return;
        for (const auto & name : names)
            disk->removeFile(joinPath(directory, name));
        disk->removeDirectory(String(directory));
    }

    void removeStagingTransaction(UInt64 transaction_id)
    {
        const String directory = paths.stagingTransactionDirectory(transaction_id);
        std::set<String> allowed;
        for (const auto & name : listNames(directory))
        {
            if (!isCanonicalStagedArtifactName(name))
                storageFail(StorageError::Code::UnsafePath, "staging directory contains a noncanonical entry");
            allowed.insert(name);
        }
        removeValidatedFlatDirectory(directory, allowed);
    }

    void removeWALTransaction(UInt64 transaction_id) { removeTransactionDirectory(paths.walTransactionDirectory(transaction_id)); }

    void removeTransactionDirectory(std::string_view directory)
    {
        const std::set<String> allowed{
            "prepare.wal",
            "prepare.wal.prepare.tmp",
            "commit.wal",
            "commit.wal.commit.tmp",
            "recovery.bin",
            "recovery.bin.recovery.tmp",
        };
        const auto names_vector = validateFlatDirectory(directory, allowed);
        if (!disk->existsFileOrDirectory(String(directory)))
            return;
        const std::set<String> names(names_vector.begin(), names_vector.end());
        for (const auto name : transaction_file_removal_order)
        {
            if (names.contains(String(name)))
                disk->removeFile(joinPath(directory, name));
        }
        disk->removeDirectory(String(directory));
    }

    UInt8 validateTransactionDirectory(std::string_view directory) const
    {
        if (!disk->existsFileOrDirectory(String(directory)))
            return 0;
        requireNotSymlink(directory);
        if (!disk->existsDirectory(String(directory)))
            storageFail(StorageError::Code::UnsafePath, "schema-WAL transaction path is not a directory");

        UInt8 result = 0;
        UInt64 entries = 0;
        auto iterator = disk->iterateDirectory(String(directory));
        while (iterator->isValid())
        {
            if (entries >= transaction_file_removal_order.size())
                storageFail(StorageError::Code::UnsafePath, "schema-WAL transaction directory contains too many entries");
            const String name = iterator->name();
            if (name.empty() || name.size() > 256 || name == "." || name == ".." || name.find('/') != String::npos)
                storageFail(StorageError::Code::UnsafePath, "schema-WAL transaction directory contains an unsafe entry");
            const auto allowed = std::find(transaction_file_removal_order.begin(), transaction_file_removal_order.end(), name);
            if (allowed == transaction_file_removal_order.end())
                storageFail(StorageError::Code::UnsafePath, "schema-WAL transaction directory contains an unknown entry");
            const size_t index = static_cast<size_t>(allowed - transaction_file_removal_order.begin());
            const UInt8 bit = static_cast<UInt8>(1U << index);
            if (result & bit)
                storageFail(StorageError::Code::CorruptDurableState, "schema-WAL transaction directory contains a duplicate entry");
            const String path = joinPath(directory, name);
            requireNotSymlink(path);
            if (!disk->existsFile(path))
                storageFail(StorageError::Code::UnsafePath, "schema-WAL transaction directory contains a non-file entry");
            result |= bit;
            ++entries;
            iterator->next();
        }
        return result;
    }

    void removeWALTransactionTemporaries(UInt64 transaction_id)
    {
        const String directory = paths.walTransactionDirectory(transaction_id);
        if (!disk->existsFileOrDirectory(directory))
            return;
        validateTransactionDirectory(directory);
        bool removed = false;
        for (const auto name : {"recovery.bin.recovery.tmp", "commit.wal.commit.tmp", "prepare.wal.prepare.tmp"})
        {
            const String path = joinPath(directory, name);
            ensureRegularFileOrAbsent(path);
            if (!disk->existsFile(path))
                continue;
            disk->removeFile(path);
            removed = true;
        }
        if (removed)
            syncDirectory(directory);
    }

    bool transactionDirectoryMatchesExactOrDeletionProgress(std::string_view source, std::string_view destination) const
    {
        validateTransactionDirectory(source);
        validateTransactionDirectory(destination);
        const auto source_names_vector = listNames(source);
        const auto destination_names_vector = listNames(destination);
        const std::set<String> source_names(source_names_vector.begin(), source_names_vector.end());
        std::set<String> remaining_destination_names(destination_names_vector.begin(), destination_names_vector.end());
        bool recognized_state = source_names == remaining_destination_names;
        for (const auto name : transaction_file_removal_order)
        {
            if (recognized_state)
                break;
            remaining_destination_names.erase(String(name));
            recognized_state = source_names == remaining_destination_names;
        }
        if (!recognized_state)
            return false;
        for (const auto & name : source_names)
        {
            const UInt64 maximum_entry_bytes
                = name.starts_with("recovery.bin") ? internal_record_overhead : limits.wal.maximum_encoded_bytes;
            if (readRequiredFile(joinPath(source, name), maximum_entry_bytes)
                != readRequiredFile(joinPath(destination, name), maximum_entry_bytes))
                return false;
        }
        return true;
    }

    std::optional<DatabaseSchemaWALCheckpoint> validateRetiredCheckpointImage(UInt64 checkpoint_id) const
    {
        const String directory = paths.retiredCheckpointImageDirectory(checkpoint_id);
        const std::set<String> allowed{
            "checkpoint.wal",
            "checkpoint.wal.checkpoint.tmp",
            "inventory.snapshot",
            "inventory.snapshot.inventory.tmp",
            "schema_graph.snapshot",
            "schema_graph.snapshot.graph.tmp",
        };
        const auto names_vector = validateFlatDirectory(directory, allowed);
        const std::set<String> names(names_vector.begin(), names_vector.end());
        if (!names.contains("checkpoint.wal"))
            return std::nullopt;

        const std::set<String> complete{"checkpoint.wal", "inventory.snapshot", "schema_graph.snapshot"};
        const std::set<String> inventory_removed{"checkpoint.wal", "schema_graph.snapshot"};
        const std::set<String> snapshots_removed{"checkpoint.wal"};
        if (names != complete && names != inventory_removed && names != snapshots_removed)
            storageFail(StorageError::Code::CorruptDurableState, "retired checkpoint image is outside its exact deletion sequence");
        return readCheckpointRecordFromDirectory(checkpoint_id, directory);
    }

    void removeRetiredCheckpointImage(UInt64 checkpoint_id)
    {
        const String directory = paths.retiredCheckpointImageDirectory(checkpoint_id);
        static_cast<void>(validateRetiredCheckpointImage(checkpoint_id));
        for (const auto & name :
             {"checkpoint.wal.checkpoint.tmp",
              "schema_graph.snapshot.graph.tmp",
              "inventory.snapshot.inventory.tmp",
              "inventory.snapshot",
              "schema_graph.snapshot",
              "checkpoint.wal"})
        {
            const String path = joinPath(directory, name);
            ensureRegularFileOrAbsent(path);
            if (!disk->existsFile(path))
                continue;
            disk->removeFile(path);
            syncDirectory(directory);
        }
        if (!listNames(directory).empty())
            storageFail(StorageError::Code::CorruptDurableState, "retired checkpoint image cleanup left an unknown entry");
        disk->removeDirectory(directory);
        syncDirectory(paths.retiredCheckpointDirectory());
    }

    void retireCheckpointImage(UInt64 checkpoint_id)
    {
        const String source = paths.checkpointDirectory(checkpoint_id);
        const String destination = paths.retiredCheckpointImageDirectory(checkpoint_id);
        const bool source_exists = disk->existsFileOrDirectory(source);
        const bool destination_exists = disk->existsFileOrDirectory(destination);
        if (source_exists && destination_exists)
            storageFail(StorageError::Code::CorruptDurableState, "live and retired checkpoint images coexist");
        if (!source_exists && !destination_exists)
            return;
        if (source_exists)
        {
            requireNotSymlink(source);
            if (!disk->existsDirectory(source))
                storageFail(StorageError::Code::UnsafePath, "checkpoint image retirement source is not a directory");
            ensureDirectory(paths.retiredCheckpointDirectory());
            requireNotSymlink(destination);
            disk->moveDirectory(source, destination);
            syncDirectory(paths.retiredCheckpointDirectory());
            syncDirectory(paths.checkpointsDirectory());
        }
        static_cast<void>(validateRetiredCheckpointImage(checkpoint_id));
    }

    void retireAndSweepObsoleteCheckpointImages(std::optional<UInt64> retained_checkpoint_id)
    {
        auto retired_entries = retiredCheckpointNamespaceEntries();
        auto live_checkpoints = checkpointDirectoryStates();
        size_t live_index = 0;
        size_t retired_index = 0;
        while (live_index < live_checkpoints.size() && retired_index < retired_entries.checkpoint_image_ids.size())
        {
            const UInt64 live_id = live_checkpoints[live_index].checkpoint_id;
            const UInt64 retired_id = retired_entries.checkpoint_image_ids[retired_index];
            if (live_id == retired_id)
                storageFail(StorageError::Code::CorruptDurableState, "live and retired checkpoint images coexist");
            if (live_id < retired_id)
                ++live_index;
            else
                ++retired_index;
        }
        for (const UInt64 checkpoint_id : retired_entries.checkpoint_image_ids)
            removeRetiredCheckpointImage(checkpoint_id);

        for (const auto & state : live_checkpoints)
        {
            if (retained_checkpoint_id && state.checkpoint_id == *retained_checkpoint_id)
                continue;
            retireCheckpointImage(state.checkpoint_id);
            removeRetiredCheckpointImage(state.checkpoint_id);
        }
    }

    void
    validateRetiredCheckpointTransaction(UInt64 checkpoint_id, UInt64 transaction_id, const DatabaseSchemaWALCheckpoint & checkpoint) const
    {
        const String directory = paths.retiredCheckpointTransactionDirectory(checkpoint_id, transaction_id);
        const std::set<String> allowed{"prepare.wal", "commit.wal", "recovery.bin"};
        const auto names_vector = validateFlatDirectory(directory, allowed);
        const std::set<String> names(names_vector.begin(), names_vector.end());
        if (transaction_id > checkpoint.covered_commit.transaction_id)
            storageFail(StorageError::Code::CorruptDurableState, "checkpoint-retired transaction is newer than its checkpoint");
        if (names.empty())
            return;

        const auto prepare = readPrepareFromDirectory(transaction_id, directory);
        const auto commit = readStandaloneCommitFromDirectory(transaction_id, directory);
        if ((prepare && !commit) || (!prepare && names != std::set<String>{"commit.wal"}))
            storageFail(StorageError::Code::CorruptDurableState, "checkpoint-retired cleanup state is invalid");
        if (prepare && commit && !commitMatchesPrepare(*commit, *prepare))
            storageFail(StorageError::Code::CorruptDurableState, "checkpoint-retired Commit does not bind its Prepare marker");
        if (transaction_id == checkpoint.covered_commit.transaction_id && commit && *commit != checkpoint.covered_commit)
            storageFail(StorageError::Code::CorruptDurableState, "checkpoint-retired transaction is not covered by its checkpoint");
        if (const auto recovery_bytes = readOptionalFile(joinPath(directory, "recovery.bin"), internal_record_overhead))
        {
            const auto recovery = decodeRecoveryDecisionEnvelopeInternal(*recovery_bytes);
            if (recovery.database_uuid != paths.getDatabaseUUID() || recovery.transaction_id != transaction_id
                || recovery.decision != DatabaseSchemaWALRecoveryDecision::CompleteCommitted
                || (prepare && recovery.prepare_hash != prepare->prepare_hash) || (commit && recovery.prepare_hash != commit->prepare_hash))
                storageFail(StorageError::Code::CorruptDurableState, "checkpoint-retired recovery marker is inconsistent");
        }
    }

    void removeRetiredCheckpointTransaction(UInt64 checkpoint_id, UInt64 transaction_id)
    {
        const String directory = paths.retiredCheckpointTransactionDirectory(checkpoint_id, transaction_id);
        for (const auto & name : {"recovery.bin", "prepare.wal", "commit.wal"})
        {
            const String path = joinPath(directory, name);
            ensureRegularFileOrAbsent(path);
            if (!disk->existsFile(path))
                continue;
            disk->removeFile(path);
            syncDirectory(directory);
        }
        if (disk->existsFileOrDirectory(directory))
        {
            if (!listNames(directory).empty())
                storageFail(StorageError::Code::CorruptDurableState, "checkpoint-retired cleanup left an unknown entry");
            disk->removeDirectory(directory);
            syncDirectory(parentPath(directory));
        }
    }

    void moveWALTransactionTo(UInt64 transaction_id, std::string_view destination)
    {
        const String source = paths.walTransactionDirectory(transaction_id);
        const bool source_exists = disk->existsFileOrDirectory(source);
        const bool destination_exists = disk->existsFileOrDirectory(String(destination));
        if (source_exists && destination_exists)
        {
            if (!transactionDirectoryMatchesExactOrDeletionProgress(source, destination))
                storageFail(StorageError::Code::CorruptDurableState, "live and retired schema-WAL transaction copies differ");
            syncDirectory(parentPath(destination));
            removeTransactionDirectory(source);
            syncDirectory(paths.walDirectory());
            return;
        }
        if (destination_exists)
        {
            validateTransactionDirectory(destination);
            syncDirectory(parentPath(destination));
            syncDirectory(paths.walDirectory());
            return;
        }
        if (!source_exists)
            storageFail(StorageError::Code::CorruptDurableState, "schema-WAL transaction is absent from live and retired namespaces");
        validateTransactionDirectory(source);
        ensureDirectory(parentPath(destination));
        requireNotSymlink(destination);
        disk->moveDirectory(source, String(destination));
        syncDirectory(parentPath(destination));
        syncDirectory(paths.walDirectory());
    }

    std::optional<DatabaseSchemaWALPrepare> validateRetiredRollback(UInt64 transaction_id) const
    {
        const String directory = paths.retiredRollbackTransactionDirectory(transaction_id);
        const std::set<String> allowed{"prepare.wal", "recovery.bin"};
        const auto names_vector = validateFlatDirectory(directory, allowed);
        const std::set<String> names(names_vector.begin(), names_vector.end());
        if (names.empty())
            return std::nullopt;

        const auto prepare = readPrepareFromDirectory(transaction_id, directory);
        if (!prepare)
            storageFail(StorageError::Code::CorruptDurableState, "retired rollback cleanup state is invalid");
        if (const auto recovery_bytes = readOptionalFile(joinPath(directory, "recovery.bin"), internal_record_overhead))
        {
            const auto recovery = decodeRecoveryDecisionEnvelopeInternal(*recovery_bytes);
            if (recovery.database_uuid != paths.getDatabaseUUID() || recovery.transaction_id != transaction_id
                || recovery.decision != DatabaseSchemaWALRecoveryDecision::RollBackPrepared
                || (prepare && recovery.prepare_hash != prepare->prepare_hash))
                storageFail(StorageError::Code::CorruptDurableState, "retired rollback recovery marker is inconsistent");
        }
        return prepare;
    }

    void removeRetiredRollbackTransaction(UInt64 transaction_id)
    {
        const String directory = paths.retiredRollbackTransactionDirectory(transaction_id);
        for (const auto & name : {"recovery.bin", "prepare.wal"})
        {
            const String path = joinPath(directory, name);
            ensureRegularFileOrAbsent(path);
            if (!disk->existsFile(path))
                continue;
            disk->removeFile(path);
            syncDirectory(directory);
        }
        if (disk->existsFileOrDirectory(directory))
        {
            if (!listNames(directory).empty())
                storageFail(StorageError::Code::CorruptDurableState, "retired rollback cleanup left an unknown entry");
            disk->removeDirectory(directory);
            syncDirectory(parentPath(directory));
        }
    }

    bool eraseRolledBackFirstActivationNamespace(UInt64 transaction_id, const std::optional<DatabaseSchemaWALPrepare> & prepare)
    {
        if (prepare && prepare->before_authority_state)
            return false;
        if (currentAuthorityState() || latestCheckpointRecord())
            return false;
        const auto retired_checkpoints = retiredCheckpointNamespaceEntries();
        if (!transactionIDs().empty()
            || numericDirectoryIDs(paths.retiredRollbackDirectory(), "retired rollback namespace") != std::vector<UInt64>{transaction_id}
            || !retired_checkpoints.transaction_checkpoint_ids.empty() || !retired_checkpoints.checkpoint_image_ids.empty())
            return false;

        const auto high_water_bytes = readOptionalFile(paths.highWaterMarkPath(), internal_record_overhead);
        if (high_water_bytes
            && decodeAtomicDatabaseSchemaMutationHighWaterMark(*high_water_bytes, paths.getDatabaseUUID()) != transaction_id)
            storageFail(StorageError::Code::CorruptDurableState, "first-activation rollback has a different durable high-water mark");
        if (listNames(paths.typesDirectory()) != std::vector<String>{".authority"})
            storageFail(StorageError::Code::CorruptDurableState, "first-activation rollback left canonical definition records");
        const auto staging_names = listNames(paths.stagingDirectory());
        if (staging_names != std::vector<String>{} && staging_names != std::vector<String>{fixedWidthID(transaction_id)})
            storageFail(StorageError::Code::CorruptDurableState, "first-activation rollback has unrelated staging");

        const std::set<String> allowed_authority_entries{
            "checkpoints",
            "expectations",
            "retired",
            "staging",
            "synthetic",
            "transaction_high_water.bin",
            "wal",
        };
        for (const auto & name : listNames(paths.authorityDirectory()))
        {
            if (!allowed_authority_entries.contains(name))
                storageFail(StorageError::Code::CorruptDurableState, "first-activation rollback left an unknown authority entry");
        }
        for (const auto & directory :
             {paths.checkpointsDirectory(),
              joinPath(paths.authorityDirectory(), "expectations"),
              joinPath(paths.authorityDirectory(), "synthetic")})
        {
            requireNotSymlink(directory);
            if (disk->existsFileOrDirectory(directory) && (!disk->existsDirectory(directory) || !listNames(directory).empty()))
                storageFail(StorageError::Code::CorruptDurableState, "first-activation rollback left canonical artifacts");
        }

        if (!staging_names.empty())
        {
            removeStagingTransaction(transaction_id);
            syncDirectory(paths.stagingDirectory());
        }
        if (high_water_bytes)
        {
            disk->removeFile(paths.highWaterMarkPath());
            syncDirectory(paths.authorityDirectory());
        }
        /// Remove the database-root marker while the retired rollback record
        /// still proves why first activation may be erased. A crash after this
        /// barrier therefore remains recoverable from `retired/rollback`;
        /// once that record is gone, only an exact empty scaffold remains.
        removeFirstActivationMarker(transaction_id);
        removeRetiredRollbackTransaction(transaction_id);
        removeEmptyOwnedDirectory(joinPath(paths.authorityDirectory(), "expectations"));
        removeEmptyOwnedDirectory(joinPath(paths.authorityDirectory(), "synthetic"));
        cleanupNeverEnabledScaffold();
        return true;
    }

    DiskPtr disk;
    AtomicDatabaseSchemaMutationPaths paths;
    AtomicDatabaseSchemaMutationStorageLimits limits;
    mutable std::mutex mutex;
    std::atomic<UInt64> current_guard_identity{0};
    std::atomic<UInt64> recovery_required_transaction{0};
    std::atomic<UInt8> recovery_required_phase{0};
    std::optional<UInt64> active_transaction_id;
    std::optional<UInt64> finished_staging_transaction_id;
    std::set<String> touched_installation_directories;
};

PreparedAtomicDatabaseUDTConfigurationV2::PreparedAtomicDatabaseUDTConfigurationV2(
    std::shared_ptr<AtomicDatabaseUDTConfigurationCleanupState> cleanup_state_) noexcept
    : cleanup_state(std::move(cleanup_state_))
{
}

PreparedAtomicDatabaseUDTConfigurationV2::PreparedAtomicDatabaseUDTConfigurationV2(
    PreparedAtomicDatabaseUDTConfigurationV2 && other) noexcept
    : cleanup_state(std::move(other.cleanup_state))
{
}

PreparedAtomicDatabaseUDTConfigurationV2::~PreparedAtomicDatabaseUDTConfigurationV2()
{
    if (!cleanup_state)
        return;
    std::lock_guard lock(cleanup_state->mutex);
    if (cleanup_state->owner)
        cleanup_state->owner->discardPreparedUDTConfigurationV2IfInactiveNoThrow();
}

void PreparedAtomicDatabaseUDTConfigurationV2::disarmAfterDurableActivation() noexcept
{
    cleanup_state.reset();
}

AtomicDatabaseSchemaMutationStorage::AtomicDatabaseSchemaMutationStorage(
    DiskPtr disk_, UUID database_uuid_, String metadata_root_, AtomicDatabaseSchemaMutationStorageLimits limits_)
    : impl(std::make_unique<Impl>(std::move(disk_), database_uuid_, std::move(metadata_root_), String{}, std::move(limits_)))
    , configuration_cleanup_state(std::make_shared<AtomicDatabaseUDTConfigurationCleanupState>())
{
    configuration_cleanup_state->owner = this;
}

AtomicDatabaseSchemaMutationStorage::AtomicDatabaseSchemaMutationStorage(
    DiskPtr disk_, UUID database_uuid_, String metadata_root_, String database_name_, AtomicDatabaseSchemaMutationStorageLimits limits_)
    : impl(
          std::make_unique<Impl>(
              std::move(disk_), database_uuid_, std::move(metadata_root_), std::move(database_name_), std::move(limits_)))
    , configuration_cleanup_state(std::make_shared<AtomicDatabaseUDTConfigurationCleanupState>())
{
    configuration_cleanup_state->owner = this;
}

AtomicDatabaseSchemaMutationStorage::~AtomicDatabaseSchemaMutationStorage()
{
    std::lock_guard lock(configuration_cleanup_state->mutex);
    configuration_cleanup_state->owner = nullptr;
}

DatabaseSchemaMutationGuard AtomicDatabaseSchemaMutationStorage::issueMutationGuard()
{
    static std::atomic<UInt64> next_guard_identity{1};
    std::lock_guard lock(impl->mutex);
    impl->ensureLayout();
    const UInt64 durable_predecessor = impl->durableHighWater();
    const UInt64 identity = next_guard_identity.fetch_add(1, std::memory_order_relaxed);
    if (identity == 0)
        storageFail(StorageError::Code::InvalidConfiguration, "schema-mutation guard identity space is exhausted");
    impl->current_guard_identity.store(identity, std::memory_order_release);
    impl->active_transaction_id.reset();
    impl->finished_staging_transaction_id.reset();
    impl->touched_installation_directories.clear();
    return DatabaseSchemaMutationGuard::issue(impl->paths.getDatabaseUUID(), identity, durable_predecessor);
}

bool AtomicDatabaseSchemaMutationStorage::hasDurableAuthorityMarker() const
{
    std::lock_guard lock(impl->mutex);
    return impl->hasDurableAuthorityMarker();
}

bool AtomicDatabaseSchemaMutationStorage::hasCompleteDurableActivationMarker() const
{
    std::lock_guard lock(impl->mutex);
    return impl->hasCompleteDurableActivationMarker();
}

bool AtomicDatabaseSchemaMutationStorage::hasBoundedDurableAuthorityHead() const
{
    std::lock_guard lock(impl->mutex);
    return impl->hasBoundedDurableAuthorityHead();
}

bool AtomicDatabaseSchemaMutationStorage::cleanupNeverEnabledScaffold()
{
    std::lock_guard lock(impl->mutex);
    if (impl->hasDurableAuthorityMarker())
        return false;
    return impl->cleanupNeverEnabledScaffold();
}

PreparedAtomicDatabaseUDTConfigurationV2 AtomicDatabaseSchemaMutationStorage::prepareUDTConfigurationForFirstActivationV2(
    const AtomicDatabaseUDTPersistedConfigurationV2 & configured)
{
    std::lock_guard lock(impl->mutex);
    impl->prepareUDTConfigurationForFirstActivationV2(configured);
    return PreparedAtomicDatabaseUDTConfigurationV2(configuration_cleanup_state);
}

AtomicDatabaseUDTPersistedConfigurationV2 AtomicDatabaseSchemaMutationStorage::reconcileUDTConfigurationForActiveStartupV2(
    const AtomicDatabaseUDTPersistedConfigurationV2 & configured)
{
    std::lock_guard lock(impl->mutex);
    return impl->reconcileUDTConfigurationForActiveStartupV2(configured);
}

AtomicDatabaseUDTPersistedConfigurationV2 AtomicDatabaseSchemaMutationStorage::readUDTConfigurationForActiveStartupV2()
{
    std::lock_guard lock(impl->mutex);
    if (!impl->hasBoundedDurableAuthorityHead())
    {
        throw AtomicDatabaseSchemaMutationStorageError(
            AtomicDatabaseSchemaMutationStorageError::Code::CorruptDurableState,
            "cannot read V2 policy without an active Atomic authority");
    }
    return impl->readOrMigrateCombinedConfiguration().configuration;
}

void AtomicDatabaseSchemaMutationStorage::discardPreparedUDTConfigurationV2IfInactiveNoThrow() noexcept
{
    try
    {
        std::lock_guard lock(impl->mutex);
        if (!impl->hasDurableAuthorityMarker())
            static_cast<void>(impl->cleanupNeverEnabledScaffold());
    }
    catch (...)
    {
        /// A no-throw rollback token must not erase policy bytes unless exact
        /// inactivity was re-proved. Startup recovery remains authoritative.
    }
}

std::optional<AuthorityVerificationScheduleCursor> AtomicDatabaseSchemaMutationStorage::loadAuthorityVerificationCursor()
{
    std::lock_guard lock(impl->mutex);
    return impl->loadAuthorityVerificationCursor();
}

void AtomicDatabaseSchemaMutationStorage::persistAuthorityVerificationCursor(const AuthorityVerificationScheduleCursor & cursor)
{
    std::lock_guard lock(impl->mutex);
    impl->persistAuthorityVerificationCursor(cursor);
}

UInt64 AtomicDatabaseSchemaMutationStorage::getDurableHighWaterMark() const
{
    std::lock_guard lock(impl->mutex);
    impl->ensureLayout();
    return impl->durableHighWater();
}

std::optional<AuthorityState> AtomicDatabaseSchemaMutationStorage::getCurrentAuthorityState() const
{
    std::lock_guard lock(impl->mutex);
    impl->ensureLayout();
    return impl->currentAuthorityState();
}

std::optional<UInt64> AtomicDatabaseSchemaMutationStorage::getRecoveryRequiredTransactionID() const noexcept
{
    const UInt64 transaction_id = impl->recovery_required_transaction.load(std::memory_order_acquire);
    if (transaction_id == 0)
        return std::nullopt;
    return transaction_id;
}

std::vector<UInt64> AtomicDatabaseSchemaMutationStorage::listDurableTransactionIDs() const
{
    std::lock_guard lock(impl->mutex);
    impl->ensureLayout();
    return impl->transactionIDs();
}

std::optional<std::vector<UInt64>> AtomicDatabaseSchemaMutationStorage::resumeDurableTransactionIDDiscoveryForAuthorityRepair(
    AtomicDatabaseSchemaMutationDurableTransactionDiscovery & continuation,
    const AuthorityRootGraphIdentity & root,
    UInt64 maximum_transactions,
    UInt64 maximum_control_bytes,
    const AuthorityVerificationPassBudget & pass_budget) const
{
    std::lock_guard lock(impl->mutex);
    impl->ensureLayout();
    return impl->resumeLightweightTransactionIDs(*continuation.impl, root, maximum_transactions, maximum_control_bytes, pass_budget);
}

std::optional<AtomicDatabaseSchemaMutationRecoveryTransaction>
AtomicDatabaseSchemaMutationStorage::loadCommittedTransactionForAuthorityRepair(
    UInt64 transaction_id, UInt64 maximum_total_staged_artifact_bytes, UInt64 maximum_staged_artifacts, UInt64 maximum_control_bytes) const
{
    std::lock_guard lock(impl->mutex);
    impl->ensureLayout();
    return impl->loadCommittedTransactionForAuthorityRepair(
        transaction_id, maximum_total_staged_artifact_bytes, maximum_staged_artifacts, maximum_control_bytes);
}

AtomicDatabaseSchemaMutationRecoveryTransaction AtomicDatabaseSchemaMutationStorage::loadTransactionForRecovery(
    UInt64 transaction_id, UInt64 maximum_total_staged_artifact_bytes, UInt64 maximum_staged_artifacts, UInt64 maximum_control_bytes) const
{
    std::lock_guard lock(impl->mutex);
    impl->ensureLayout();
    return impl->loadTransaction(transaction_id, maximum_total_staged_artifact_bytes, maximum_staged_artifacts, maximum_control_bytes);
}

std::optional<AtomicDatabaseSchemaMutationCheckpointImage> AtomicDatabaseSchemaMutationStorage::loadLatestCheckpoint() const
{
    std::lock_guard lock(impl->mutex);
    impl->ensureLayout();
    return impl->latestCheckpoint();
}

std::optional<DatabaseSchemaWALExactRepairProvenance> AtomicDatabaseSchemaMutationStorage::loadLatestExactRepairProvenance() const
{
    std::lock_guard lock(impl->mutex);
    impl->ensureLayout();
    return impl->checkpointMaintenancePlan().latest_exact_repair_provenance;
}

bool AtomicDatabaseSchemaMutationStorage::startupExactRepairArtifactNeedsInstallation(
    const DatabaseSchemaWALStagedArtifact & artifact) const
{
    std::lock_guard lock(impl->mutex);
    if (artifact.image != DatabaseSchemaWALStagedArtifactImage::After
        || (artifact.kind != DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord
            && artifact.kind != DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord
            && artifact.kind != DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar))
    {
        storageFail(StorageError::Code::InvalidConfiguration, "startup exact repair received an unsupported artifact image");
    }
    validateSupportedArtifact(impl->paths.getDatabaseUUID(), artifact.kind, artifact.object);
    if (artifact.canonical_bytes.size() > impl->limits.wal.maximum_staged_artifact_bytes)
        storageFail(StorageError::Code::LimitExceeded, "startup exact-repair candidate exceeds its artifact byte limit");

    const String target = impl->paths.canonicalArtifactPath(artifact.kind, artifact.object);
    impl->ensureRegularFileOrAbsent(target);
    if (!impl->disk->existsFile(target))
        return true;
    if (impl->disk->getFileSize(target) > impl->limits.wal.maximum_staged_artifact_bytes)
        return true;
    return impl->readRequiredFile(target, impl->limits.wal.maximum_staged_artifact_bytes) != artifact.canonical_bytes;
}

void AtomicDatabaseSchemaMutationStorage::maintainCheckpointBeforeMutation(const AuthorityRoot & current_root)
{
    Impl::CheckpointMaintenancePlan plan;
    DatabaseSchemaWALLimits wal_limits;
    {
        std::lock_guard lock(impl->mutex);
        impl->ensureLayout();
        if (current_root.getDatabaseUUID() != impl->paths.getDatabaseUUID())
            storageFail(StorageError::Code::InvalidConfiguration, "checkpoint maintenance root belongs to another database");
        plan = impl->checkpointMaintenancePlan();
        wal_limits = impl->limits.wal;
    }
    if (!plan.needsMaintenance())
        return;

    std::optional<DatabaseSchemaWALValidatedCheckpoint> new_checkpoint;
    if (plan.checkpoint_commit)
    {
        const auto inventory = current_root.pinAuthorityInventory();
        const auto graph = current_root.pinSchemaObjectDependencyGraph();
        new_checkpoint.emplace(
            DatabaseSchemaWALCheckpointBuilder::build(
                plan.checkpoint_commit->transaction_id,
                *plan.checkpoint_commit,
                current_root.getAuthorityState(),
                inventory,
                graph,
                wal_limits,
                plan.latest_exact_repair_provenance));
    }

    /// This guard is consumed only by checkpoint maintenance. The fresh
    /// mutation caller issues a second guard after every maintenance barrier.
    auto maintenance_guard = issueMutationGuard();
    static_cast<void>(sweepRetiredTransactions(maintenance_guard));

    const DatabaseSchemaWALCheckpoint * checkpoint_to_compact = nullptr;
    if (new_checkpoint)
    {
        persistValidatedDatabaseSchemaCheckpoint(*this, maintenance_guard, *new_checkpoint, wal_limits);
        checkpoint_to_compact = std::addressof(new_checkpoint->getCheckpoint());
    }
    else if (plan.resume_compaction)
    {
        if (!plan.latest_checkpoint)
            storageFail(StorageError::Code::CorruptDurableState, "checkpoint maintenance lost its durable compaction anchor");
        checkpoint_to_compact = std::addressof(*plan.latest_checkpoint);
    }

    if (checkpoint_to_compact)
        compactThroughValidatedCheckpoint(maintenance_guard, *checkpoint_to_compact);
    static_cast<void>(sweepRetiredTransactions(maintenance_guard));
}

AtomicDatabaseSchemaMutationReconciliation AtomicDatabaseSchemaMutationStorage::readAndReconcileAuthorityRecords(
    const AuthorityInventory & anchored_inventory, const SchemaObjectDependencyGraph & anchored_graph) const
{
    std::lock_guard lock(impl->mutex);
    return impl->reconcileAuthorityRecords(anchored_inventory, anchored_graph);
}

AtomicDatabaseSchemaMutationReconciliation AtomicDatabaseSchemaMutationStorage::readAndReconcileAuthorityRecordsForObjects(
    const AuthorityInventory & anchored_inventory,
    const SchemaObjectDependencyGraph & anchored_graph,
    std::span<const SchemaObjectID> selected_objects) const
{
    std::lock_guard lock(impl->mutex);
    return impl->reconcileAuthorityRecords(anchored_inventory, anchored_graph, selected_objects, false);
}

AtomicDatabaseSchemaMutationStorage::VerificationTargetRead AtomicDatabaseSchemaMutationStorage::readAuthorityVerificationTarget(
    const AuthorityRoot & anchored_root, const ScheduledAuthorityVerificationTarget & target) const
{
    std::lock_guard lock(impl->mutex);
    return impl->readAuthorityVerificationTarget(anchored_root, target);
}

AtomicDatabaseSchemaMutationStorage::RepairAuditTargetRead AtomicDatabaseSchemaMutationStorage::readAuthorityRepairAuditTarget(
    const AuthorityRoot & anchored_root, const AuthorityInventoryLeaf & leaf, UInt64 maximum_retained_bytes) const
{
    std::lock_guard lock(impl->mutex);
    return impl->readAuthorityRepairAuditTarget(anchored_root, leaf, maximum_retained_bytes);
}

std::vector<AuthorityVerificationTarget> AtomicDatabaseSchemaMutationStorage::snapshotAuthorityVerificationTargets(
    const AuthorityRoot & anchored_root,
    const AuthorityVerificationScheduleLimits & schedule_limits,
    const AuthorityVerificationBatchExecutorLimits & executor_limits,
    UInt64 begin_target,
    UInt64 maximum_targets,
    std::span<const AuthorityVerificationTargetHistory> history,
    const AuthorityVerificationPassBudget & pass_budget,
    UInt64 * consumed_work_items) const
{
    std::lock_guard lock(impl->mutex);
    return impl->snapshotAuthorityVerificationTargets(
        anchored_root, schedule_limits, executor_limits, begin_target, maximum_targets, history, pass_budget, consumed_work_items);
}

const AtomicDatabaseSchemaMutationPaths & AtomicDatabaseSchemaMutationStorage::getPaths() const noexcept
{
    return impl->paths;
}

void AtomicDatabaseSchemaMutationStorage::validateMutationGuardAndDurablePredecessor(
    const DatabaseSchemaMutationGuard & guard,
    const std::optional<AuthorityState> & expected_preceding_authority_state,
    UInt64 transaction_id)
{
    std::lock_guard lock(impl->mutex);
    impl->ensureLayout();
    impl->validateGuard(guard);
    static_cast<void>(impl->hasDurableAuthorityMarker());
    if (impl->recovery_required_transaction.load(std::memory_order_acquire) != 0 || impl->hasUnresolvedPrepare())
        replayConflict("database is fail-stopped pending schema-mutation recovery");
    const UInt64 durable_predecessor = impl->durableHighWater();
    if (guard.getDurablePredecessorTransactionID() != durable_predecessor)
        replayConflict("schema-mutation guard captured a stale durable predecessor");
    if (transaction_id <= durable_predecessor)
        replayConflict("schema-mutation transaction ID is not above the durable high-water mark");
    if (impl->currentAuthorityState() != expected_preceding_authority_state)
        replayConflict("schema-mutation preceding authority state is stale");
    impl->active_transaction_id = transaction_id;
    impl->finished_staging_transaction_id.reset();
    impl->touched_installation_directories.clear();
}

void AtomicDatabaseSchemaMutationStorage::markMutationRecoveryRequired(
    const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id, DatabaseSchemaMutationDurabilityPhase phase) noexcept
{
    if (guard.getDatabaseUUID() != impl->paths.getDatabaseUUID()
        || guard.getOpaqueIdentity() != impl->current_guard_identity.load(std::memory_order_acquire))
        return;
    impl->recovery_required_phase.store(static_cast<UInt8>(phase), std::memory_order_relaxed);
    impl->recovery_required_transaction.store(transaction_id, std::memory_order_release);
}

void AtomicDatabaseSchemaMutationStorage::validateRecoveryGuard(const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id)
{
    std::lock_guard lock(impl->mutex);
    impl->ensureLayout();
    impl->validateGuard(guard);
    if (transaction_id > impl->durableHighWater())
        replayConflict("schema-mutation recovery transaction exceeds the durable high-water mark");
    if (!impl->readPrepare(transaction_id))
        replayConflict("schema-mutation recovery has no durable Prepare marker");
    impl->active_transaction_id = transaction_id;
    impl->finished_staging_transaction_id = transaction_id;
    impl->touched_installation_directories.clear();
}

void AtomicDatabaseSchemaMutationStorage::stageArtifact(
    const DatabaseSchemaWALStagedArtifactLocator & locator,
    const DatabaseSchemaWALStagedArtifactRef & artifact,
    std::string_view canonical_bytes)
{
    std::lock_guard lock(impl->mutex);
    impl->requireActiveDatabase(locator.database_uuid, locator.transaction_id);
    if (impl->finished_staging_transaction_id)
        replayConflict("cannot add a staged artifact after the staging barrier");
    if (locator.ordinal >= impl->limits.wal.maximum_staged_artifacts
        || canonical_bytes.size() > impl->limits.wal.maximum_staged_artifact_bytes)
        storageFail(StorageError::Code::LimitExceeded, "staged artifact exceeds its configured storage limit");
    const String envelope = encodeStagedEnvelope(locator, artifact, canonical_bytes);
    impl->installImmutableFile(impl->paths.stagedArtifactPath(locator.transaction_id, locator.ordinal), envelope, "stage");
}

void AtomicDatabaseSchemaMutationStorage::finishStaging(UUID database_uuid, UInt64 transaction_id)
{
    std::lock_guard lock(impl->mutex);
    impl->requireActiveDatabase(database_uuid, transaction_id);
    impl->ensureDirectory(impl->paths.stagingTransactionDirectory(transaction_id));
    impl->validateStagingTransaction(transaction_id);
    impl->validateMappedTableStagingPreconditions(transaction_id);
    impl->syncDirectory(impl->paths.stagingTransactionDirectory(transaction_id));
    impl->finished_staging_transaction_id = transaction_id;
}

void AtomicDatabaseSchemaMutationStorage::persistPrepare(UInt64 transaction_id, std::string_view canonical_prepare)
{
    std::lock_guard lock(impl->mutex);
    impl->requireActiveTransaction(transaction_id);
    auto prepare = [&]
    {
        try
        {
            return decodeDatabaseSchemaWALPrepare(canonical_prepare, impl->limits.wal);
        }
        catch (const DatabaseSchemaWALError &)
        {
            replayConflict("Prepare marker bytes are not a valid V1 record");
        }
    }();
    if (prepare.transaction_id != transaction_id || prepare.after_authority_state.database_uuid != impl->paths.getDatabaseUUID())
        replayConflict("Prepare marker transaction identity differs");
    if (impl->finished_staging_transaction_id != transaction_id)
        replayConflict("Prepare marker has no completed staging barrier");
    static_cast<void>(impl->loadValidatedStagingManifest(transaction_id, prepare, true));
    impl->installImmutableFile(impl->paths.preparePath(transaction_id), canonical_prepare, "prepare");
    impl->persistHighWater(transaction_id);
}

void AtomicDatabaseSchemaMutationStorage::installArtifact(
    const DatabaseSchemaWALStagedArtifactRef & artifact, std::string_view canonical_bytes)
{
    std::lock_guard lock(impl->mutex);
    if (!impl->active_transaction_id)
        replayConflict("artifact installation has no active transaction");
    validateSupportedArtifact(impl->paths.getDatabaseUUID(), artifact.kind, artifact.object);
    if (artifact.byte_size != canonical_bytes.size()
        || artifact.content_hash != computeDatabaseSchemaWALStagedArtifactHash(artifact.kind, canonical_bytes))
        replayConflict("artifact installation bytes differ from their content address");
    const auto selected = impl->findSelectedImage(artifact);
    if (selected.canonical_bytes != canonical_bytes)
        replayConflict("artifact installation bytes differ from the selected durable staged image");

    fiu_do_on(DB::FailPoints::udt_schema_storage_temp_rename_failure, {
        storageFail(StorageError::Code::FaultInjected, "fault injected before UDT schema artifact installation");
    });

    const auto authenticated_prepare = impl->readPrepare(*impl->active_transaction_id);
    const bool exact_repair_install
        = authenticated_prepare && authenticated_prepare->exact_repair && artifact.image == DatabaseSchemaWALStagedArtifactImage::After;
    if (exact_repair_install)
    {
        /// validateTransition authenticated every exact-repair After image
        /// against the unchanged rooted inventory before Prepare became
        /// durable. Unlike an ordinary mutation, the target is expected to be
        /// missing or arbitrarily damaged and therefore has no admissible
        /// opposite image. Reinstall only the selected staged content address.
        const String target = impl->canonicalArtifactTarget(selected);
        const String parent = parentPath(target);
        const String temporary = target + ".install-" + fixedWidthID(*impl->active_transaction_id) + ".tmp";
        impl->ensureDirectory(parent);
        impl->ensureRegularFileOrAbsent(target);
        impl->ensureRegularFileOrAbsent(temporary);

        const auto remove_nonmatching_temporary = [&]
        {
            if (!impl->disk->existsFile(temporary))
                return;
            if (impl->disk->getFileSize(temporary) > impl->limits.wal.maximum_staged_artifact_bytes)
            {
                impl->disk->removeFile(temporary);
                impl->syncDirectory(parent);
                return;
            }
            const auto bytes = impl->readRequiredFile(temporary, impl->limits.wal.maximum_staged_artifact_bytes);
            if (bytes != canonical_bytes)
            {
                impl->disk->removeFile(temporary);
                impl->syncDirectory(parent);
            }
        };
        remove_nonmatching_temporary();
        if (!impl->disk->existsFile(temporary))
            impl->writeFreshFile(temporary, canonical_bytes);

        bool target_matches = false;
        if (impl->disk->existsFile(target) && impl->disk->getFileSize(target) <= impl->limits.wal.maximum_staged_artifact_bytes)
        {
            target_matches = impl->readRequiredFile(target, impl->limits.wal.maximum_staged_artifact_bytes) == canonical_bytes;
        }
        if (target_matches)
            impl->disk->removeFile(temporary);
        else if (impl->disk->existsFile(target))
            impl->disk->replaceFile(temporary, target);
        else
            impl->disk->moveFile(temporary, target);
        impl->touched_installation_directories.insert(parent);
        return;
    }

    if (artifact.kind == DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata
        && isOrdinaryDependentObjectKind(artifact.object.kind))
    {
        const auto selected_installation = impl->findTableInstallationForImage(artifact, artifact.image);
        const auto opposite_image = artifact.image == DatabaseSchemaWALStagedArtifactImage::Before
            ? DatabaseSchemaWALStagedArtifactImage::After
            : DatabaseSchemaWALStagedArtifactImage::Before;
        const auto opposite_installation = impl->findTableInstallationForImage(artifact, opposite_image);

        if (selected_installation && opposite_installation && selected_installation->object_name != opposite_installation->object_name)
        {
            const auto opposite = impl->findOppositeImage(artifact);
            if (!opposite)
                replayConflict("mapped table RENAME has no opposite metadata image");
            const String target = impl->paths.tableMetadataPath(selected_installation->object_name);
            const String opposite_target = impl->paths.tableMetadataPath(opposite_installation->object_name);
            const String target_parent = parentPath(target);
            const String opposite_parent = parentPath(opposite_target);
            impl->ensureDirectory(target_parent);
            impl->ensureDirectory(opposite_parent);

            const auto existing = impl->readOptionalFile(target, impl->limits.wal.maximum_staged_artifact_bytes);
            const auto opposite_existing = impl->readOptionalFile(opposite_target, impl->limits.wal.maximum_staged_artifact_bytes);
            if (existing && *existing != canonical_bytes)
                replayConflict("mapped table RENAME target differs from its selected durable image");
            if (opposite_existing && *opposite_existing != opposite->canonical_bytes)
                replayConflict("mapped table RENAME source differs from its opposite durable image");
            if (!existing && !opposite_existing)
                replayConflict("mapped table RENAME lost both durable metadata images");

            const String temporary = target + ".install-" + fixedWidthID(*impl->active_transaction_id) + ".tmp";
            if (!existing)
            {
                if (const auto temporary_bytes = impl->readOptionalFile(temporary, impl->limits.wal.maximum_staged_artifact_bytes);
                    temporary_bytes && *temporary_bytes != canonical_bytes)
                {
                    impl->disk->removeFile(temporary);
                }
                if (!impl->disk->existsFile(temporary))
                    impl->writeFreshFile(temporary, canonical_bytes);
                impl->ensureRegularFileOrAbsent(target);
                impl->disk->moveFile(temporary, target);
            }
            else if (impl->disk->existsFileOrDirectory(temporary))
            {
                impl->ensureRegularFileOrAbsent(temporary);
                impl->disk->removeFile(temporary);
            }
            if (opposite_existing)
                impl->disk->removeFile(opposite_target);
            impl->touched_installation_directories.insert(target_parent);
            impl->touched_installation_directories.insert(opposite_parent);
            return;
        }

        /// A rollback of a mapped DROP reinstalls its sole Before image. The
        /// forward removal keeps that image in metadata_dropped so a crash is
        /// recoverable both by this transaction and by DatabaseCatalog.
        if (artifact.image == DatabaseSchemaWALStagedArtifactImage::Before && selected_installation && !opposite_installation
            && impl->paths.hasDatabaseName())
        {
            const String target = impl->paths.tableMetadataPath(selected_installation->object_name);
            const String dropped = impl->paths.droppedTableMetadataPath(selected_installation->object_name, artifact.object.object_uuid);
            const String target_parent = parentPath(target);
            const String dropped_parent = parentPath(dropped);
            impl->ensureDirectory(target_parent);
            impl->ensureDirectory(dropped_parent);
            const auto existing = impl->readOptionalFile(target, impl->limits.wal.maximum_staged_artifact_bytes);
            const auto dropped_existing = impl->readOptionalFile(dropped, impl->limits.wal.maximum_staged_artifact_bytes);
            if (existing && *existing != canonical_bytes)
                replayConflict("rolled-back mapped DROP target differs from its durable Before image");
            if (dropped_existing && *dropped_existing != canonical_bytes)
                replayConflict("rolled-back mapped DROP tombstone differs from its durable Before image");
            if (!existing && dropped_existing)
                impl->disk->moveFile(dropped, target);
            else if (existing && dropped_existing)
                impl->disk->removeFile(dropped);
            if (existing || dropped_existing)
            {
                impl->touched_installation_directories.insert(target_parent);
                impl->touched_installation_directories.insert(dropped_parent);
                return;
            }
        }
    }

    const String target = impl->canonicalArtifactTarget(selected);
    const String parent = parentPath(target);
    const String temporary = target + ".install-" + fixedWidthID(*impl->active_transaction_id) + ".tmp";
    impl->ensureDirectory(parent);
    const auto existing = impl->readOptionalFile(target, impl->limits.wal.maximum_staged_artifact_bytes);
    if (existing && *existing == canonical_bytes)
    {
        if (impl->disk->existsFileOrDirectory(temporary))
        {
            impl->ensureRegularFileOrAbsent(temporary);
            impl->disk->removeFile(temporary);
        }
        impl->touched_installation_directories.insert(parent);
        return;
    }
    const auto opposite = impl->findOppositeImage(artifact);
    if (!existing && opposite)
        replayConflict("canonical artifact disappeared instead of matching the opposite durable image");
    if (existing && (!opposite || opposite->canonical_bytes != *existing))
        replayConflict("canonical artifact matches neither durable transition image");

    if (const auto temporary_bytes = impl->readOptionalFile(temporary, impl->limits.wal.maximum_staged_artifact_bytes);
        temporary_bytes && *temporary_bytes != canonical_bytes)
    {
        impl->disk->removeFile(temporary);
        impl->touched_installation_directories.insert(parent);
    }
    impl->writeFreshFile(temporary, canonical_bytes);
    impl->ensureRegularFileOrAbsent(target);
    if (existing)
        impl->disk->replaceFile(temporary, target);
    else
        impl->disk->moveFile(temporary, target);
    impl->touched_installation_directories.insert(parent);
}

void AtomicDatabaseSchemaMutationStorage::removeArtifact(DatabaseSchemaWALStagedArtifactKind kind, const SchemaObjectID & object)
{
    std::lock_guard lock(impl->mutex);
    if (!impl->active_transaction_id)
        replayConflict("artifact removal has no active transaction");
    validateSupportedArtifact(impl->paths.getDatabaseUUID(), kind, object);
    const auto only_image = impl->findOnlyImage(kind, object);
    if (!only_image)
        replayConflict("artifact removal requires exactly one durable image");

    if (kind == DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata && isOrdinaryDependentObjectKind(object.kind)
        && only_image->artifact.image == DatabaseSchemaWALStagedArtifactImage::Before && impl->paths.hasDatabaseName())
    {
        const auto installation = impl->findTableInstallationForMetadata(only_image->artifact);
        const String target = impl->paths.tableMetadataPath(installation.object_name);
        const String dropped = impl->paths.droppedTableMetadataPath(installation.object_name, object.object_uuid);
        const String target_parent = parentPath(target);
        const String dropped_parent = parentPath(dropped);
        impl->ensureDirectory(target_parent);
        impl->ensureDirectory(dropped_parent);
        const auto existing = impl->readOptionalFile(target, impl->limits.wal.maximum_staged_artifact_bytes);
        const auto dropped_existing = impl->readOptionalFile(dropped, impl->limits.wal.maximum_staged_artifact_bytes);
        if (existing && *existing != only_image->canonical_bytes)
            replayConflict("mapped DROP metadata differs from its durable Before image");
        if (dropped_existing && *dropped_existing != only_image->canonical_bytes)
            replayConflict("mapped DROP tombstone differs from its durable Before image");
        if (existing && dropped_existing)
            replayConflict("mapped DROP has both live metadata and a durable tombstone");

        const String temporary = target + ".install-" + fixedWidthID(*impl->active_transaction_id) + ".tmp";
        if (impl->disk->existsFileOrDirectory(temporary))
        {
            impl->ensureRegularFileOrAbsent(temporary);
            impl->disk->removeFile(temporary);
        }
        const auto prepare = impl->readPrepare(*impl->active_transaction_id);
        if (!prepare)
            replayConflict("mapped DROP artifact removal has no durable Prepare marker");
        const bool committed = impl->readCommit(*impl->active_transaction_id, *prepare).has_value();
        if (committed)
        {
            /// `metadata_dropped` is consumed by DatabaseCatalog independently
            /// of this WAL. Publish there only after Commit is durable. The
            /// exact Before bytes remain in transaction staging until a
            /// checkpoint, so committed recovery can recreate this tombstone
            /// even when the server stopped between Commit and this step.
            impl->installImmutableFile(dropped, only_image->canonical_bytes, "drop", true);
            if (existing)
                impl->disk->removeFile(target);
            impl->syncDirectory(target_parent);
            impl->touched_installation_directories.insert(dropped_parent);
        }
        else
        {
            /// Before Commit the live metadata must become absent, but it must
            /// not enter Atomic's ordinary dropped-table namespace. A rollback
            /// reinstalls it from the already-durable staged Before artifact.
            if (dropped_existing)
                replayConflict("an uncommitted mapped DROP is visible in the ordinary Atomic tombstone namespace");
            if (existing)
                impl->disk->removeFile(target);
            impl->touched_installation_directories.insert(target_parent);
        }
        return;
    }

    const String target = impl->canonicalArtifactTarget(*only_image);
    const String parent = parentPath(target);
    impl->ensureDirectory(parent);
    const auto existing = impl->readOptionalFile(target, impl->limits.wal.maximum_staged_artifact_bytes);
    if (existing && (!only_image || only_image->canonical_bytes != *existing))
        replayConflict("canonical artifact removal does not match the durable transition image");
    const String temporary = target + ".install-" + fixedWidthID(*impl->active_transaction_id) + ".tmp";
    if (impl->disk->existsFileOrDirectory(temporary))
    {
        impl->ensureRegularFileOrAbsent(temporary);
        impl->disk->removeFile(temporary);
    }
    if (existing)
        impl->disk->removeFile(target);
    impl->touched_installation_directories.insert(parent);
}

void AtomicDatabaseSchemaMutationStorage::finishInstallation(UUID database_uuid, UInt64 transaction_id)
{
    std::lock_guard lock(impl->mutex);
    impl->requireActiveDatabase(database_uuid, transaction_id);
    for (const auto & directory : impl->touched_installation_directories)
        impl->syncDirectory(directory);
    impl->touched_installation_directories.clear();
}

void AtomicDatabaseSchemaMutationStorage::persistCommit(UInt64 transaction_id, std::string_view canonical_commit)
{
    std::lock_guard lock(impl->mutex);
    impl->requireActiveTransaction(transaction_id);
    const auto prepare = impl->readPrepare(transaction_id);
    if (!prepare)
        replayConflict("Commit marker has no durable Prepare marker");
    const auto commit = [&]
    {
        try
        {
            return decodeDatabaseSchemaWALCommit(canonical_commit, impl->limits.wal);
        }
        catch (const DatabaseSchemaWALError &)
        {
            replayConflict("Commit marker bytes are not a valid V1 record");
        }
    }();
    if (!commitMatchesPrepare(commit, *prepare))
        replayConflict("Commit marker does not bind its durable Prepare marker");
    const auto existing_commit = impl->readCommit(transaction_id, *prepare);
    const auto activation_transaction_id = impl->activationMarkerTransactionID();
    const bool has_activation_temporary = impl->hasActivationMarkerTemporary();
    if (!prepare->before_authority_state)
    {
        if (existing_commit && !activation_transaction_id)
            storageFail(StorageError::Code::CorruptDurableState, "first Atomic authority Commit has no activation marker");
        if (activation_transaction_id && *activation_transaction_id != transaction_id)
            storageFail(StorageError::Code::CorruptDurableState, "first Atomic authority Commit has another activation identity");
        impl->persistActivationMarker(transaction_id);
    }
    else
    {
        if (!activation_transaction_id || has_activation_temporary)
            storageFail(StorageError::Code::CorruptDurableState, "existing Atomic authority has no complete activation marker");
        if (*activation_transaction_id >= transaction_id)
            storageFail(StorageError::Code::CorruptDurableState, "Atomic authority activation identity is not before this Commit");
    }
    impl->installImmutableFile(impl->paths.commitPath(transaction_id), canonical_commit, "commit");

    /// A mapped DROP deliberately keeps its Before metadata out of the
    /// independently-scanned `metadata_dropped` directory until the Commit
    /// marker above is durable. Complete that publication before reporting a
    /// successful durable commit. If it throws, the transaction remains
    /// recovery-required and committed recovery repeats the exact remove
    /// action idempotently.
    bool published_mapped_drop = false;
    for (const auto & artifact : prepare->staged_artifacts)
    {
        if (artifact.kind != DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata
            || artifact.image != DatabaseSchemaWALStagedArtifactImage::Before || !isOrdinaryDependentObjectKind(artifact.object.kind))
            continue;
        const bool has_after_metadata = std::any_of(
            prepare->staged_artifacts.begin(),
            prepare->staged_artifacts.end(),
            [&](const auto & candidate)
            {
                return candidate.kind == DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata
                    && candidate.image == DatabaseSchemaWALStagedArtifactImage::After && candidate.object == artifact.object;
            });
        if (has_after_metadata)
            continue;

        const auto only_image = impl->findOnlyImage(artifact.kind, artifact.object);
        if (!only_image)
            replayConflict("committed mapped DROP requires exactly one durable metadata image");
        const auto installation = impl->findTableInstallationForMetadata(only_image->artifact);
        const String target = impl->paths.tableMetadataPath(installation.object_name);
        const String dropped = impl->paths.droppedTableMetadataPath(installation.object_name, artifact.object.object_uuid);
        const String target_parent = parentPath(target);
        const String dropped_parent = parentPath(dropped);
        impl->ensureDirectory(target_parent);
        impl->ensureDirectory(dropped_parent);
        const auto existing = impl->readOptionalFile(target, impl->limits.wal.maximum_staged_artifact_bytes);
        const auto dropped_existing = impl->readOptionalFile(dropped, impl->limits.wal.maximum_staged_artifact_bytes);
        if (existing && *existing != only_image->canonical_bytes)
            replayConflict("committed mapped DROP metadata differs from its durable Before image");
        if (dropped_existing && *dropped_existing != only_image->canonical_bytes)
            replayConflict("committed mapped DROP tombstone differs from its durable Before image");
        impl->installImmutableFile(dropped, only_image->canonical_bytes, "drop", true);
        if (existing)
        {
            impl->disk->removeFile(target);
            impl->syncDirectory(target_parent);
        }
        published_mapped_drop = true;
    }

    if (published_mapped_drop)
    {
        /// This marker is the durable distinction between a completed DROP
        /// whose tombstone may later be consumed by background cleanup and a
        /// crash in the Commit-to-tombstone publication window. It is written
        /// only after every tombstone and live-metadata removal barrier above.
        const String recovery = encodeAtomicDatabaseSchemaMutationRecoveryDecision(
            impl->paths.getDatabaseUUID(), transaction_id, DatabaseSchemaWALRecoveryDecision::CompleteCommitted, prepare->prepare_hash);
        impl->installImmutableFile(impl->paths.recoveryDecisionPath(transaction_id), recovery, "recovery", true);
    }
}

void AtomicDatabaseSchemaMutationStorage::finishRecovery(
    UUID database_uuid, UInt64 transaction_id, DatabaseSchemaWALRecoveryDecision decision)
{
    std::lock_guard lock(impl->mutex);
    impl->requireActiveDatabase(database_uuid, transaction_id);
    const auto prepare = impl->readPrepare(transaction_id);
    if (!prepare)
        replayConflict("recovery decision has no durable Prepare marker");
    if (decision != DatabaseSchemaWALRecoveryDecision::RollBackPrepared && decision != DatabaseSchemaWALRecoveryDecision::CompleteCommitted)
        replayConflict("recovery decision is not a registered V1 value");
    const auto commit = impl->readCommit(transaction_id, *prepare);
    if ((decision == DatabaseSchemaWALRecoveryDecision::CompleteCommitted) != commit.has_value())
        replayConflict("recovery decision differs from durable Commit presence");
    const String encoded
        = encodeAtomicDatabaseSchemaMutationRecoveryDecision(database_uuid, transaction_id, decision, prepare->prepare_hash);
    impl->installImmutableFile(impl->paths.recoveryDecisionPath(transaction_id), encoded, "recovery", true);
    impl->persistHighWater(transaction_id);
    if (impl->recovery_required_transaction.load(std::memory_order_acquire) == transaction_id)
    {
        impl->recovery_required_transaction.store(0, std::memory_order_release);
        impl->recovery_required_phase.store(0, std::memory_order_relaxed);
    }
}

void AtomicDatabaseSchemaMutationStorage::discardUnpreparedStaging(const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id)
{
    std::lock_guard lock(impl->mutex);
    impl->validateGuard(guard);
    if (impl->readPrepare(transaction_id))
        replayConflict("cannot discard staging for a durable prepared transaction");
    impl->removeStagingTransaction(transaction_id);
    impl->syncDirectory(impl->paths.stagingDirectory());
    if (impl->disk->existsFileOrDirectory(impl->paths.walTransactionDirectory(transaction_id)))
    {
        impl->validateUnpreparedWALTransaction(transaction_id);
        impl->removeTransactionDirectory(impl->paths.walTransactionDirectory(transaction_id));
        impl->syncDirectory(impl->paths.walDirectory());
    }
    impl->cleanupNeverEnabledScaffold();
    if (impl->finished_staging_transaction_id == transaction_id)
        impl->finished_staging_transaction_id.reset();
    if (impl->recovery_required_transaction.load(std::memory_order_acquire) == transaction_id)
    {
        impl->recovery_required_transaction.store(0, std::memory_order_release);
        impl->recovery_required_phase.store(0, std::memory_order_relaxed);
    }
}

UInt64 AtomicDatabaseSchemaMutationStorage::sweepUnpreparedStaging(const DatabaseSchemaMutationGuard & guard)
{
    std::lock_guard lock(impl->mutex);
    impl->ensureLayout();
    impl->validateGuard(guard);
    std::set<UInt64> removed_transactions;
    for (const auto & name : impl->listNames(impl->paths.stagingDirectory()))
    {
        const auto transaction_id = parseFixedWidthID(name);
        if (!transaction_id)
            storageFail(StorageError::Code::UnsafePath, "staging root contains a noncanonical transaction name");
        const String directory = impl->paths.stagingTransactionDirectory(*transaction_id);
        impl->requireNotSymlink(directory);
        if (!impl->disk->existsDirectory(directory))
            storageFail(StorageError::Code::UnsafePath, "staging transaction entry is not a directory");
        impl->validateStagingTransaction(*transaction_id);
        if (impl->readPrepare(*transaction_id))
            continue;
        impl->removeStagingTransaction(*transaction_id);
        removed_transactions.insert(*transaction_id);
        if (impl->recovery_required_transaction.load(std::memory_order_acquire) == *transaction_id)
        {
            impl->recovery_required_transaction.store(0, std::memory_order_release);
            impl->recovery_required_phase.store(0, std::memory_order_relaxed);
        }
    }
    if (!removed_transactions.empty())
        impl->syncDirectory(impl->paths.stagingDirectory());

    const auto unprepared_wal = impl->unpreparedWALTransactionIDs();
    for (const UInt64 transaction_id : unprepared_wal)
    {
        impl->validateUnpreparedWALTransaction(transaction_id);
        impl->removeTransactionDirectory(impl->paths.walTransactionDirectory(transaction_id));
        removed_transactions.insert(transaction_id);
    }
    if (!unprepared_wal.empty())
        impl->syncDirectory(impl->paths.walDirectory());
    impl->cleanupNeverEnabledScaffold();
    return removed_transactions.size();
}

UInt64 AtomicDatabaseSchemaMutationStorage::sweepRetiredTransactions(const DatabaseSchemaMutationGuard & guard)
{
    std::lock_guard lock(impl->mutex);
    impl->ensureLayout();
    impl->validateGuard(guard);
    UInt64 removed = 0;
    for (const UInt64 transaction_id : impl->numericDirectoryIDs(impl->paths.retiredRollbackDirectory(), "retired rollback namespace"))
    {
        impl->moveWALTransactionTo(transaction_id, impl->paths.retiredRollbackTransactionDirectory(transaction_id));
        const auto prepare = impl->validateRetiredRollback(transaction_id);
        impl->persistHighWater(transaction_id);
        if (!impl->eraseRolledBackFirstActivationNamespace(transaction_id, prepare))
        {
            impl->removeStagingTransaction(transaction_id);
            impl->syncDirectory(impl->paths.stagingDirectory());
            impl->removeRetiredRollbackTransaction(transaction_id);
        }
        ++removed;
    }

    const auto retired_checkpoints = impl->retiredCheckpointNamespaceEntries();
    /// A fully validated newest checkpoint is the proof that every
    /// checkpoint-retired WAL transaction is redundant.  Validate it before
    /// deleting either those replay records or their staged artifacts; a
    /// readable checkpoint.wal with a damaged snapshot must leave the older
    /// checkpoint-to-head replay tail intact for diagnosis/recovery.
    const auto latest_checkpoint = impl->latestCheckpoint();
    for (const UInt64 checkpoint_id : retired_checkpoints.transaction_checkpoint_ids)
    {
        const auto checkpoint = impl->readCheckpointRecord(checkpoint_id);
        const String checkpoint_retirement_directory = parentPath(impl->paths.retiredCheckpointTransactionDirectory(checkpoint_id, 1));
        const auto transaction_ids = impl->numericDirectoryIDs(checkpoint_retirement_directory, "retired checkpoint transaction namespace");
        for (const UInt64 transaction_id : transaction_ids)
        {
            impl->moveWALTransactionTo(transaction_id, impl->paths.retiredCheckpointTransactionDirectory(checkpoint_id, transaction_id));
            static_cast<void>(impl->validateRetiredCheckpointTransaction(checkpoint_id, transaction_id, checkpoint));
            impl->removeStagingTransaction(transaction_id);
            impl->removeRetiredCheckpointTransaction(checkpoint_id, transaction_id);
            ++removed;
        }
        if (!transaction_ids.empty())
        {
            impl->syncDirectory(impl->paths.stagingDirectory());
            impl->syncDirectory(checkpoint_retirement_directory);
        }
        impl->removeEmptyOwnedDirectory(checkpoint_retirement_directory);
    }
    impl->retireAndSweepObsoleteCheckpointImages(
        latest_checkpoint ? std::optional<UInt64>(latest_checkpoint->checkpoint.checkpoint_id) : std::nullopt);
    return removed;
}

void AtomicDatabaseSchemaMutationStorage::retireRolledBackTransaction(const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id)
{
    std::lock_guard lock(impl->mutex);
    impl->validateGuard(guard);
    const String retired_directory = impl->paths.retiredRollbackTransactionDirectory(transaction_id);
    const bool retired_exists = impl->disk->existsFileOrDirectory(retired_directory);
    auto prepare = impl->readPrepare(transaction_id);
    if (!prepare && !retired_exists)
    {
        const UInt64 durable_high_water = impl->durableHighWater();
        if (transaction_id <= durable_high_water || (durable_high_water == 0 && !impl->hasDurableAuthorityMarker()))
            return;
        replayConflict("rolled-back schema transaction has no durable Prepare marker");
    }
    if (prepare)
    {
        if (impl->readCommit(transaction_id, *prepare))
            replayConflict("committed schema transaction cannot be retired as rolled back");
        if (impl->readRecoveryDecision(transaction_id, *prepare) != DatabaseSchemaWALRecoveryDecision::RollBackPrepared)
            replayConflict("schema transaction has no durable rollback decision");
    }
    else
    {
        prepare = impl->validateRetiredRollback(transaction_id);
    }
    impl->removeWALTransactionTemporaries(transaction_id);
    impl->persistHighWater(transaction_id);
    impl->moveWALTransactionTo(transaction_id, retired_directory);
    prepare = impl->validateRetiredRollback(transaction_id);
    if (!impl->eraseRolledBackFirstActivationNamespace(transaction_id, prepare))
    {
        impl->removeStagingTransaction(transaction_id);
        impl->syncDirectory(impl->paths.stagingDirectory());
        impl->removeRetiredRollbackTransaction(transaction_id);
    }
}

void AtomicDatabaseSchemaMutationStorage::persistValidatedCheckpoint(
    const DatabaseSchemaMutationGuard & guard,
    const DatabaseSchemaWALCheckpoint & checkpoint,
    std::string_view canonical_checkpoint,
    std::string_view canonical_inventory_snapshot,
    std::string_view canonical_schema_graph_snapshot)
{
    std::lock_guard lock(impl->mutex);
    impl->ensureLayout();
    impl->validateGuard(guard);
    const auto decoded = [&]
    {
        try
        {
            return decodeDatabaseSchemaWALCheckpoint(canonical_checkpoint, impl->limits.wal);
        }
        catch (const DatabaseSchemaWALError &)
        {
            replayConflict("checkpoint bytes are not a valid V1 record");
        }
    }();
    if (decoded != checkpoint || checkpoint.authority_state.database_uuid != impl->paths.getDatabaseUUID())
        replayConflict("checkpoint bytes differ from the supplied checkpoint identity");
    try
    {
        static_cast<void>(DatabaseSchemaWALCheckpointBuilder::validateDecoded(
            checkpoint, canonical_inventory_snapshot, canonical_schema_graph_snapshot, impl->limits.wal));
    }
    catch (const DatabaseSchemaWALError &)
    {
        replayConflict("checkpoint snapshots differ from the supplied checkpoint identity");
    }
    if (impl->disk->existsFile(impl->paths.checkpointRecordPath(checkpoint.checkpoint_id)))
    {
        const auto existing = impl->loadCheckpoint(checkpoint.checkpoint_id);
        if (existing.checkpoint != checkpoint || existing.inventory_snapshot_bytes != canonical_inventory_snapshot
            || existing.schema_graph_snapshot_bytes != canonical_schema_graph_snapshot)
            replayConflict("durable checkpoint replay differs from its keyed record");
        return;
    }
    if (impl->currentAuthorityState() != checkpoint.authority_state)
        replayConflict("checkpoint authority state is not current");

    bool covered_commit_is_durable = false;
    if (const auto prepare = impl->readPrepare(checkpoint.covered_commit.transaction_id))
    {
        const auto commit = impl->readCommit(checkpoint.covered_commit.transaction_id, *prepare);
        covered_commit_is_durable = commit && *commit == checkpoint.covered_commit;
    }
    if (!covered_commit_is_durable)
        replayConflict("checkpoint does not cover an exact durable commit");

    impl->installImmutableFile(
        impl->paths.checkpointInventorySnapshotPath(checkpoint.checkpoint_id), canonical_inventory_snapshot, "inventory", true);
    impl->installImmutableFile(
        impl->paths.checkpointSchemaGraphSnapshotPath(checkpoint.checkpoint_id), canonical_schema_graph_snapshot, "graph", true);
    impl->installImmutableFile(impl->paths.checkpointRecordPath(checkpoint.checkpoint_id), canonical_checkpoint, "checkpoint", true);
}

void AtomicDatabaseSchemaMutationStorage::compactThroughValidatedCheckpoint(
    const DatabaseSchemaMutationGuard & guard, const DatabaseSchemaWALCheckpoint & checkpoint)
{
    std::lock_guard lock(impl->mutex);
    impl->ensureLayout();
    impl->validateGuard(guard);
    const auto durable = impl->loadCheckpoint(checkpoint.checkpoint_id);
    if (durable.checkpoint != checkpoint)
        replayConflict("WAL compaction has no exact durable checkpoint");
    const UInt64 covered = checkpoint.covered_commit.transaction_id;
    impl->persistHighWater(covered);

    std::set<UInt64> removable;
    for (const UInt64 transaction_id : impl->transactionIDs())
    {
        if (transaction_id > covered)
            continue;
        const auto prepare = impl->readPrepare(transaction_id);
        if (!prepare)
            continue;
        if (!impl->readCommit(transaction_id, *prepare)
            || impl->readRecoveryDecision(transaction_id, *prepare) == DatabaseSchemaWALRecoveryDecision::RollBackPrepared)
            replayConflict("checkpoint compaction encountered an unresolved or rolled-back transaction");
        removable.insert(transaction_id);
    }

    const String retired_checkpoint_directory = parentPath(impl->paths.retiredCheckpointTransactionDirectory(checkpoint.checkpoint_id, 1));
    for (const UInt64 transaction_id : impl->numericDirectoryIDs(retired_checkpoint_directory, "retired checkpoint transaction namespace"))
    {
        static_cast<void>(impl->validateRetiredCheckpointTransaction(checkpoint.checkpoint_id, transaction_id, checkpoint));
        removable.insert(transaction_id);
    }

    for (const UInt64 transaction_id : removable)
    {
        impl->removeWALTransactionTemporaries(transaction_id);
        const String destination = impl->paths.retiredCheckpointTransactionDirectory(checkpoint.checkpoint_id, transaction_id);
        impl->moveWALTransactionTo(transaction_id, destination);
        static_cast<void>(impl->validateRetiredCheckpointTransaction(checkpoint.checkpoint_id, transaction_id, checkpoint));
    }

    for (const UInt64 transaction_id : removable)
        impl->removeStagingTransaction(transaction_id);
    if (!removable.empty())
        impl->syncDirectory(impl->paths.stagingDirectory());
    for (const UInt64 transaction_id : removable)
        impl->removeRetiredCheckpointTransaction(checkpoint.checkpoint_id, transaction_id);
    if (!removable.empty())
        impl->syncDirectory(retired_checkpoint_directory);
    impl->removeEmptyOwnedDirectory(retired_checkpoint_directory);
}

}
