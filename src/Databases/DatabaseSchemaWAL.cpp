#include <Databases/DatabaseSchemaWAL.h>

#include <Databases/UDT/AuthorityRoot.h>

#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <algorithm>
#include <array>
#include <limits>
#include <numeric>
#include <tuple>
#include <utility>

namespace DB::UDT
{
namespace
{

constexpr std::string_view schema_wal_hash_domain = "ClickHouse UDT schema WAL transaction V1";
constexpr std::string_view exact_repair_artifact_manifest_hash_domain = "ClickHouse UDT schema WAL exact repair artifact manifest V1";
constexpr UInt16 extension_version = 1;
constexpr UInt16 prepare_exact_repair_extension_flag = UInt16{1} << 0;
constexpr UInt16 prepare_exact_repair_provenance_extension_flag = UInt16{1} << 1;
constexpr UInt16 supported_prepare_extension_flags = prepare_exact_repair_extension_flag | prepare_exact_repair_provenance_extension_flag;
constexpr UInt16 checkpoint_exact_repair_provenance_extension_flag = UInt16{1} << 0;
constexpr UInt16 supported_checkpoint_extension_flags = checkpoint_exact_repair_provenance_extension_flag;
constexpr UInt8 staged_artifact_envelope_discriminant = 4;
constexpr UInt64 schema_object_id_bytes = sizeof(UInt8) + 2 * sizeof(CanonicalUUID);
constexpr UInt64 dependency_edge_bytes = 2 * schema_object_id_bytes + sizeof(UInt8);
constexpr UInt64 authority_delta_minimum_bytes = sizeof(UInt16) + sizeof(UInt8) + sizeof(CanonicalUUID) + 2 * sizeof(UInt8);
constexpr UInt64 dependent_object_delta_minimum_bytes = schema_object_id_bytes + 2 * sizeof(UInt8);
constexpr UInt64 staged_artifact_ref_minimum_bytes
    = 2 * sizeof(UInt8) + schema_object_id_bytes + sizeof(UInt64) + sizeof(UInt8) + sizeof(Digest);
/// Fixed validation/anchor scratch which can coexist with the encoded input
/// and every decoded Prepare vector. Canonical payloads are not part of this
/// control-memory domain.
constexpr UInt64 prepare_decode_transient_control_bytes = database_schema_wal_prepare_decode_transient_control_bytes;
[[noreturn]] void fail(DatabaseSchemaWALError::Code code, std::string_view message)
{
    throw DatabaseSchemaWALError(code, message);
}

UInt64 varUIntSize(UInt64 value) noexcept
{
    UInt64 result = 1;
    while (value >= 0x80)
    {
        value >>= 7;
        ++result;
    }
    return result;
}

UInt64 checkedSize(size_t value, std::string_view message)
{
    if (!std::in_range<UInt64>(value))
        fail(DatabaseSchemaWALError::Code::LimitExceeded, message);
    return static_cast<UInt64>(value);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(DatabaseSchemaWALError::Code::LimitExceeded, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(DatabaseSchemaWALError::Code::LimitExceeded, message);
    return lhs * rhs;
}

void validateLimits(const DatabaseSchemaWALLimits & limits)
{
    constexpr UInt64 maximum_count = 1ULL << 24;
    constexpr UInt64 maximum_bytes = 1ULL << 30;
    if (limits.maximum_authority_record_deltas == 0 || limits.maximum_dependent_object_deltas == 0 || limits.maximum_graph_node_deltas == 0
        || limits.maximum_graph_edge_deltas == 0 || limits.maximum_staged_artifacts == 0 || limits.maximum_staged_artifact_bytes == 0
        || limits.maximum_total_staged_artifact_bytes == 0 || limits.maximum_encoded_bytes <= sizeof(Digest)
        || limits.maximum_decode_control_bytes < database_schema_wal_prepare_minimum_decode_control_bytes)
    {
        fail(DatabaseSchemaWALError::Code::InvalidConfiguration, "every schema-WAL limit must be nonzero");
    }
    if (limits.maximum_authority_record_deltas > maximum_count || limits.maximum_dependent_object_deltas > maximum_count
        || limits.maximum_graph_node_deltas > maximum_count || limits.maximum_graph_edge_deltas > maximum_count
        || limits.maximum_staged_artifacts > maximum_count || limits.maximum_staged_artifact_bytes > maximum_bytes
        || limits.maximum_total_staged_artifact_bytes > maximum_bytes || limits.maximum_encoded_bytes > maximum_bytes
        || limits.maximum_decode_control_bytes > maximum_bytes)
    {
        fail(DatabaseSchemaWALError::Code::InvalidConfiguration, "a schema-WAL limit exceeds the implementation maximum");
    }
    if (limits.maximum_staged_artifact_bytes > limits.maximum_total_staged_artifact_bytes)
        fail(DatabaseSchemaWALError::Code::InvalidConfiguration, "the per-artifact limit exceeds the total artifact limit");
}

UInt64 prefixByteLimit(const DatabaseSchemaWALLimits & limits)
{
    validateLimits(limits);
    return limits.maximum_encoded_bytes - sizeof(Digest);
}

class Writer final
{
public:
    explicit Writer(UInt64 maximum_bytes_)
        : maximum_bytes(maximum_bytes_)
    {
    }

    void writeByte(UInt8 value)
    {
        require(sizeof(value));
        output.push_back(static_cast<char>(value));
    }

    void writeUInt16LE(UInt16 value)
    {
        require(sizeof(value));
        output.push_back(static_cast<char>(value));
        output.push_back(static_cast<char>(value >> 8));
    }

    void writeUInt64LE(UInt64 value)
    {
        require(sizeof(value));
        for (size_t index = 0; index < sizeof(value); ++index)
            output.push_back(static_cast<char>(value >> (8 * index)));
    }

    void writeVarUInt(UInt64 value)
    {
        require(varUIntSize(value));
        while (value >= 0x80)
        {
            output.push_back(static_cast<char>(static_cast<UInt8>(value) | 0x80));
            value >>= 7;
        }
        output.push_back(static_cast<char>(value));
    }

    template <size_t size>
    void writeArray(const std::array<CanonicalByte, size> & value)
    {
        require(size);
        output.append(reinterpret_cast<const char *>(value.data()), value.size());
    }

    void writeUUID(const UUID & value) { writeArray(uuidToCanonicalBytes(value)); }

    void writeBytes(std::string_view value)
    {
        require(checkedSize(value.size(), "schema-WAL byte span does not fit UInt64"));
        output.append(value);
    }

    void writeFrame(std::string_view value)
    {
        writeVarUInt(checkedSize(value.size(), "schema-WAL frame size does not fit UInt64"));
        writeBytes(value);
    }

    String release() && { return std::move(output); }

private:
    void require(UInt64 count) const
    {
        if (output.size() > maximum_bytes || count > maximum_bytes - output.size())
            fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL record exceeds its size limit");
    }

    UInt64 maximum_bytes;
    String output;
};

class Reader final
{
public:
    Reader(std::string_view bytes_, UInt64 maximum_bytes)
        : bytes(bytes_)
    {
        if (bytes.size() > maximum_bytes)
            fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL record exceeds its size limit");
    }

    UInt8 readByte()
    {
        require(sizeof(UInt8));
        return static_cast<UInt8>(bytes[position++]);
    }

    UInt16 readUInt16LE()
    {
        require(sizeof(UInt16));
        const UInt16 result = static_cast<UInt16>(
            static_cast<UInt8>(bytes[position]) | (static_cast<UInt16>(static_cast<UInt8>(bytes[position + 1])) << 8));
        position += sizeof(UInt16);
        return result;
    }

    UInt64 readUInt64LE()
    {
        require(sizeof(UInt64));
        UInt64 result = 0;
        for (size_t index = 0; index < sizeof(UInt64); ++index)
            result |= static_cast<UInt64>(static_cast<UInt8>(bytes[position + index])) << (8 * index);
        position += sizeof(UInt64);
        return result;
    }

    UInt64 readMinimalVarUInt(UInt64 maximum)
    {
        UInt64 result = 0;
        UInt8 shift = 0;
        size_t encoded_size = 0;
        while (true)
        {
            const UInt8 byte = readByte();
            ++encoded_size;
            if (shift == 63 && (byte & 0xfe) != 0)
                fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL VarUInt overflows UInt64");
            result |= static_cast<UInt64>(byte & 0x7f) << shift;
            if ((byte & 0x80) == 0)
                break;
            if (shift >= 63)
                fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL VarUInt overflows UInt64");
            shift = static_cast<UInt8>(shift + 7);
        }
        if (encoded_size != varUIntSize(result))
            fail(DatabaseSchemaWALError::Code::NonCanonical, "schema-WAL VarUInt is not minimally encoded");
        if (result > maximum)
            fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL count exceeds its limit");
        return result;
    }

    UInt64 readCount(UInt64 maximum, UInt64 minimum_item_bytes)
    {
        const UInt64 count = readMinimalVarUInt(maximum);
        if (minimum_item_bytes != 0 && count > remaining() / minimum_item_bytes)
            fail(DatabaseSchemaWALError::Code::Truncated, "schema-WAL item count exceeds the remaining record bytes");
        return count;
    }

    std::string_view readFrame(UInt64 maximum)
    {
        const UInt64 size = readMinimalVarUInt(maximum);
        require(size);
        const auto result = bytes.substr(position, size);
        position += size;
        return result;
    }

    template <size_t size>
    std::array<CanonicalByte, size> readArray()
    {
        require(size);
        std::array<CanonicalByte, size> result{};
        std::copy_n(reinterpret_cast<const CanonicalByte *>(bytes.data() + position), size, result.begin());
        position += size;
        return result;
    }

    UUID readUUID() { return uuidFromCanonicalBytes(readArray<sizeof(CanonicalUUID)>()); }

    UInt64 remaining() const noexcept { return bytes.size() - position; }

    void requireEnd() const
    {
        if (position != bytes.size())
            fail(DatabaseSchemaWALError::Code::TrailingData, "schema-WAL record has trailing data");
    }

private:
    void require(UInt64 count) const
    {
        if (count > bytes.size() - position)
            fail(DatabaseSchemaWALError::Code::Truncated, "schema-WAL record is truncated");
    }

    std::string_view bytes;
    size_t position = 0;
};

UInt8 encodeAuthorityRecordKind(AuthorityInventoryRecordKind kind)
{
    switch (kind)
    {
        case AuthorityInventoryRecordKind::TypeDefinition: return 1;
        case AuthorityInventoryRecordKind::SidecarExpectation: return 2;
    }
    fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL authority-record kind is invalid");
}

AuthorityInventoryRecordKind decodeAuthorityRecordKind(UInt8 value)
{
    switch (value)
    {
        case 1: return AuthorityInventoryRecordKind::TypeDefinition;
        case 2: return AuthorityInventoryRecordKind::SidecarExpectation;
        default: fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL authority-record kind is invalid");
    }
}

UInt8 encodeSchemaObjectKind(SchemaObjectKind kind)
{
    switch (kind)
    {
        case SchemaObjectKind::Table: return 1;
        case SchemaObjectKind::View: return 2;
        case SchemaObjectKind::Dictionary: return 3;
        case SchemaObjectKind::TypeDefinition: return 4;
        case SchemaObjectKind::SyntheticTestObject: return 254;
    }
    fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL schema-object kind is invalid");
}

SchemaObjectKind decodeSchemaObjectKind(UInt8 value)
{
    switch (value)
    {
        case 1: return SchemaObjectKind::Table;
        case 2: return SchemaObjectKind::View;
        case 3: return SchemaObjectKind::Dictionary;
        case 4: return SchemaObjectKind::TypeDefinition;
        case 254: return SchemaObjectKind::SyntheticTestObject;
        default: fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL schema-object kind is invalid");
    }
}

UInt8 encodeEdgeKind(SchemaObjectDependencyEdgeKind kind)
{
    switch (kind)
    {
        case SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition: return 1;
        case SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition: return 2;
        case SchemaObjectDependencyEdgeKind::ObjectDependsOnObject: return 3;
    }
    fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL graph-edge kind is invalid");
}

SchemaObjectDependencyEdgeKind decodeEdgeKind(UInt8 value)
{
    switch (value)
    {
        case 1: return SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition;
        case 2: return SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition;
        case 3: return SchemaObjectDependencyEdgeKind::ObjectDependsOnObject;
        default: fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL graph-edge kind is invalid");
    }
}

UInt8 encodeArtifactKind(DatabaseSchemaWALStagedArtifactKind kind)
{
    switch (kind)
    {
        case DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord: return 1;
        case DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord: return 2;
        case DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata: return 3;
        case DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar: return 4;
        case DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord: return 5;
    }
    fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL staged-artifact kind is invalid");
}

DatabaseSchemaWALStagedArtifactKind decodeArtifactKind(UInt8 value)
{
    switch (value)
    {
        case 1: return DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord;
        case 2: return DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord;
        case 3: return DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata;
        case 4: return DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar;
        case 5: return DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord;
        default: fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL staged-artifact kind is invalid");
    }
}

UInt8 encodeArtifactImage(DatabaseSchemaWALStagedArtifactImage image)
{
    switch (image)
    {
        case DatabaseSchemaWALStagedArtifactImage::Before: return 1;
        case DatabaseSchemaWALStagedArtifactImage::After: return 2;
    }
    fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL staged-artifact image is invalid");
}

DatabaseSchemaWALStagedArtifactImage decodeArtifactImage(UInt8 value)
{
    switch (value)
    {
        case 1: return DatabaseSchemaWALStagedArtifactImage::Before;
        case 2: return DatabaseSchemaWALStagedArtifactImage::After;
        default: fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL staged-artifact image is invalid");
    }
}

void writeSchemaObjectID(Writer & writer, const SchemaObjectID & object)
{
    writer.writeByte(encodeSchemaObjectKind(object.kind));
    writer.writeUUID(object.database_uuid);
    writer.writeUUID(object.object_uuid);
}

SchemaObjectID readSchemaObjectID(Reader & reader)
{
    return SchemaObjectID{
        .kind = decodeSchemaObjectKind(reader.readByte()),
        .database_uuid = reader.readUUID(),
        .object_uuid = reader.readUUID(),
    };
}

void writeAuthorityKey(Writer & writer, const AuthorityInventoryKey & key)
{
    writer.writeUInt16LE(key.format_version);
    writer.writeByte(encodeAuthorityRecordKind(key.record_kind));
    writer.writeUUID(key.object_uuid);
}

AuthorityInventoryKey readAuthorityKey(Reader & reader)
{
    return AuthorityInventoryKey{
        .format_version = reader.readUInt16LE(),
        .record_kind = decodeAuthorityRecordKind(reader.readByte()),
        .object_uuid = reader.readUUID(),
    };
}

void writeAuthorityRecordState(Writer & writer, const std::optional<DatabaseSchemaWALAuthorityRecordState> & state)
{
    writer.writeByte(state ? 1 : 0);
    if (state)
    {
        writer.writeUInt64LE(state->object_revision);
        writer.writeArray(state->canonical_record_hash);
    }
}

std::optional<DatabaseSchemaWALAuthorityRecordState> readAuthorityRecordState(Reader & reader)
{
    const UInt8 present = reader.readByte();
    if (present > 1)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL authority-record presence tag is invalid");
    if (!present)
        return std::nullopt;
    return DatabaseSchemaWALAuthorityRecordState{
        .object_revision = reader.readUInt64LE(),
        .canonical_record_hash = reader.readArray<sizeof(Digest)>(),
    };
}

void writeDependentObjectState(Writer & writer, const std::optional<DatabaseSchemaWALDependentObjectState> & state)
{
    writer.writeByte(state ? 1 : 0);
    if (!state)
        return;
    writer.writeUInt64LE(state->object_schema_revision);
    writer.writeArray(state->metadata_hash);
    writer.writeByte(state->sidecar_record_hash ? 1 : 0);
    if (state->sidecar_record_hash)
        writer.writeArray(*state->sidecar_record_hash);
    writer.writeByte(state->expectation_record_hash ? 1 : 0);
    if (state->expectation_record_hash)
        writer.writeArray(*state->expectation_record_hash);
}

std::optional<DatabaseSchemaWALDependentObjectState> readDependentObjectState(Reader & reader)
{
    const UInt8 present = reader.readByte();
    if (present > 1)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL dependent-object presence tag is invalid");
    if (!present)
        return std::nullopt;
    DatabaseSchemaWALDependentObjectState result{
        .object_schema_revision = reader.readUInt64LE(),
        .metadata_hash = reader.readArray<sizeof(Digest)>(),
        .sidecar_record_hash = std::nullopt,
        .expectation_record_hash = std::nullopt,
    };
    const UInt8 sidecar_present = reader.readByte();
    if (sidecar_present > 1)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL sidecar presence tag is invalid");
    if (sidecar_present)
        result.sidecar_record_hash = reader.readArray<sizeof(Digest)>();
    const UInt8 expectation_present = reader.readByte();
    if (expectation_present > 1)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL expectation presence tag is invalid");
    if (expectation_present)
        result.expectation_record_hash = reader.readArray<sizeof(Digest)>();
    return result;
}

void writeGraphEdge(Writer & writer, const SchemaObjectDependencyEdge & edge)
{
    writeSchemaObjectID(writer, edge.dependent);
    writeSchemaObjectID(writer, edge.dependency);
    writer.writeByte(encodeEdgeKind(edge.kind));
}

SchemaObjectDependencyEdge readGraphEdge(Reader & reader)
{
    return SchemaObjectDependencyEdge{
        .dependent = readSchemaObjectID(reader),
        .dependency = readSchemaObjectID(reader),
        .kind = decodeEdgeKind(reader.readByte()),
    };
}

void writeAuthorityStateFrame(Writer & writer, const AuthorityState & state, const DatabaseSchemaWALLimits & limits)
{
    writer.writeFrame(encodeAuthorityState(state, limits.authority_state));
}

AuthorityState readAuthorityStateFrame(Reader & reader, const DatabaseSchemaWALLimits & limits)
{
    try
    {
        return decodeAuthorityState(reader.readFrame(limits.authority_state.maximum_encoded_bytes), limits.authority_state);
    }
    catch (const AuthorityStateError &)
    {
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL authority-state frame is invalid");
    }
}

void writeExtension(Writer & writer, UInt16 flags = 0)
{
    writer.writeUInt16LE(extension_version);
    writer.writeUInt16LE(flags);
}

UInt16 readExtension(Reader & reader, UInt16 supported_flags = 0)
{
    if (reader.readUInt16LE() != extension_version)
        fail(DatabaseSchemaWALError::Code::UnsupportedVersion, "unsupported schema-WAL extension");
    const UInt16 flags = reader.readUInt16LE();
    if ((flags & ~supported_flags) != 0)
        fail(DatabaseSchemaWALError::Code::UnsupportedVersion, "unsupported schema-WAL extension flags");
    return flags;
}

void writeExactRepairProvenance(Writer & writer, const DatabaseSchemaWALExactRepairProvenance & provenance)
{
    writer.writeUInt64LE(provenance.transaction_id);
    writer.writeUInt64LE(provenance.damaged_artifact_count);
    writer.writeArray(provenance.damaged_artifact_manifest_digest);
    writer.writeUInt64LE(provenance.local_wal_sources);
    writer.writeUInt64LE(provenance.replicated_authority_sources);
    writer.writeUInt64LE(provenance.verified_backup_sources);
    writer.writeUInt64LE(provenance.previous_catalog_epoch);
    writer.writeArray(provenance.previous_authority_anchor);
    writer.writeUInt64LE(provenance.repaired_catalog_epoch);
    writer.writeArray(provenance.repaired_authority_anchor);
}

DatabaseSchemaWALExactRepairProvenance readExactRepairProvenance(Reader & reader)
{
    return {
        .transaction_id = reader.readUInt64LE(),
        .damaged_artifact_count = reader.readUInt64LE(),
        .damaged_artifact_manifest_digest = reader.readArray<sizeof(Digest)>(),
        .local_wal_sources = reader.readUInt64LE(),
        .replicated_authority_sources = reader.readUInt64LE(),
        .verified_backup_sources = reader.readUInt64LE(),
        .previous_catalog_epoch = reader.readUInt64LE(),
        .previous_authority_anchor = reader.readArray<sizeof(Digest)>(),
        .repaired_catalog_epoch = reader.readUInt64LE(),
        .repaired_authority_anchor = reader.readArray<sizeof(Digest)>(),
    };
}

void validateExactRepairProvenanceFields(const DatabaseSchemaWALExactRepairProvenance & provenance)
{
    if (provenance.transaction_id == 0 || provenance.damaged_artifact_count == 0 || provenance.previous_catalog_epoch == 0
        || provenance.previous_catalog_epoch == std::numeric_limits<UInt64>::max()
        || provenance.repaired_catalog_epoch != provenance.previous_catalog_epoch + 1
        || provenance.damaged_artifact_manifest_digest == Digest{} || provenance.previous_authority_anchor == Digest{}
        || provenance.repaired_authority_anchor == Digest{} || provenance.previous_authority_anchor == provenance.repaired_authority_anchor)
    {
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL exact-repair provenance identity is invalid");
    }
    const UInt64 selected_sources = checkedAdd(
        checkedAdd(
            provenance.local_wal_sources, provenance.replicated_authority_sources, "schema-WAL exact-repair source count overflows UInt64"),
        provenance.verified_backup_sources,
        "schema-WAL exact-repair source count overflows UInt64");
    if (selected_sources != provenance.damaged_artifact_count)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL exact-repair provenance source counts are inconsistent");
}

void readAndValidateHeader(Reader & reader, DatabaseSchemaWALRecordKind expected_kind)
{
    if (reader.readUInt16LE() != database_schema_wal_format_version)
        fail(DatabaseSchemaWALError::Code::UnsupportedVersion, "unsupported schema-WAL record version");
    if (reader.readByte() != static_cast<UInt8>(expected_kind))
        fail(DatabaseSchemaWALError::Code::UnknownRecordKind, "schema-WAL record kind does not match the decoder");
}

bool authorityDeltaLess(const DatabaseSchemaWALAuthorityRecordDelta & lhs, const DatabaseSchemaWALAuthorityRecordDelta & rhs) noexcept
{
    return authorityInventoryKeyLess(lhs.key, rhs.key);
}

bool dependentDeltaLess(const DatabaseSchemaWALDependentObjectDelta & lhs, const DatabaseSchemaWALDependentObjectDelta & rhs) noexcept
{
    return lhs.object < rhs.object;
}

template <typename Artifact>
bool artifactLess(const Artifact & lhs, const Artifact & rhs) noexcept
{
    if (lhs.kind != rhs.kind)
        return static_cast<UInt8>(lhs.kind) < static_cast<UInt8>(rhs.kind);
    if (lhs.object != rhs.object)
        return lhs.object < rhs.object;
    if (lhs.revision != rhs.revision)
        return lhs.revision < rhs.revision;
    return static_cast<UInt8>(lhs.image) < static_cast<UInt8>(rhs.image);
}

template <typename Value, typename Less>
void validateStrictOrder(std::span<const Value> values, Less less, DatabaseSchemaWALError::Code code, std::string_view message)
{
    for (size_t index = 1; index < values.size(); ++index)
    {
        if (!less(values[index - 1], values[index]))
            fail(code, message);
    }
}

template <typename Value, typename Less>
bool hasIntersection(std::span<const Value> lhs, std::span<const Value> rhs, Less less)
{
    size_t lhs_index = 0;
    size_t rhs_index = 0;
    while (lhs_index < lhs.size() && rhs_index < rhs.size())
    {
        if (less(lhs[lhs_index], rhs[rhs_index]))
            ++lhs_index;
        else if (less(rhs[rhs_index], lhs[lhs_index]))
            ++rhs_index;
        else
            return true;
    }
    return false;
}

void validateAuthorityState(const AuthorityState & state, const DatabaseSchemaWALLimits & limits)
{
    try
    {
        static_cast<void>(encodeAuthorityState(state, limits.authority_state));
    }
    catch (const AuthorityStateError &)
    {
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL authority state is invalid");
    }
}

void validateAuthorityKey(const AuthorityInventoryKey & key)
{
    if (key.format_version != authority_inventory_format_version)
        fail(DatabaseSchemaWALError::Code::UnsupportedVersion, "unsupported schema-WAL authority-key version");
    static_cast<void>(encodeAuthorityRecordKind(key.record_kind));
    if (key.object_uuid == UUIDHelpers::Nil)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL authority-key UUID is nil");
}

void validateAuthorityRecordState(const DatabaseSchemaWALAuthorityRecordState & state)
{
    if (state.object_revision == 0)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL authority-record revision is zero");
}

void validateDependentObjectState(const DatabaseSchemaWALDependentObjectState & state)
{
    if (state.object_schema_revision == 0)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL dependent-object revision is zero");
    if (state.sidecar_record_hash.has_value() != state.expectation_record_hash.has_value())
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL sidecar and expectation presence differ");
}

void validateSchemaObjectID(const SchemaObjectID & object, UUID database_uuid)
{
    if (!object.isValid() || object.database_uuid != database_uuid)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL schema-object identity is invalid");
}

void validateDependentObjectID(const SchemaObjectID & object, UUID database_uuid)
{
    validateSchemaObjectID(object, database_uuid);
    if (object.kind == SchemaObjectKind::TypeDefinition)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL dependent object uses the type-definition kind");
}

bool graphDeltaEmpty(const SchemaObjectDependencyGraphMutation & delta) noexcept
{
    return delta.node_additions.empty() && delta.node_removals.empty() && delta.edge_additions.empty() && delta.edge_removals.empty();
}

void validateGraphEdge(const SchemaObjectDependencyEdge & edge, UUID database_uuid)
{
    validateSchemaObjectID(edge.dependent, database_uuid);
    validateSchemaObjectID(edge.dependency, database_uuid);
    static_cast<void>(encodeEdgeKind(edge.kind));
    const bool dependent_is_definition = edge.dependent.kind == SchemaObjectKind::TypeDefinition;
    const bool dependency_is_definition = edge.dependency.kind == SchemaObjectKind::TypeDefinition;
    switch (edge.kind)
    {
        case SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition:
            if (!dependent_is_definition || !dependency_is_definition)
                fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL definition edge has incompatible endpoints");
            return;
        case SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition:
            if (dependent_is_definition || !dependency_is_definition)
                fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL object-to-definition edge has incompatible endpoints");
            return;
        case SchemaObjectDependencyEdgeKind::ObjectDependsOnObject:
            if (dependent_is_definition || dependency_is_definition)
                fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL object edge has incompatible endpoints");
            return;
    }
    fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL graph-edge kind is invalid");
}

void validateGraphDelta(const SchemaObjectDependencyGraphMutation & delta, UUID database_uuid, const DatabaseSchemaWALLimits & limits)
{
    const UInt64 node_count = checkedAdd(
        checkedSize(delta.node_additions.size(), "schema-WAL graph-node count does not fit UInt64"),
        checkedSize(delta.node_removals.size(), "schema-WAL graph-node count does not fit UInt64"),
        "schema-WAL total graph-node delta overflows UInt64");
    const UInt64 edge_count = checkedAdd(
        checkedSize(delta.edge_additions.size(), "schema-WAL graph-edge count does not fit UInt64"),
        checkedSize(delta.edge_removals.size(), "schema-WAL graph-edge count does not fit UInt64"),
        "schema-WAL total graph-edge delta overflows UInt64");
    if (node_count > limits.maximum_graph_node_deltas)
        fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL total graph-node delta exceeds its limit");
    if (edge_count > limits.maximum_graph_edge_deltas)
        fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL total graph-edge delta exceeds its limit");

    validateStrictOrder<SchemaObjectID>(
        delta.node_additions,
        std::less<>{},
        DatabaseSchemaWALError::Code::DuplicateDelta,
        "schema-WAL graph-node additions are not unique and sorted");
    validateStrictOrder<SchemaObjectID>(
        delta.node_removals,
        std::less<>{},
        DatabaseSchemaWALError::Code::DuplicateDelta,
        "schema-WAL graph-node removals are not unique and sorted");
    validateStrictOrder<SchemaObjectDependencyEdge>(
        delta.edge_additions,
        std::less<>{},
        DatabaseSchemaWALError::Code::DuplicateDelta,
        "schema-WAL graph-edge additions are not unique and sorted");
    validateStrictOrder<SchemaObjectDependencyEdge>(
        delta.edge_removals,
        std::less<>{},
        DatabaseSchemaWALError::Code::DuplicateDelta,
        "schema-WAL graph-edge removals are not unique and sorted");

    if (hasIntersection<SchemaObjectID>(delta.node_additions, delta.node_removals, std::less<>{})
        || hasIntersection<SchemaObjectDependencyEdge>(delta.edge_additions, delta.edge_removals, std::less<>{}))
    {
        fail(DatabaseSchemaWALError::Code::ConflictingDelta, "schema-WAL graph delta adds and removes the same value");
    }

    for (const auto & node : delta.node_additions)
        validateSchemaObjectID(node, database_uuid);
    for (const auto & node : delta.node_removals)
        validateSchemaObjectID(node, database_uuid);
    for (const auto & edge : delta.edge_additions)
        validateGraphEdge(edge, database_uuid);
    for (const auto & edge : delta.edge_removals)
        validateGraphEdge(edge, database_uuid);
}

void validateArtifactRef(const DatabaseSchemaWALStagedArtifactRef & artifact, UUID database_uuid, const DatabaseSchemaWALLimits & limits)
{
    static_cast<void>(encodeArtifactKind(artifact.kind));
    static_cast<void>(encodeArtifactImage(artifact.image));
    validateSchemaObjectID(artifact.object, database_uuid);
    if (artifact.revision == 0)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL staged-artifact revision is zero");
    if (artifact.byte_size > limits.maximum_staged_artifact_bytes)
        fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL staged artifact exceeds its byte limit");
    const bool is_definition = artifact.object.kind == SchemaObjectKind::TypeDefinition;
    if ((artifact.kind == DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord) != is_definition)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL staged-artifact kind and object identity disagree");
}

void validateStagedArtifactRefs(
    std::span<const DatabaseSchemaWALStagedArtifactRef> artifacts, UUID database_uuid, const DatabaseSchemaWALLimits & limits)
{
    if (artifacts.size() > limits.maximum_staged_artifacts)
        fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL staged-artifact count exceeds its limit");
    validateStrictOrder<DatabaseSchemaWALStagedArtifactRef>(
        artifacts,
        artifactLess<DatabaseSchemaWALStagedArtifactRef>,
        DatabaseSchemaWALError::Code::DuplicateDelta,
        "schema-WAL staged-artifact manifest is not unique and sorted");
    UInt64 total_bytes = 0;
    for (const auto & artifact : artifacts)
    {
        validateArtifactRef(artifact, database_uuid, limits);
        total_bytes = checkedAdd(total_bytes, artifact.byte_size, "schema-WAL total staged-artifact size overflows UInt64");
    }
    if (total_bytes > limits.maximum_total_staged_artifact_bytes)
        fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL total staged-artifact size exceeds its limit");
}

void preflightStagedArtifactBudget(std::span<const DatabaseSchemaWALStagedArtifact> artifacts, const DatabaseSchemaWALLimits & limits)
{
    const UInt64 artifact_count = checkedSize(artifacts.size(), "schema-WAL staged-artifact count does not fit UInt64");
    if (artifact_count > limits.maximum_staged_artifacts)
        fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL staged-artifact count exceeds its limit");

    UInt64 total_bytes = 0;
    for (const auto & artifact : artifacts)
    {
        const UInt64 byte_size = checkedSize(artifact.canonical_bytes.size(), "schema-WAL staged-artifact size does not fit UInt64");
        if (byte_size > limits.maximum_staged_artifact_bytes)
            fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL staged artifact exceeds its byte limit");
        total_bytes = checkedAdd(total_bytes, byte_size, "schema-WAL total staged-artifact size overflows UInt64");
    }
    if (total_bytes > limits.maximum_total_staged_artifact_bytes)
        fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL total staged-artifact size exceeds its limit");
}

void canonicalizePrepare(DatabaseSchemaWALPrepare & prepare)
{
    std::sort(prepare.authority_record_deltas.begin(), prepare.authority_record_deltas.end(), authorityDeltaLess);
    std::sort(prepare.dependent_object_deltas.begin(), prepare.dependent_object_deltas.end(), dependentDeltaLess);
    std::sort(prepare.graph_delta.node_additions.begin(), prepare.graph_delta.node_additions.end());
    std::sort(prepare.graph_delta.node_removals.begin(), prepare.graph_delta.node_removals.end());
    std::sort(prepare.graph_delta.edge_additions.begin(), prepare.graph_delta.edge_additions.end());
    std::sort(prepare.graph_delta.edge_removals.begin(), prepare.graph_delta.edge_removals.end());
}

void validatePrepareFields(const DatabaseSchemaWALPrepare & prepare, const DatabaseSchemaWALLimits & limits)
{
    validateLimits(limits);
    if (prepare.format_version != database_schema_wal_format_version)
        fail(DatabaseSchemaWALError::Code::UnsupportedVersion, "unsupported schema-WAL prepare version");
    if (prepare.transaction_id == 0)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL transaction ID is zero");
    validateAuthorityState(prepare.after_authority_state, limits);
    const UUID database_uuid = prepare.after_authority_state.database_uuid;
    if (prepare.before_authority_state)
    {
        validateAuthorityState(*prepare.before_authority_state, limits);
        if (prepare.before_authority_state->database_uuid != database_uuid)
            fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL authority states belong to different databases");
    }
    if (prepare.exact_repair_provenance)
    {
        const auto & provenance = *prepare.exact_repair_provenance;
        validateExactRepairProvenanceFields(provenance);
        if (!prepare.exact_repair || !prepare.before_authority_state || provenance.transaction_id != prepare.transaction_id
            || provenance.previous_catalog_epoch != prepare.before_authority_state->database_catalog_epoch
            || provenance.previous_authority_anchor != prepare.before_authority_state->anchor_hash
            || provenance.repaired_catalog_epoch != prepare.after_authority_state.database_catalog_epoch
            || provenance.repaired_authority_anchor != prepare.after_authority_state.anchor_hash)
        {
            fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL exact-repair provenance does not match its transition");
        }
    }

    if (prepare.authority_record_deltas.size() > limits.maximum_authority_record_deltas)
        fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL authority-record delta count exceeds its limit");
    validateStrictOrder<DatabaseSchemaWALAuthorityRecordDelta>(
        prepare.authority_record_deltas,
        authorityDeltaLess,
        DatabaseSchemaWALError::Code::DuplicateDelta,
        "schema-WAL authority-record deltas are not unique and sorted");
    for (const auto & delta : prepare.authority_record_deltas)
    {
        validateAuthorityKey(delta.key);
        if (!delta.before && !delta.after)
            fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL authority-record delta has no image");
        if (delta.before)
            validateAuthorityRecordState(*delta.before);
        if (delta.after)
            validateAuthorityRecordState(*delta.after);
        if (delta.before && delta.after && *delta.before == *delta.after)
            fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL authority-record delta is unchanged");
    }

    if (prepare.dependent_object_deltas.size() > limits.maximum_dependent_object_deltas)
        fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL dependent-object delta count exceeds its limit");
    validateStrictOrder<DatabaseSchemaWALDependentObjectDelta>(
        prepare.dependent_object_deltas,
        dependentDeltaLess,
        DatabaseSchemaWALError::Code::DuplicateDelta,
        "schema-WAL dependent-object deltas are not unique and sorted");
    for (const auto & delta : prepare.dependent_object_deltas)
    {
        validateDependentObjectID(delta.object, database_uuid);
        if (!delta.before && !delta.after)
            fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL dependent-object delta has no image");
        if (delta.before)
            validateDependentObjectState(*delta.before);
        if (delta.after)
            validateDependentObjectState(*delta.after);
        if (delta.before && delta.after && *delta.before == *delta.after)
            fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL dependent-object delta is unchanged");
    }

    validateGraphDelta(prepare.graph_delta, database_uuid, limits);
    validateStagedArtifactRefs(prepare.staged_artifacts, database_uuid, limits);
    if (prepare.exact_repair_provenance && prepare.exact_repair_provenance->damaged_artifact_count > prepare.staged_artifacts.size())
    {
        fail(
            DatabaseSchemaWALError::Code::InvalidValue,
            "schema-WAL exact-repair provenance names more damaged artifacts than the transition repairs");
    }
    const UInt64 decoder_control_bytes = checkedAdd(
        getDatabaseSchemaWALPrepareDecodedControlBytes(prepare),
        prepare_decode_transient_control_bytes,
        "schema-WAL Prepare decoded control byte count overflows UInt64");
    if (decoder_control_bytes > limits.maximum_decode_control_bytes)
        fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL Prepare exceeds its decoded control byte limit");
}

void writeArtifactRef(Writer & writer, const DatabaseSchemaWALStagedArtifactRef & artifact)
{
    writer.writeByte(encodeArtifactKind(artifact.kind));
    writer.writeByte(encodeArtifactImage(artifact.image));
    writeSchemaObjectID(writer, artifact.object);
    writer.writeUInt64LE(artifact.revision);
    writer.writeVarUInt(artifact.byte_size);
    writer.writeArray(artifact.content_hash);
}

DatabaseSchemaWALStagedArtifactRef readArtifactRef(Reader & reader, const DatabaseSchemaWALLimits & limits)
{
    return DatabaseSchemaWALStagedArtifactRef{
        .kind = decodeArtifactKind(reader.readByte()),
        .image = decodeArtifactImage(reader.readByte()),
        .object = readSchemaObjectID(reader),
        .revision = reader.readUInt64LE(),
        .byte_size = reader.readMinimalVarUInt(limits.maximum_staged_artifact_bytes),
        .content_hash = reader.readArray<sizeof(Digest)>(),
    };
}

String encodePreparePrefix(const DatabaseSchemaWALPrepare & prepare, const DatabaseSchemaWALLimits & limits)
{
    validatePrepareFields(prepare, limits);
    Writer writer(prefixByteLimit(limits));
    writer.writeUInt16LE(prepare.format_version);
    writer.writeByte(static_cast<UInt8>(DatabaseSchemaWALRecordKind::Prepare));
    writer.writeUInt64LE(prepare.transaction_id);
    writer.writeByte(prepare.before_authority_state ? 1 : 0);
    if (prepare.before_authority_state)
        writeAuthorityStateFrame(writer, *prepare.before_authority_state, limits);
    writeAuthorityStateFrame(writer, prepare.after_authority_state, limits);

    writer.writeVarUInt(prepare.authority_record_deltas.size());
    for (const auto & delta : prepare.authority_record_deltas)
    {
        writeAuthorityKey(writer, delta.key);
        writeAuthorityRecordState(writer, delta.before);
        writeAuthorityRecordState(writer, delta.after);
    }

    writer.writeVarUInt(prepare.dependent_object_deltas.size());
    for (const auto & delta : prepare.dependent_object_deltas)
    {
        writeSchemaObjectID(writer, delta.object);
        writeDependentObjectState(writer, delta.before);
        writeDependentObjectState(writer, delta.after);
    }

    const auto write_nodes = [&writer](const std::vector<SchemaObjectID> & nodes)
    {
        writer.writeVarUInt(nodes.size());
        for (const auto & node : nodes)
            writeSchemaObjectID(writer, node);
    };
    const auto write_edges = [&writer](const std::vector<SchemaObjectDependencyEdge> & edges)
    {
        writer.writeVarUInt(edges.size());
        for (const auto & edge : edges)
            writeGraphEdge(writer, edge);
    };
    write_nodes(prepare.graph_delta.node_additions);
    write_nodes(prepare.graph_delta.node_removals);
    write_edges(prepare.graph_delta.edge_additions);
    write_edges(prepare.graph_delta.edge_removals);

    writer.writeVarUInt(prepare.staged_artifacts.size());
    for (const auto & artifact : prepare.staged_artifacts)
        writeArtifactRef(writer, artifact);
    UInt16 extension_flags = prepare.exact_repair ? prepare_exact_repair_extension_flag : 0;
    if (prepare.exact_repair_provenance)
        extension_flags |= prepare_exact_repair_provenance_extension_flag;
    writeExtension(writer, extension_flags);
    if (prepare.exact_repair_provenance)
        writeExactRepairProvenance(writer, *prepare.exact_repair_provenance);
    return std::move(writer).release();
}

void validateCommitFields(const DatabaseSchemaWALCommit & commit, const DatabaseSchemaWALLimits & limits)
{
    validateLimits(limits);
    if (commit.format_version != database_schema_wal_format_version)
        fail(DatabaseSchemaWALError::Code::UnsupportedVersion, "unsupported schema-WAL commit version");
    if (commit.transaction_id == 0 || commit.database_uuid == UUIDHelpers::Nil || commit.database_catalog_epoch == 0)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL commit identity is invalid");
}

String encodeCommitPrefix(const DatabaseSchemaWALCommit & commit, const DatabaseSchemaWALLimits & limits)
{
    validateCommitFields(commit, limits);
    Writer writer(prefixByteLimit(limits));
    writer.writeUInt16LE(commit.format_version);
    writer.writeByte(static_cast<UInt8>(DatabaseSchemaWALRecordKind::Commit));
    writer.writeUInt64LE(commit.transaction_id);
    writer.writeUUID(commit.database_uuid);
    writer.writeUInt64LE(commit.database_catalog_epoch);
    writer.writeArray(commit.inventory_root);
    writer.writeArray(commit.schema_graph_root);
    writer.writeArray(commit.authority_anchor);
    writer.writeArray(commit.prepare_hash);
    writeExtension(writer);
    return std::move(writer).release();
}

void validateCheckpointFields(const DatabaseSchemaWALCheckpoint & checkpoint, const DatabaseSchemaWALLimits & limits)
{
    validateLimits(limits);
    if (checkpoint.format_version != database_schema_wal_format_version)
        fail(DatabaseSchemaWALError::Code::UnsupportedVersion, "unsupported schema-WAL checkpoint version");
    if (checkpoint.checkpoint_id == 0)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL checkpoint ID is zero");
    validateCommitFields(checkpoint.covered_commit, limits);
    validateAuthorityState(checkpoint.authority_state, limits);
    if (checkpoint.last_exact_repair_provenance)
    {
        const auto & provenance = *checkpoint.last_exact_repair_provenance;
        validateExactRepairProvenanceFields(provenance);
        if (provenance.damaged_artifact_count > limits.maximum_staged_artifacts)
        {
            fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL checkpoint exact-repair provenance exceeds its artifact limit");
        }
        if (provenance.transaction_id > checkpoint.covered_commit.transaction_id
            || provenance.repaired_catalog_epoch > checkpoint.authority_state.database_catalog_epoch)
        {
            fail(
                DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL checkpoint exact-repair provenance is outside its covered history");
        }
        const UInt64 following_transactions = checkpoint.covered_commit.transaction_id - provenance.transaction_id;
        const UInt64 following_catalog_epochs = checkpoint.authority_state.database_catalog_epoch - provenance.repaired_catalog_epoch;
        if ((following_transactions == 0) != (following_catalog_epochs == 0) || following_transactions < following_catalog_epochs
            || (following_transactions == 0 && provenance.repaired_authority_anchor != checkpoint.authority_state.anchor_hash))
        {
            fail(
                DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL checkpoint exact-repair provenance is outside its covered history");
        }
    }
}

String encodeCheckpointPrefix(const DatabaseSchemaWALCheckpoint & checkpoint, const DatabaseSchemaWALLimits & limits)
{
    validateCheckpointFields(checkpoint, limits);
    Writer writer(prefixByteLimit(limits));
    writer.writeUInt16LE(checkpoint.format_version);
    writer.writeByte(static_cast<UInt8>(DatabaseSchemaWALRecordKind::Checkpoint));
    writer.writeUInt64LE(checkpoint.checkpoint_id);
    writer.writeFrame(encodeDatabaseSchemaWALCommit(checkpoint.covered_commit, limits));
    writeAuthorityStateFrame(writer, checkpoint.authority_state, limits);
    writer.writeArray(checkpoint.inventory_snapshot_hash);
    writer.writeArray(checkpoint.schema_graph_snapshot_hash);
    const UInt16 extension_flags = checkpoint.last_exact_repair_provenance ? checkpoint_exact_repair_provenance_extension_flag : 0;
    writeExtension(writer, extension_flags);
    if (checkpoint.last_exact_repair_provenance)
        writeExactRepairProvenance(writer, *checkpoint.last_exact_repair_provenance);
    return std::move(writer).release();
}

AuthorityInventoryLeaf leafFromState(const AuthorityInventoryKey & key, const DatabaseSchemaWALAuthorityRecordState & state)
{
    return AuthorityInventoryLeaf{
        .key = key,
        .object_revision = state.object_revision,
        .canonical_record_hash = state.canonical_record_hash,
    };
}

void validateReplacementRevision(const DatabaseSchemaWALAuthorityRecordDelta & delta)
{
    if (!delta.before || !delta.after)
        return;
    const UInt64 before = delta.before->object_revision;
    const UInt64 after = delta.after->object_revision;
    if (delta.key.record_kind == AuthorityInventoryRecordKind::TypeDefinition)
    {
        if (after == before)
            return;
        if (before != std::numeric_limits<UInt64>::max() && after == before + 1)
            return;
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL definition revision transition is invalid");
    }
    if (before == std::numeric_limits<UInt64>::max() || after != before + 1)
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL expectation revision must advance exactly once");
}

AuthorityInventory::Ptr applyAuthorityDeltas(
    const AuthorityInventory::Ptr & base,
    std::span<const DatabaseSchemaWALAuthorityRecordDelta> deltas,
    const DatabaseSchemaWALLimits & limits)
{
    std::vector<AuthorityInventoryLeafDelta> inventory_deltas;
    inventory_deltas.reserve(deltas.size());
    for (const auto & delta : deltas)
    {
        validateReplacementRevision(delta);
        inventory_deltas.push_back({
            .key = delta.key,
            .before = delta.before ? std::optional<AuthorityInventoryLeaf>(leafFromState(delta.key, *delta.before)) : std::nullopt,
            .after = delta.after ? std::optional<AuthorityInventoryLeaf>(leafFromState(delta.key, *delta.after)) : std::nullopt,
        });
    }

    try
    {
        return AuthorityInventory::applyMutation(base, inventory_deltas, limits.inventory_snapshot.inventory);
    }
    catch (const AuthorityInventoryError & error)
    {
        if (error.code == AuthorityInventoryError::Code::LimitExceeded)
            fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL authority inventory exceeds its limit");
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL authority inventory transition is invalid");
    }
}

bool sameInventorySummary(const AuthorityInventorySummary & summary, const AuthorityState & state) noexcept
{
    return summary.leaf_count == state.leaf_count && summary.merkle_radix_root == state.inventory_root;
}

void validatePinnedInventoryLimits(const AuthorityInventory::Ptr & inventory, const AuthorityInventoryLimits & limits)
{
    try
    {
        static_cast<void>(AuthorityInventory::applyMutation(inventory, {}, limits));
    }
    catch (const AuthorityInventoryError & error)
    {
        if (error.code == AuthorityInventoryError::Code::LimitExceeded)
            fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL pinned authority inventory exceeds its limit");
        fail(DatabaseSchemaWALError::Code::InvalidConfiguration, "schema-WAL authority inventory limits are invalid");
    }
}

void validatePinnedGraphLimits(const SchemaObjectDependencyGraph::Ptr & graph, const SchemaObjectDependencyGraphLimits & limits)
{
    try
    {
        graph->validateAgainstLimits(limits);
    }
    catch (const SchemaObjectDependencyGraphError & error)
    {
        if (error.code == SchemaObjectDependencyGraphError::Code::LimitExceeded)
            fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL pinned schema graph exceeds its limit");
        fail(DatabaseSchemaWALError::Code::InvalidConfiguration, "schema-WAL schema graph limits are invalid");
    }
}

void validateBaseState(const DatabaseSchemaWALTransitionBase & base, UUID database_uuid, const DatabaseSchemaWALLimits & limits)
{
    if (!base.authority_inventory || !base.schema_graph)
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL transition base is incomplete");
    validatePinnedInventoryLimits(base.authority_inventory, limits.inventory_snapshot.inventory);
    validatePinnedGraphLimits(base.schema_graph, limits.schema_graph);
    if (base.schema_graph->getDatabaseUUID() != database_uuid)
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL base graph belongs to another database");
    if (base.authority_state)
    {
        if (base.authority_state->database_uuid != database_uuid
            || !sameInventorySummary(base.authority_inventory->getSummary(), *base.authority_state)
            || base.schema_graph->computeRoot() != base.authority_state->schema_graph_root)
        {
            fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL actual base snapshots do not match the authority state");
        }
    }
    else if (base.authority_inventory->getSummary().leaf_count != 0 || base.schema_graph->getNodeCount() != 0)
    {
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL first enablement requires empty base snapshots");
    }
}

bool isDefinitionOnlyAuthority(UInt64 capabilities) noexcept
{
    return capabilities == definition_authority_capability_mask;
}

bool isDependentObjectCapableAuthority(UInt64 capabilities) noexcept
{
    return capabilities == dependent_object_authority_capability_mask;
}

void validateCapabilityUse(const DatabaseSchemaWALPrepare & prepare)
{
    if (isDependentObjectCapableAuthority(prepare.after_authority_state.persistent_capability_mask))
        return;
    if (!prepare.dependent_object_deltas.empty())
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL definition-only transition contains dependent-object deltas");
    for (const auto & delta : prepare.authority_record_deltas)
    {
        if (delta.key.record_kind != AuthorityInventoryRecordKind::TypeDefinition)
            fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL definition-only transition contains an expectation delta");
    }
    const auto validate_node = [](const SchemaObjectID & node)
    {
        if (node.kind != SchemaObjectKind::TypeDefinition)
            fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL definition-only graph contains a dependent-object node");
    };
    for (const auto & node : prepare.graph_delta.node_additions)
        validate_node(node);
    for (const auto & node : prepare.graph_delta.node_removals)
        validate_node(node);
    const auto validate_edge = [](const SchemaObjectDependencyEdge & edge)
    {
        if (edge.kind != SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition)
            fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL definition-only graph contains a dependent-object edge");
    };
    for (const auto & edge : prepare.graph_delta.edge_additions)
        validate_edge(edge);
    for (const auto & edge : prepare.graph_delta.edge_removals)
        validate_edge(edge);
}

void validateDependentRevisions(const DatabaseSchemaWALPrepare & prepare)
{
    for (const auto & delta : prepare.dependent_object_deltas)
    {
        if (!delta.before || !delta.after)
            continue;
        if (delta.before->object_schema_revision == std::numeric_limits<UInt64>::max()
            || delta.after->object_schema_revision != delta.before->object_schema_revision + 1)
        {
            fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL dependent-object revision must advance exactly once");
        }
    }
}

SchemaObjectDependencyGraph::Ptr
applyGraphDelta(const SchemaObjectDependencyGraph::Ptr & base, const SchemaObjectDependencyGraphMutation & delta)
{
    try
    {
        return SchemaObjectDependencyGraph::applyMutation(base, delta);
    }
    catch (const SchemaObjectDependencyGraphError & error)
    {
        if (error.code == SchemaObjectDependencyGraphError::Code::LimitExceeded)
            fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL graph transition exceeds its limit");
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL graph delta does not apply to the actual base graph");
    }
}

size_t findArtifact(
    std::span<const DatabaseSchemaWALStagedArtifactRef> artifacts,
    DatabaseSchemaWALStagedArtifactKind kind,
    DatabaseSchemaWALStagedArtifactImage image,
    const SchemaObjectID & object,
    UInt64 revision)
{
    const DatabaseSchemaWALStagedArtifactRef key{
        .kind = kind,
        .image = image,
        .object = object,
        .revision = revision,
    };
    const auto it = std::lower_bound(artifacts.begin(), artifacts.end(), key, artifactLess<DatabaseSchemaWALStagedArtifactRef>);
    if (it == artifacts.end() || it->kind != kind || it->image != image || it->object != object || it->revision != revision)
        fail(DatabaseSchemaWALError::Code::MissingArtifact, "schema-WAL required staged artifact is missing");
    return static_cast<size_t>(it - artifacts.begin());
}

const DatabaseSchemaWALAuthorityRecordDelta *
findAuthorityDelta(std::span<const DatabaseSchemaWALAuthorityRecordDelta> deltas, const AuthorityInventoryKey & key)
{
    const DatabaseSchemaWALAuthorityRecordDelta target{.key = key, .before = std::nullopt, .after = std::nullopt};
    const auto it = std::lower_bound(deltas.begin(), deltas.end(), target, authorityDeltaLess);
    if (it == deltas.end() || it->key != key)
        return nullptr;
    return &*it;
}

size_t
authorityDeltaIndex(std::span<const DatabaseSchemaWALAuthorityRecordDelta> deltas, const DatabaseSchemaWALAuthorityRecordDelta * delta)
{
    return static_cast<size_t>(delta - deltas.data());
}

const std::optional<DatabaseSchemaWALAuthorityRecordState> &
selectAuthorityImage(const DatabaseSchemaWALAuthorityRecordDelta & delta, DatabaseSchemaWALStagedArtifactImage image)
{
    return image == DatabaseSchemaWALStagedArtifactImage::Before ? delta.before : delta.after;
}

const std::optional<DatabaseSchemaWALDependentObjectState> &
selectDependentImage(const DatabaseSchemaWALDependentObjectDelta & delta, DatabaseSchemaWALStagedArtifactImage image)
{
    return image == DatabaseSchemaWALStagedArtifactImage::Before ? delta.before : delta.after;
}

void claimArtifact(size_t index, std::vector<bool> & claimed)
{
    if (claimed[index])
        fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL staged artifact is claimed more than once");
    claimed[index] = true;
}

Record decodeDefinitionArtifact(
    std::span<const DatabaseSchemaWALStagedArtifactRef> artifacts,
    std::span<const String> bytes,
    std::vector<bool> & claimed,
    DatabaseSchemaWALStagedArtifactImage image,
    const SchemaObjectID & object,
    const DatabaseSchemaWALAuthorityRecordState & state,
    const DatabaseSchemaWALLimits & limits)
{
    const size_t index
        = findArtifact(artifacts, DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord, image, object, state.object_revision);
    claimArtifact(index, claimed);
    try
    {
        const auto record = decodeRecord(bytes[index], limits.definition_record);
        if (record.identity.database_uuid != object.database_uuid || record.identity.type_uuid != object.object_uuid
            || record.identity.revision != state.object_revision
            || computeRecordHash(record, limits.definition_record) != state.canonical_record_hash
            || encodeRecord(record, limits.definition_record) != bytes[index])
        {
            fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL definition artifact does not match its authority image");
        }
        return record;
    }
    catch (const RecordError &)
    {
        fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL definition artifact is invalid");
    }
}

bool sameDefinitionRevisionPayload(Record lhs, const Record & rhs)
{
    /// A rename or comment edit may update the recovery SQL spelling without
    /// creating a new immutable definition revision. Every other byte remains
    /// part of that revision and must stay exact.
    lhs.normalized_name = rhs.normalized_name;
    lhs.normalized_local_name = rhs.normalized_local_name;
    lhs.canonical_definition_sql = rhs.canonical_definition_sql;
    lhs.canonical_physical_template_sql = rhs.canonical_physical_template_sql;
    lhs.comment = rhs.comment;
    return lhs == rhs;
}

SidecarExpectationRecord decodeExpectationArtifact(
    std::span<const DatabaseSchemaWALStagedArtifactRef> artifacts,
    std::span<const String> bytes,
    std::vector<bool> & claimed,
    DatabaseSchemaWALStagedArtifactImage image,
    const SchemaObjectID & object,
    const DatabaseSchemaWALAuthorityRecordState & authority_state)
{
    const size_t index = findArtifact(
        artifacts, DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord, image, object, authority_state.object_revision);
    claimArtifact(index, claimed);
    try
    {
        const auto record = decodeSidecarExpectationRecord(bytes[index]);
        if (record.object != object || record.object_schema_revision != authority_state.object_revision
            || computeSidecarExpectationRecordHash(record) != authority_state.canonical_record_hash
            || encodeSidecarExpectationRecord(record) != bytes[index])
        {
            fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL expectation artifact does not match its authority image");
        }
        return record;
    }
    catch (const SidecarExpectationRecordError &)
    {
        fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL expectation artifact is invalid");
    }
}

PersistedTypeReferences decodeSidecarArtifact(
    std::span<const DatabaseSchemaWALStagedArtifactRef> artifacts,
    std::span<const String> bytes,
    std::vector<bool> & claimed,
    DatabaseSchemaWALStagedArtifactImage image,
    const SchemaObjectID & object,
    UInt64 revision,
    const DatabaseSchemaWALLimits & limits)
{
    const size_t index
        = findArtifact(artifacts, DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar, image, object, revision);
    claimArtifact(index, claimed);
    try
    {
        const auto record = decodePersistedTypeReferences(bytes[index], limits.persisted_references);
        if (record.object != object || record.object_schema_revision != revision
            || encodePersistedTypeReferences(record, limits.persisted_references) != bytes[index])
        {
            fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL sidecar artifact does not match its object image");
        }
        return record;
    }
    catch (const PersistedTypeReferencesError &)
    {
        fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL sidecar artifact is invalid");
    }
}

DependentObjectMetadataInstallationRecord decodeInstallationArtifact(
    std::span<const DatabaseSchemaWALStagedArtifactRef> artifacts,
    std::span<const String> bytes,
    std::vector<bool> & claimed,
    DatabaseSchemaWALStagedArtifactImage image,
    const SchemaObjectID & object,
    UInt64 revision,
    const Digest & expected_record_hash,
    const DatabaseSchemaWALLimits & limits)
{
    const size_t index
        = findArtifact(artifacts, DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord, image, object, revision);
    claimArtifact(index, claimed);
    try
    {
        const auto record = decodeDependentObjectMetadataInstallationRecord(bytes[index], limits.installation_record);
        if (record.object != object || record.object_schema_revision != revision
            || computeDependentObjectMetadataInstallationRecordHash(record, limits.installation_record) != expected_record_hash
            || encodeDependentObjectMetadataInstallationRecord(record, limits.installation_record) != bytes[index])
        {
            fail(
                DatabaseSchemaWALError::Code::ArtifactMismatch,
                "schema-WAL metadata-installation artifact does not match its expectation image");
        }
        return record;
    }
    catch (const DependentObjectMetadataInstallationRecordError &)
    {
        fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL metadata-installation artifact is invalid");
    }
}

void validateArtifactContentAddresses(
    std::span<const DatabaseSchemaWALStagedArtifactRef> artifacts, std::span<const String> bytes, const DatabaseSchemaWALLimits & limits)
{
    if (bytes.size() != artifacts.size())
        fail(DatabaseSchemaWALError::Code::MissingArtifact, "schema-WAL staged-artifact byte count does not match the manifest");
    UInt64 total_bytes = 0;
    for (size_t index = 0; index < artifacts.size(); ++index)
    {
        const UInt64 byte_size = checkedSize(bytes[index].size(), "schema-WAL staged-artifact size does not fit UInt64");
        total_bytes = checkedAdd(total_bytes, byte_size, "schema-WAL total staged-artifact size overflows UInt64");
        if (artifacts[index].byte_size != byte_size
            || artifacts[index].content_hash != computeDatabaseSchemaWALStagedArtifactHash(artifacts[index].kind, bytes[index]))
        {
            fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL staged-artifact content address does not match");
        }
    }
    if (total_bytes > limits.maximum_total_staged_artifact_bytes)
        fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL total staged-artifact size exceeds its limit");
}

void validateUniqueDependentUUIDs(std::span<const DatabaseSchemaWALDependentObjectDelta> deltas)
{
    std::vector<CanonicalUUID> uuids;
    uuids.reserve(deltas.size());
    for (const auto & delta : deltas)
        uuids.push_back(uuidToCanonicalBytes(delta.object.object_uuid));
    std::sort(uuids.begin(), uuids.end());
    if (std::adjacent_find(uuids.begin(), uuids.end()) != uuids.end())
        fail(DatabaseSchemaWALError::Code::ConflictingDelta, "schema-WAL dependent-object UUID occurs under multiple identities");
}

void validateTableDependentGraphMembership(
    const DatabaseSchemaWALPrepare & prepare,
    const SchemaObjectDependencyGraph & before_graph,
    const SchemaObjectDependencyGraph & after_graph)
{
    const auto validate_image = [](const DatabaseSchemaWALDependentObjectDelta & delta,
                                   const std::optional<DatabaseSchemaWALDependentObjectState> & state,
                                   const SchemaObjectDependencyGraph & graph)
    {
        const bool logical_image = state && state->sidecar_record_hash && state->expectation_record_hash;
        if (graph.containsNode(delta.object) != logical_image)
        {
            fail(
                DatabaseSchemaWALError::Code::TransitionMismatch,
                "schema-WAL table graph membership differs from its logical metadata image");
        }
    };

    for (const auto & delta : prepare.dependent_object_deltas)
    {
        if (delta.object.kind != SchemaObjectKind::Table)
            continue;
        validate_image(delta, delta.before, before_graph);
        validate_image(delta, delta.after, after_graph);
    }
}

void validateArtifacts(const DatabaseSchemaWALPrepare & prepare, std::span<const String> bytes, const DatabaseSchemaWALLimits & limits)
{
    const auto artifacts = std::span<const DatabaseSchemaWALStagedArtifactRef>(prepare.staged_artifacts);
    validateArtifactContentAddresses(artifacts, bytes, limits);
    validateUniqueDependentUUIDs(prepare.dependent_object_deltas);
    std::vector<bool> claimed(artifacts.size(), false);
    std::vector<bool> linked_expectation_delta(prepare.authority_record_deltas.size(), false);
    const UUID database_uuid = prepare.after_authority_state.database_uuid;

    for (const auto & delta : prepare.authority_record_deltas)
    {
        if (delta.key.record_kind != AuthorityInventoryRecordKind::TypeDefinition)
            continue;
        const SchemaObjectID object{
            .kind = SchemaObjectKind::TypeDefinition,
            .database_uuid = database_uuid,
            .object_uuid = delta.key.object_uuid,
        };
        std::optional<Record> before_record;
        std::optional<Record> after_record;
        if (delta.before)
            before_record = decodeDefinitionArtifact(
                artifacts, bytes, claimed, DatabaseSchemaWALStagedArtifactImage::Before, object, *delta.before, limits);
        if (delta.after)
            after_record = decodeDefinitionArtifact(
                artifacts, bytes, claimed, DatabaseSchemaWALStagedArtifactImage::After, object, *delta.after, limits);
        if (delta.before && delta.after && delta.before->object_revision == delta.after->object_revision
            && !sameDefinitionRevisionPayload(*before_record, *after_record))
        {
            fail(
                DatabaseSchemaWALError::Code::TransitionMismatch,
                "schema-WAL same-revision definition replacement changes immutable definition semantics");
        }
    }

    const auto validate_dependent_image
        = [&](const DatabaseSchemaWALDependentObjectDelta & delta,
              DatabaseSchemaWALStagedArtifactImage image) -> std::optional<DependentObjectMetadataInstallationRecord>
    {
        const auto & state = selectDependentImage(delta, image);
        const AuthorityInventoryKey expectation_key{
            .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
            .object_uuid = delta.object.object_uuid,
        };
        const auto * expectation_delta = findAuthorityDelta(prepare.authority_record_deltas, expectation_key);
        const auto * expectation_state = expectation_delta ? &selectAuthorityImage(*expectation_delta, image) : nullptr;
        if (!state)
        {
            if (expectation_state && *expectation_state)
                fail(DatabaseSchemaWALError::Code::ConflictingDelta, "schema-WAL expectation survives an absent dependent-object image");
            return std::nullopt;
        }

        const size_t metadata_index = findArtifact(
            artifacts, DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, image, delta.object, state->object_schema_revision);
        claimArtifact(metadata_index, claimed);
        if (computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, bytes[metadata_index])
            != state->metadata_hash)
        {
            fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL metadata artifact does not match its object image");
        }

        if (!state->sidecar_record_hash)
        {
            if (expectation_state && *expectation_state)
                fail(DatabaseSchemaWALError::Code::ConflictingDelta, "schema-WAL physical-only object image has an expectation");
            return std::nullopt;
        }
        if (!expectation_delta || !expectation_state || !*expectation_state)
            fail(DatabaseSchemaWALError::Code::ConflictingDelta, "schema-WAL logical object image lacks its authority expectation");
        if ((*expectation_state)->object_revision != state->object_schema_revision
            || (*expectation_state)->canonical_record_hash != *state->expectation_record_hash)
        {
            fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL object and expectation authority images disagree");
        }
        linked_expectation_delta[authorityDeltaIndex(prepare.authority_record_deltas, expectation_delta)] = true;

        const auto expectation = decodeExpectationArtifact(artifacts, bytes, claimed, image, delta.object, **expectation_state);
        const auto sidecar = decodeSidecarArtifact(artifacts, bytes, claimed, image, delta.object, state->object_schema_revision, limits);
        if (computePersistedTypeReferencesSidecarHash(sidecar, limits.persisted_references) != *state->sidecar_record_hash
            || expectation.sidecar_hash != *state->sidecar_record_hash
            || expectation.physical_schema_fingerprint != sidecar.physical_schema_fingerprint)
        {
            fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL sidecar, expectation and object image disagree");
        }
        if (delta.object.kind == SchemaObjectKind::Table && !expectation.installation_record_hash)
        {
            fail(DatabaseSchemaWALError::Code::MissingArtifact, "schema-WAL logical table image lacks its metadata installation record");
        }
        if (delta.object.kind == SchemaObjectKind::SyntheticTestObject && expectation.installation_record_hash)
        {
            fail(
                DatabaseSchemaWALError::Code::ArtifactMismatch,
                "schema-WAL synthetic object image unexpectedly addresses a metadata installation record");
        }
        std::optional<DependentObjectMetadataInstallationRecord> installation;
        if (expectation.installation_record_hash)
        {
            installation = decodeInstallationArtifact(
                artifacts,
                bytes,
                claimed,
                image,
                delta.object,
                state->object_schema_revision,
                *expectation.installation_record_hash,
                limits);
            if (installation->metadata_artifact_hash != state->metadata_hash)
            {
                fail(
                    DatabaseSchemaWALError::Code::ArtifactMismatch,
                    "schema-WAL metadata installation record addresses another metadata artifact");
            }
        }
        return installation;
    };

    for (const auto & delta : prepare.dependent_object_deltas)
    {
        const auto before_installation = validate_dependent_image(delta, DatabaseSchemaWALStagedArtifactImage::Before);
        const auto after_installation = validate_dependent_image(delta, DatabaseSchemaWALStagedArtifactImage::After);
        if (delta.object.kind != SchemaObjectKind::Table)
            continue;
        if (!before_installation && !after_installation)
        {
            fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL table delta has no mapped metadata image");
        }
        /// A mapped Atomic table rename is represented by two exact logical
        /// images with successive object revisions and distinct installation
        /// names. The durable storage adapter validates and switches both
        /// ordinary metadata paths as one replayable transition.
    }

    for (size_t index = 0; index < prepare.authority_record_deltas.size(); ++index)
    {
        if (prepare.authority_record_deltas[index].key.record_kind == AuthorityInventoryRecordKind::SidecarExpectation
            && !linked_expectation_delta[index])
        {
            fail(DatabaseSchemaWALError::Code::ConflictingDelta, "schema-WAL expectation delta has no matching dependent-object image");
        }
    }
    if (std::find(claimed.begin(), claimed.end(), false) != claimed.end())
        fail(DatabaseSchemaWALError::Code::UnexpectedArtifact, "schema-WAL staged-artifact manifest contains an unexpected entry");
}

void validateExactRepairArtifacts(
    const DatabaseSchemaWALPrepare & prepare,
    const AuthorityInventory & inventory,
    const SchemaObjectDependencyGraph & graph,
    std::span<const String> bytes,
    const DatabaseSchemaWALLimits & limits)
{
    const auto artifacts = std::span<const DatabaseSchemaWALStagedArtifactRef>(prepare.staged_artifacts);
    validateArtifactContentAddresses(artifacts, bytes, limits);
    if (artifacts.empty())
        fail(DatabaseSchemaWALError::Code::MissingArtifact, "schema-WAL exact repair contains no artifact");

    struct RepairedExpectation
    {
        SchemaObjectID object;
        SidecarExpectationRecord record;
    };
    std::vector<RepairedExpectation> expectations;
    expectations.reserve(artifacts.size());

    for (size_t index = 0; index < artifacts.size(); ++index)
    {
        const auto & artifact = artifacts[index];
        if (artifact.image != DatabaseSchemaWALStagedArtifactImage::After)
            fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL exact repair contains a non-After artifact image");
        if (!graph.containsNode(artifact.object))
            fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL exact repair artifact is absent from the anchored graph");

        try
        {
            switch (artifact.kind)
            {
                case DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord: {
                    if (artifact.object.kind != SchemaObjectKind::TypeDefinition)
                        fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL exact repair definition identity is invalid");
                    const Record record = decodeRecord(bytes[index], limits.definition_record);
                    const AuthorityInventoryKey key{
                        .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                        .object_uuid = artifact.object.object_uuid,
                    };
                    const auto * leaf = inventory.find(key);
                    if (!leaf || record.identity.database_uuid != artifact.object.database_uuid
                        || record.identity.type_uuid != artifact.object.object_uuid || record.identity.revision != artifact.revision
                        || leaf->object_revision != artifact.revision
                        || leaf->canonical_record_hash != computeRecordHash(record, limits.definition_record)
                        || encodeRecord(record, limits.definition_record) != bytes[index])
                    {
                        fail(
                            DatabaseSchemaWALError::Code::ArtifactMismatch,
                            "schema-WAL exact repair definition differs from its anchored leaf");
                    }
                    break;
                }
                case DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord: {
                    if (artifact.object.kind == SchemaObjectKind::TypeDefinition)
                        fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL exact repair expectation identity is invalid");
                    const SidecarExpectationRecord record = decodeSidecarExpectationRecord(bytes[index]);
                    const AuthorityInventoryKey key{
                        .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
                        .object_uuid = artifact.object.object_uuid,
                    };
                    const auto * leaf = inventory.find(key);
                    if (!leaf || record.object != artifact.object || record.object_schema_revision != artifact.revision
                        || leaf->object_revision != artifact.revision
                        || leaf->canonical_record_hash != computeSidecarExpectationRecordHash(record)
                        || encodeSidecarExpectationRecord(record) != bytes[index])
                    {
                        fail(
                            DatabaseSchemaWALError::Code::ArtifactMismatch,
                            "schema-WAL exact repair expectation differs from its anchored leaf");
                    }
                    expectations.push_back({artifact.object, std::move(record)});
                    break;
                }
                case DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar: {
                    if (artifact.object.kind == SchemaObjectKind::TypeDefinition)
                        fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL exact repair sidecar identity is invalid");
                    const PersistedTypeReferences references = decodePersistedTypeReferences(bytes[index], limits.persisted_references);
                    if (references.object != artifact.object || references.object_schema_revision != artifact.revision
                        || encodePersistedTypeReferences(references, limits.persisted_references) != bytes[index])
                    {
                        fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL exact repair sidecar identity is invalid");
                    }
                    break;
                }
                case DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata:
                case DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord:
                    fail(
                        DatabaseSchemaWALError::Code::UnexpectedArtifact,
                        "schema-WAL exact repair contains an unauthoritative artifact kind");
            }
        }
        catch (const RecordError &)
        {
            fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL exact repair definition is malformed");
        }
        catch (const SidecarExpectationRecordError &)
        {
            fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL exact repair expectation is malformed");
        }
        catch (const PersistedTypeReferencesError &)
        {
            fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL exact repair sidecar is malformed");
        }
    }

    std::sort(expectations.begin(), expectations.end(), [](const auto & lhs, const auto & rhs) { return lhs.object < rhs.object; });
    for (size_t index = 1; index < expectations.size(); ++index)
    {
        if (expectations[index - 1].object == expectations[index].object)
            fail(DatabaseSchemaWALError::Code::DuplicateDelta, "schema-WAL exact repair repeats an expectation object");
    }

    for (size_t index = 0; index < artifacts.size(); ++index)
    {
        const auto & artifact = artifacts[index];
        if (artifact.kind != DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar)
            continue;
        const auto expectation_it = std::lower_bound(
            expectations.begin(),
            expectations.end(),
            artifact.object,
            [](const auto & value, const SchemaObjectID & object) { return value.object < object; });
        if (expectation_it == expectations.end() || expectation_it->object != artifact.object)
            fail(DatabaseSchemaWALError::Code::MissingArtifact, "schema-WAL exact sidecar repair lacks its rooted expectation image");
        try
        {
            const PersistedTypeReferences references = decodePersistedTypeReferences(bytes[index], limits.persisted_references);
            if (expectation_it->record.object_schema_revision != references.object_schema_revision
                || expectation_it->record.physical_schema_fingerprint != references.physical_schema_fingerprint
                || expectation_it->record.sidecar_hash
                    != computePersistedTypeReferencesSidecarHash(references, limits.persisted_references))
            {
                fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL exact repair sidecar differs from its rooted expectation");
            }
        }
        catch (const PersistedTypeReferencesError &)
        {
            fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL exact repair sidecar is malformed");
        }
    }
}

struct TransitionResult
{
    AuthorityInventory::Ptr inventory;
    SchemaObjectDependencyGraph::Ptr graph;
};

TransitionResult validateTransition(
    const DatabaseSchemaWALPrepare & prepare,
    const DatabaseSchemaWALTransitionBase & base,
    std::span<const String> staged_artifact_bytes,
    const DatabaseSchemaWALLimits & limits)
{
    validatePrepareFields(prepare, limits);
    const auto & after = prepare.after_authority_state;
    validateBaseState(base, after.database_uuid, limits);
    if (prepare.before_authority_state != base.authority_state)
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL declared before-state does not match the actual base state");

    if (prepare.exact_repair)
    {
        if (!base.authority_state || prepare.authority_record_deltas.size() != 0 || prepare.dependent_object_deltas.size() != 0
            || !graphDeltaEmpty(prepare.graph_delta) || prepare.staged_artifacts.empty())
        {
            fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL exact repair has a content mutation or no repair image");
        }
        const auto & before = *base.authority_state;
        if (before.database_catalog_epoch == std::numeric_limits<UInt64>::max()
            || after.database_catalog_epoch != before.database_catalog_epoch + 1 || after.database_uuid != before.database_uuid
            || after.persistent_capability_mask != before.persistent_capability_mask || after.leaf_count != before.leaf_count
            || after.inventory_root != before.inventory_root || after.schema_graph_root != before.schema_graph_root)
        {
            fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL exact repair is not a content-neutral epoch successor");
        }
    }
    else if (!base.authority_state)
    {
        if (after.database_catalog_epoch != 1 || !isDefinitionOnlyAuthority(after.persistent_capability_mask))
            fail(
                DatabaseSchemaWALError::Code::TransitionMismatch,
                "schema-WAL first enablement must create a definition-only authority at epoch one");
        if (!prepare.dependent_object_deltas.empty())
            fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL first enablement cannot mutate dependent objects");
    }
    else
    {
        const auto & before = *base.authority_state;
        if (before.database_catalog_epoch == std::numeric_limits<UInt64>::max()
            || after.database_catalog_epoch != before.database_catalog_epoch + 1)
        {
            fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL database catalog epoch must advance exactly once");
        }
        const bool activating = isDefinitionOnlyAuthority(before.persistent_capability_mask)
            && isDependentObjectCapableAuthority(after.persistent_capability_mask);
        if (after.persistent_capability_mask != before.persistent_capability_mask && !activating)
            fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL capability transition is invalid");
        if (activating)
        {
            AuthorityState expected;
            try
            {
                expected = activateDependentObjectAuthority(before, limits.authority_state);
            }
            catch (const AuthorityStateError &)
            {
                fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL dependent-object-capable activation state is invalid");
            }
            if (after != expected || !prepare.authority_record_deltas.empty() || !prepare.dependent_object_deltas.empty()
                || !graphDeltaEmpty(prepare.graph_delta) || !prepare.staged_artifacts.empty())
            {
                fail(
                    DatabaseSchemaWALError::Code::TransitionMismatch,
                    "schema-WAL dependent-object-capable activation must be content-neutral");
            }
        }
    }
    validateCapabilityUse(prepare);
    validateDependentRevisions(prepare);

    auto inventory = applyAuthorityDeltas(base.authority_inventory, prepare.authority_record_deltas, limits);
    if (!sameInventorySummary(inventory->getSummary(), after))
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL after-state does not match the derived authority inventory");
    auto graph = applyGraphDelta(base.schema_graph, prepare.graph_delta);
    validatePinnedGraphLimits(graph, limits.schema_graph);
    if (graph->computeRoot() != after.schema_graph_root)
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL after-state does not match the derived schema graph");
    validateTableDependentGraphMembership(prepare, *base.schema_graph, *graph);
    if (prepare.exact_repair)
        validateExactRepairArtifacts(prepare, *base.authority_inventory, *base.schema_graph, staged_artifact_bytes, limits);
    else
        validateArtifacts(prepare, staged_artifact_bytes, limits);
    return {.inventory = std::move(inventory), .graph = std::move(graph)};
}

void validateCommitAgainstState(const DatabaseSchemaWALCommit & commit, const AuthorityState & state)
{
    if (commit.database_uuid != state.database_uuid || commit.database_catalog_epoch != state.database_catalog_epoch
        || commit.inventory_root != state.inventory_root || commit.schema_graph_root != state.schema_graph_root
        || commit.authority_anchor != state.anchor_hash)
    {
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL covered commit does not match the authority state");
    }
}

void validateCheckpointSnapshots(
    const DatabaseSchemaWALCheckpoint & checkpoint,
    const AuthorityInventorySnapshot & inventory_snapshot,
    const AuthorityInventory::Ptr & inventory,
    const SchemaObjectDependencyGraph::Ptr & graph)
{
    validateCommitAgainstState(checkpoint.covered_commit, checkpoint.authority_state);
    if (inventory_snapshot.database_uuid != checkpoint.authority_state.database_uuid
        || !sameInventorySummary(inventory->getSummary(), checkpoint.authority_state)
        || graph->getDatabaseUUID() != checkpoint.authority_state.database_uuid
        || graph->computeRoot() != checkpoint.authority_state.schema_graph_root)
    {
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL checkpoint snapshots do not match the authority state");
    }
}

}

DatabaseSchemaWALError::DatabaseSchemaWALError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

bool DatabaseSchemaWALPrepare::operator==(const DatabaseSchemaWALPrepare & other) const
{
    return format_version == other.format_version && transaction_id == other.transaction_id && exact_repair == other.exact_repair
        && exact_repair_provenance == other.exact_repair_provenance && before_authority_state == other.before_authority_state
        && after_authority_state == other.after_authority_state && authority_record_deltas == other.authority_record_deltas
        && dependent_object_deltas == other.dependent_object_deltas && graph_delta.node_additions == other.graph_delta.node_additions
        && graph_delta.node_removals == other.graph_delta.node_removals && graph_delta.edge_additions == other.graph_delta.edge_additions
        && graph_delta.edge_removals == other.graph_delta.edge_removals && staged_artifacts == other.staged_artifacts
        && prepare_hash == other.prepare_hash;
}

bool isDatabaseSchemaWALExactRepair(const DatabaseSchemaWALPrepare & prepare) noexcept
{
    return prepare.exact_repair;
}

DatabaseSchemaWALValidatedTransition::DatabaseSchemaWALValidatedTransition(
    DatabaseSchemaWALPrepare prepare_,
    AuthorityInventory::Ptr after_inventory_,
    SchemaObjectDependencyGraph::Ptr after_graph_,
    std::vector<String> staged_artifact_bytes_)
    : prepare(std::move(prepare_))
    , after_inventory(std::move(after_inventory_))
    , after_graph(std::move(after_graph_))
    , staged_artifact_bytes(std::move(staged_artifact_bytes_))
{
}

DatabaseSchemaWALValidatedCheckpoint::DatabaseSchemaWALValidatedCheckpoint(
    DatabaseSchemaWALCheckpoint checkpoint_,
    String inventory_snapshot_bytes_,
    String schema_graph_snapshot_bytes_,
    AuthorityInventory::Ptr inventory_,
    SchemaObjectDependencyGraph::Ptr schema_graph_)
    : checkpoint(std::move(checkpoint_))
    , inventory_snapshot_bytes(std::move(inventory_snapshot_bytes_))
    , schema_graph_snapshot_bytes(std::move(schema_graph_snapshot_bytes_))
    , inventory(std::move(inventory_))
    , schema_graph(std::move(schema_graph_))
{
}

Digest computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind kind, std::string_view canonical_bytes)
{
    static_cast<void>(encodeArtifactKind(kind));
    /// Permanent preimage: u16 WAL version, u8 discriminant 4, u8 artifact
    /// kind, minimal VarUInt payload size, then the exact payload bytes.
    String envelope;
    envelope.reserve(sizeof(UInt16) + 2 * sizeof(UInt8) + varUIntSize(canonical_bytes.size()) + canonical_bytes.size());
    envelope.push_back(static_cast<char>(database_schema_wal_format_version));
    envelope.push_back(static_cast<char>(database_schema_wal_format_version >> 8));
    envelope.push_back(static_cast<char>(staged_artifact_envelope_discriminant));
    envelope.push_back(static_cast<char>(encodeArtifactKind(kind)));
    UInt64 size = checkedSize(canonical_bytes.size(), "schema-WAL staged-artifact size does not fit UInt64");
    while (size >= 0x80)
    {
        envelope.push_back(static_cast<char>(static_cast<UInt8>(size) | 0x80));
        size >>= 7;
    }
    envelope.push_back(static_cast<char>(size));
    envelope.append(canonical_bytes);
    return hashFramedDomainSeparated(schema_wal_hash_domain, envelope);
}

Digest computeDatabaseSchemaWALExactRepairArtifactManifestDigest(
    std::span<const DatabaseSchemaWALStagedArtifact> artifacts, const DatabaseSchemaWALLimits & limits)
{
    validateLimits(limits);
    if (artifacts.empty())
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL exact-repair provenance manifest is empty");
    const UInt64 artifact_count = checkedSize(artifacts.size(), "schema-WAL exact-repair manifest count does not fit UInt64");
    const UInt64 manifest_control_bytes = checkedMultiply(
        artifact_count,
        checkedSize(sizeof(DatabaseSchemaWALStagedArtifactRef), "schema-WAL exact-repair manifest element size does not fit UInt64"),
        "schema-WAL exact-repair manifest control bytes overflow UInt64");
    if (manifest_control_bytes > limits.maximum_decode_control_bytes)
        fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL exact-repair manifest exceeds its control-memory limit");
    preflightStagedArtifactBudget(artifacts, limits);

    std::vector<DatabaseSchemaWALStagedArtifactRef> refs;
    refs.reserve(artifacts.size());
    for (const auto & artifact : artifacts)
    {
        if (artifact.image != DatabaseSchemaWALStagedArtifactImage::After
            || (artifact.kind != DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord
                && artifact.kind != DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord
                && artifact.kind != DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar))
        {
            fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL exact-repair manifest contains an unsupported artifact image");
        }
        refs.push_back({
            .kind = artifact.kind,
            .image = artifact.image,
            .object = artifact.object,
            .revision = artifact.revision,
            .byte_size = checkedSize(artifact.canonical_bytes.size(), "schema-WAL staged-artifact size does not fit UInt64"),
            .content_hash = computeDatabaseSchemaWALStagedArtifactHash(artifact.kind, artifact.canonical_bytes),
        });
    }
    std::sort(refs.begin(), refs.end(), artifactLess<DatabaseSchemaWALStagedArtifactRef>);
    validateStagedArtifactRefs(refs, refs.front().object.database_uuid, limits);

    Writer writer(prefixByteLimit(limits));
    writer.writeUInt16LE(database_schema_wal_format_version);
    writer.writeVarUInt(refs.size());
    for (const auto & ref : refs)
        writeArtifactRef(writer, ref);
    return hashFramedDomainSeparated(exact_repair_artifact_manifest_hash_domain, std::move(writer).release());
}

DatabaseSchemaWALStagedArtifactLocator makeDatabaseSchemaWALStagedArtifactLocator(UUID database_uuid, UInt64 transaction_id, UInt64 ordinal)
{
    if (database_uuid == UUIDHelpers::Nil || transaction_id == 0)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL staged-artifact locator identity is invalid");
    return {.database_uuid = database_uuid, .transaction_id = transaction_id, .ordinal = ordinal};
}

Digest computeDatabaseSchemaWALPrepareHash(const DatabaseSchemaWALPrepare & prepare, const DatabaseSchemaWALLimits & limits)
{
    return hashFramedDomainSeparated(schema_wal_hash_domain, encodePreparePrefix(prepare, limits));
}

Digest computeDatabaseSchemaWALCommitHash(const DatabaseSchemaWALCommit & commit, const DatabaseSchemaWALLimits & limits)
{
    return hashFramedDomainSeparated(schema_wal_hash_domain, encodeCommitPrefix(commit, limits));
}

Digest computeDatabaseSchemaWALCheckpointHash(const DatabaseSchemaWALCheckpoint & checkpoint, const DatabaseSchemaWALLimits & limits)
{
    return hashFramedDomainSeparated(schema_wal_hash_domain, encodeCheckpointPrefix(checkpoint, limits));
}

DatabaseSchemaWALValidatedTransition DatabaseSchemaWALTransitionBuilder::buildDependentObjectActivation(
    UInt64 transaction_id,
    const AuthorityRoot & definition_only_root,
    const DatabaseSchemaWALLimits & limits,
    DatabaseSchemaWALDependentObjectActivationStatistics * statistics)
{
    if (statistics)
        *statistics = {};
    validateLimits(limits);

    const AuthorityState & before = definition_only_root.getAuthorityState();
    AuthorityState after;
    try
    {
        after = activateDependentObjectAuthority(before, limits.authority_state);
    }
    catch (const AuthorityStateError & error)
    {
        if (error.code == AuthorityStateError::Code::LimitExceeded)
            fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL dependent-object-capable activation state exceeds its limit");
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL dependent-object-capable activation base is invalid");
    }

    auto inventory = definition_only_root.pinAuthorityInventory();
    auto graph = definition_only_root.pinSchemaObjectDependencyGraph();
    if (!inventory || !graph || !sameInventorySummary(inventory->getSummary(), before) || graph->getDatabaseUUID() != before.database_uuid)
    {
        fail(
            DatabaseSchemaWALError::Code::TransitionMismatch,
            "schema-WAL dependent-object-capable activation root is internally inconsistent");
    }

    validatePinnedInventoryLimits(inventory, limits.inventory_snapshot.inventory);
    validatePinnedGraphLimits(graph, limits.schema_graph);

    DatabaseSchemaWALPrepare prepare{
        .format_version = database_schema_wal_format_version,
        .transaction_id = transaction_id,
        .exact_repair = false,
        .exact_repair_provenance = std::nullopt,
        .before_authority_state = before,
        .after_authority_state = std::move(after),
        .authority_record_deltas = {},
        .dependent_object_deltas = {},
        .graph_delta = {},
        .staged_artifacts = {},
        .prepare_hash = {},
    };
    canonicalizePrepare(prepare);
    validatePrepareFields(prepare, limits);
    validateCapabilityUse(prepare);
    validateDependentRevisions(prepare);
    prepare.prepare_hash = computeDatabaseSchemaWALPrepareHash(prepare, limits);
    return DatabaseSchemaWALValidatedTransition(std::move(prepare), std::move(inventory), std::move(graph), {});
}

DatabaseSchemaWALValidatedTransition DatabaseSchemaWALTransitionBuilder::buildPhysicalization(
    UInt64 transaction_id,
    const AuthorityRoot & before_root,
    const AuthorityRoot & after_root,
    std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_record_deltas,
    std::vector<DatabaseSchemaWALDependentObjectDelta> dependent_object_deltas,
    SchemaObjectDependencyGraphMutation graph_delta,
    std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts,
    const DatabaseSchemaWALLimits & limits)
{
    validateLimits(limits);
    preflightStagedArtifactBudget(staged_artifacts, limits);
    std::sort(staged_artifacts.begin(), staged_artifacts.end(), artifactLess<DatabaseSchemaWALStagedArtifact>);
    std::vector<DatabaseSchemaWALStagedArtifactRef> artifact_refs;
    std::vector<String> artifact_bytes;
    artifact_refs.reserve(staged_artifacts.size());
    artifact_bytes.reserve(staged_artifacts.size());
    for (auto & artifact : staged_artifacts)
    {
        const UInt64 byte_size = checkedSize(artifact.canonical_bytes.size(), "schema-WAL staged-artifact size does not fit UInt64");
        artifact_refs.push_back({
            .kind = artifact.kind,
            .image = artifact.image,
            .object = artifact.object,
            .revision = artifact.revision,
            .byte_size = byte_size,
            .content_hash = computeDatabaseSchemaWALStagedArtifactHash(artifact.kind, artifact.canonical_bytes),
        });
        artifact_bytes.push_back(std::move(artifact.canonical_bytes));
    }

    DatabaseSchemaWALPrepare prepare{
        .format_version = database_schema_wal_format_version,
        .transaction_id = transaction_id,
        .exact_repair = false,
        .exact_repair_provenance = std::nullopt,
        .before_authority_state = before_root.getAuthorityState(),
        .after_authority_state = after_root.getAuthorityState(),
        .authority_record_deltas = std::move(authority_record_deltas),
        .dependent_object_deltas = std::move(dependent_object_deltas),
        .graph_delta = std::move(graph_delta),
        .staged_artifacts = std::move(artifact_refs),
    };
    canonicalizePrepare(prepare);
    validatePrepareFields(prepare, limits);
    validateCapabilityUse(prepare);
    validateDependentRevisions(prepare);

    const DatabaseSchemaWALTransitionBase base{
        .authority_state = before_root.getAuthorityState(),
        .authority_inventory = before_root.pinAuthorityInventory(),
        .schema_graph = before_root.pinSchemaObjectDependencyGraph(),
    };
    validateBaseState(base, before_root.getDatabaseUUID(), limits);
    const auto & before = before_root.getAuthorityState();
    const auto & after = after_root.getAuthorityState();
    if (!isDependentObjectCapableAuthority(before.persistent_capability_mask)
        || !isDependentObjectCapableAuthority(after.persistent_capability_mask) || before.database_uuid != after.database_uuid
        || before.database_catalog_epoch == std::numeric_limits<UInt64>::max()
        || after.database_catalog_epoch != before.database_catalog_epoch + 1)
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL physicalization authority-state transition is invalid");

    auto after_inventory = after_root.pinAuthorityInventory();
    auto after_graph = after_root.pinSchemaObjectDependencyGraph();
    if (!after_inventory || !after_graph)
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL physicalization after-root pins are missing");
    validatePinnedInventoryLimits(after_inventory, limits.inventory_snapshot.inventory);
    validatePinnedGraphLimits(after_graph, limits.schema_graph);
    if (!sameInventorySummary(after_inventory->getSummary(), after) || after_graph->getDatabaseUUID() != after.database_uuid
        || after_graph->computeRoot() != after.schema_graph_root)
        fail(
            DatabaseSchemaWALError::Code::TransitionMismatch,
            "schema-WAL physicalization after-root pins do not match its authority state");

    std::vector<AuthorityInventoryKey> removal_keys;
    removal_keys.reserve(prepare.authority_record_deltas.size());
    for (const auto & delta : prepare.authority_record_deltas)
    {
        validateReplacementRevision(delta);
        if (!delta.before || delta.after)
            fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL physicalization authority delta is not a removal");
        const auto * before_leaf = base.authority_inventory->find(delta.key);
        const auto * after_leaf = after_inventory->find(delta.key);
        if (!before_leaf || before_leaf->object_revision != delta.before->object_revision
            || before_leaf->canonical_record_hash != delta.before->canonical_record_hash || after_leaf)
            fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL physicalization authority removal image is inconsistent");
        removal_keys.push_back(delta.key);
    }
    if (after_inventory->getSummary().leaf_count + removal_keys.size() != base.authority_inventory->getSummary().leaf_count)
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL physicalization authority removal count is inconsistent");
    if (!after_root.provesPhysicalizationDeltaFrom(before_root, removal_keys, prepare.graph_delta))
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL physicalization after-root lacks the exact delta proof");

    validateTableDependentGraphMembership(prepare, *base.schema_graph, *after_graph);
    validateArtifacts(prepare, artifact_bytes, limits);
    prepare.prepare_hash = computeDatabaseSchemaWALPrepareHash(prepare, limits);
    return DatabaseSchemaWALValidatedTransition(
        std::move(prepare), std::move(after_inventory), std::move(after_graph), std::move(artifact_bytes));
}

DatabaseSchemaWALValidatedTransition DatabaseSchemaWALTransitionBuilder::buildExactRepair(
    UInt64 transaction_id,
    const AuthorityRoot & before_root,
    const AuthorityRoot & after_root,
    std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts,
    const DatabaseSchemaWALLimits & limits,
    std::optional<DatabaseSchemaWALExactRepairProvenance> provenance)
{
    if (!before_root.sharesContentPayloadWith(after_root))
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL exact repair roots do not share their immutable content");

    const DatabaseSchemaWALTransitionBase base{
        .authority_state = before_root.getAuthorityState(),
        .authority_inventory = before_root.pinAuthorityInventory(),
        .schema_graph = before_root.pinSchemaObjectDependencyGraph(),
    };
    auto result = buildExactRepair(
        transaction_id, base, after_root.getAuthorityState(), std::move(staged_artifacts), limits, std::move(provenance));
    if (result.pinAfterInventory() != after_root.pinAuthorityInventory()
        || result.pinAfterGraph() != after_root.pinSchemaObjectDependencyGraph())
    {
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL exact repair did not retain the exact immutable content pins");
    }
    return result;
}

DatabaseSchemaWALValidatedTransition DatabaseSchemaWALTransitionBuilder::buildExactRepair(
    UInt64 transaction_id,
    const DatabaseSchemaWALTransitionBase & before,
    AuthorityState after_authority_state,
    std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts,
    const DatabaseSchemaWALLimits & limits,
    std::optional<DatabaseSchemaWALExactRepairProvenance> provenance)
{
    validateLimits(limits);
    preflightStagedArtifactBudget(staged_artifacts, limits);
    if (!before.authority_state || !before.authority_inventory || !before.schema_graph)
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL startup exact repair has no complete anchored base");
    if (!provenance)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "a new schema-WAL exact repair requires bounded durable provenance");

    std::sort(staged_artifacts.begin(), staged_artifacts.end(), artifactLess<DatabaseSchemaWALStagedArtifact>);
    std::vector<DatabaseSchemaWALStagedArtifactRef> artifact_refs;
    std::vector<String> artifact_bytes;
    artifact_refs.reserve(staged_artifacts.size());
    artifact_bytes.reserve(staged_artifacts.size());
    for (auto & artifact : staged_artifacts)
    {
        const UInt64 byte_size = checkedSize(artifact.canonical_bytes.size(), "schema-WAL staged-artifact size does not fit UInt64");
        artifact_refs.push_back({
            .kind = artifact.kind,
            .image = artifact.image,
            .object = artifact.object,
            .revision = artifact.revision,
            .byte_size = byte_size,
            .content_hash = computeDatabaseSchemaWALStagedArtifactHash(artifact.kind, artifact.canonical_bytes),
        });
        artifact_bytes.push_back(std::move(artifact.canonical_bytes));
    }

    DatabaseSchemaWALPrepare prepare{
        .format_version = database_schema_wal_format_version,
        .transaction_id = transaction_id,
        .exact_repair = true,
        .exact_repair_provenance = std::move(provenance),
        .before_authority_state = before.authority_state,
        .after_authority_state = std::move(after_authority_state),
        .authority_record_deltas = {},
        .dependent_object_deltas = {},
        .graph_delta = {},
        .staged_artifacts = std::move(artifact_refs),
        .prepare_hash = {},
    };
    canonicalizePrepare(prepare);
    auto validated = validateTransition(prepare, before, artifact_bytes, limits);
    prepare.prepare_hash = computeDatabaseSchemaWALPrepareHash(prepare, limits);
    return DatabaseSchemaWALValidatedTransition(
        std::move(prepare), std::move(validated.inventory), std::move(validated.graph), std::move(artifact_bytes));
}

DatabaseSchemaWALValidatedTransition DatabaseSchemaWALTransitionBuilder::build(
    UInt64 transaction_id,
    const DatabaseSchemaWALTransitionBase & base,
    AuthorityState after_authority_state,
    std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_record_deltas,
    std::vector<DatabaseSchemaWALDependentObjectDelta> dependent_object_deltas,
    SchemaObjectDependencyGraphMutation graph_delta,
    std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts,
    const DatabaseSchemaWALLimits & limits)
{
    validateLimits(limits);
    preflightStagedArtifactBudget(staged_artifacts, limits);
    std::sort(staged_artifacts.begin(), staged_artifacts.end(), artifactLess<DatabaseSchemaWALStagedArtifact>);
    std::vector<DatabaseSchemaWALStagedArtifactRef> artifact_refs;
    std::vector<String> artifact_bytes;
    artifact_refs.reserve(staged_artifacts.size());
    artifact_bytes.reserve(staged_artifacts.size());
    for (auto & artifact : staged_artifacts)
    {
        const UInt64 byte_size = checkedSize(artifact.canonical_bytes.size(), "schema-WAL staged-artifact size does not fit UInt64");
        artifact_refs.push_back(
            DatabaseSchemaWALStagedArtifactRef{
                .kind = artifact.kind,
                .image = artifact.image,
                .object = artifact.object,
                .revision = artifact.revision,
                .byte_size = byte_size,
                .content_hash = computeDatabaseSchemaWALStagedArtifactHash(artifact.kind, artifact.canonical_bytes),
            });
        artifact_bytes.push_back(std::move(artifact.canonical_bytes));
    }

    DatabaseSchemaWALPrepare prepare{
        .format_version = database_schema_wal_format_version,
        .transaction_id = transaction_id,
        .exact_repair = false,
        .exact_repair_provenance = std::nullopt,
        .before_authority_state = base.authority_state,
        .after_authority_state = std::move(after_authority_state),
        .authority_record_deltas = std::move(authority_record_deltas),
        .dependent_object_deltas = std::move(dependent_object_deltas),
        .graph_delta = std::move(graph_delta),
        .staged_artifacts = std::move(artifact_refs),
    };
    canonicalizePrepare(prepare);
    auto result = validateTransition(prepare, base, artifact_bytes, limits);
    prepare.prepare_hash = computeDatabaseSchemaWALPrepareHash(prepare, limits);
    return DatabaseSchemaWALValidatedTransition(
        std::move(prepare), std::move(result.inventory), std::move(result.graph), std::move(artifact_bytes));
}

DatabaseSchemaWALValidatedTransition DatabaseSchemaWALTransitionBuilder::validateDecoded(
    DatabaseSchemaWALPrepare prepare,
    const DatabaseSchemaWALTransitionBase & base,
    std::vector<String> staged_artifact_bytes,
    const DatabaseSchemaWALLimits & limits)
{
    if (prepare.prepare_hash != computeDatabaseSchemaWALPrepareHash(prepare, limits))
        fail(DatabaseSchemaWALError::Code::DigestMismatch, "schema-WAL prepare hash does not match its fields");
    auto result = validateTransition(prepare, base, staged_artifact_bytes, limits);
    return DatabaseSchemaWALValidatedTransition(
        std::move(prepare), std::move(result.inventory), std::move(result.graph), std::move(staged_artifact_bytes));
}

DatabaseSchemaWALCommit
makeDatabaseSchemaWALCommit(const DatabaseSchemaWALValidatedTransition & transition, const DatabaseSchemaWALLimits & limits)
{
    const auto & prepare = transition.getPrepare();
    if (prepare.prepare_hash != computeDatabaseSchemaWALPrepareHash(prepare, limits))
        fail(DatabaseSchemaWALError::Code::DigestMismatch, "schema-WAL validated prepare hash does not match its fields");
    const auto & after = prepare.after_authority_state;
    DatabaseSchemaWALCommit result{
        .format_version = database_schema_wal_format_version,
        .transaction_id = prepare.transaction_id,
        .database_uuid = after.database_uuid,
        .database_catalog_epoch = after.database_catalog_epoch,
        .inventory_root = after.inventory_root,
        .schema_graph_root = after.schema_graph_root,
        .authority_anchor = after.anchor_hash,
        .prepare_hash = prepare.prepare_hash,
    };
    result.commit_hash = computeDatabaseSchemaWALCommitHash(result, limits);
    return result;
}

String encodeDatabaseSchemaWALPrepare(const DatabaseSchemaWALPrepare & prepare, const DatabaseSchemaWALLimits & limits)
{
    String result = encodePreparePrefix(prepare, limits);
    if (prepare.prepare_hash != hashFramedDomainSeparated(schema_wal_hash_domain, result))
        fail(DatabaseSchemaWALError::Code::DigestMismatch, "schema-WAL prepare hash does not match its fields");
    result.append(reinterpret_cast<const char *>(prepare.prepare_hash.data()), prepare.prepare_hash.size());
    return result;
}

String encodeDatabaseSchemaWALCommit(const DatabaseSchemaWALCommit & commit, const DatabaseSchemaWALLimits & limits)
{
    String result = encodeCommitPrefix(commit, limits);
    if (commit.commit_hash != hashFramedDomainSeparated(schema_wal_hash_domain, result))
        fail(DatabaseSchemaWALError::Code::DigestMismatch, "schema-WAL commit hash does not match its fields");
    result.append(reinterpret_cast<const char *>(commit.commit_hash.data()), commit.commit_hash.size());
    return result;
}

String encodeDatabaseSchemaWALCheckpoint(const DatabaseSchemaWALCheckpoint & checkpoint, const DatabaseSchemaWALLimits & limits)
{
    String result = encodeCheckpointPrefix(checkpoint, limits);
    if (checkpoint.checkpoint_hash != hashFramedDomainSeparated(schema_wal_hash_domain, result))
        fail(DatabaseSchemaWALError::Code::DigestMismatch, "schema-WAL checkpoint hash does not match its fields");
    result.append(reinterpret_cast<const char *>(checkpoint.checkpoint_hash.data()), checkpoint.checkpoint_hash.size());
    return result;
}

UInt64 getDatabaseSchemaWALPrepareDecodedControlBytes(const DatabaseSchemaWALPrepare & prepare)
{
    UInt64 result = sizeof(DatabaseSchemaWALPrepare);
    const auto add_vector = [&](size_t count, size_t element_size)
    {
        const UInt64 bytes = checkedMultiply(
            checkedSize(count, "schema-WAL Prepare vector size does not fit UInt64"),
            checkedSize(element_size, "schema-WAL Prepare element size does not fit UInt64"),
            "schema-WAL Prepare decoded control byte count overflows UInt64");
        result = checkedAdd(result, bytes, "schema-WAL Prepare decoded control byte count overflows UInt64");
    };
    add_vector(prepare.authority_record_deltas.size(), sizeof(DatabaseSchemaWALAuthorityRecordDelta));
    add_vector(prepare.dependent_object_deltas.size(), sizeof(DatabaseSchemaWALDependentObjectDelta));
    add_vector(prepare.graph_delta.node_additions.size(), sizeof(SchemaObjectID));
    add_vector(prepare.graph_delta.node_removals.size(), sizeof(SchemaObjectID));
    add_vector(prepare.graph_delta.edge_additions.size(), sizeof(SchemaObjectDependencyEdge));
    add_vector(prepare.graph_delta.edge_removals.size(), sizeof(SchemaObjectDependencyEdge));
    add_vector(prepare.staged_artifacts.size(), sizeof(DatabaseSchemaWALStagedArtifactRef));
    return result;
}

DatabaseSchemaWALPrepare decodeDatabaseSchemaWALPrepare(std::string_view bytes, const DatabaseSchemaWALLimits & limits)
{
    validateLimits(limits);
    Reader reader(bytes, limits.maximum_encoded_bytes);
    readAndValidateHeader(reader, DatabaseSchemaWALRecordKind::Prepare);
    DatabaseSchemaWALPrepare result;
    UInt64 decoded_control_bytes = sizeof(DatabaseSchemaWALPrepare) + prepare_decode_transient_control_bytes;
    if (decoded_control_bytes > limits.maximum_decode_control_bytes)
        fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL Prepare exceeds its decoded control byte limit");
    const auto charge_control = [&](UInt64 count, size_t element_size)
    {
        const UInt64 bytes_to_add = checkedMultiply(
            count,
            checkedSize(element_size, "schema-WAL Prepare element size does not fit UInt64"),
            "schema-WAL Prepare decoded control byte count overflows UInt64");
        decoded_control_bytes
            = checkedAdd(decoded_control_bytes, bytes_to_add, "schema-WAL Prepare decoded control byte count overflows UInt64");
        if (decoded_control_bytes > limits.maximum_decode_control_bytes)
            fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL Prepare exceeds its decoded control byte limit");
    };
    result.transaction_id = reader.readUInt64LE();
    const UInt8 before_present = reader.readByte();
    if (before_present > 1)
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL prior-authority presence tag is invalid");
    if (before_present)
        result.before_authority_state = readAuthorityStateFrame(reader, limits);
    result.after_authority_state = readAuthorityStateFrame(reader, limits);

    const UInt64 authority_count = reader.readCount(limits.maximum_authority_record_deltas, authority_delta_minimum_bytes);
    charge_control(authority_count, sizeof(DatabaseSchemaWALAuthorityRecordDelta));
    result.authority_record_deltas.reserve(authority_count);
    for (UInt64 index = 0; index < authority_count; ++index)
    {
        result.authority_record_deltas.push_back(
            DatabaseSchemaWALAuthorityRecordDelta{
                .key = readAuthorityKey(reader),
                .before = readAuthorityRecordState(reader),
                .after = readAuthorityRecordState(reader),
            });
    }

    const UInt64 dependent_count = reader.readCount(limits.maximum_dependent_object_deltas, dependent_object_delta_minimum_bytes);
    charge_control(dependent_count, sizeof(DatabaseSchemaWALDependentObjectDelta));
    result.dependent_object_deltas.reserve(dependent_count);
    for (UInt64 index = 0; index < dependent_count; ++index)
    {
        result.dependent_object_deltas.push_back(
            DatabaseSchemaWALDependentObjectDelta{
                .object = readSchemaObjectID(reader),
                .before = readDependentObjectState(reader),
                .after = readDependentObjectState(reader),
            });
    }

    const auto read_nodes = [&reader, &charge_control](UInt64 maximum)
    {
        const UInt64 count = reader.readCount(maximum, schema_object_id_bytes);
        charge_control(count, sizeof(SchemaObjectID));
        std::vector<SchemaObjectID> nodes;
        nodes.reserve(count);
        for (UInt64 index = 0; index < count; ++index)
            nodes.push_back(readSchemaObjectID(reader));
        return nodes;
    };
    const auto read_edges = [&reader, &charge_control](UInt64 maximum)
    {
        const UInt64 count = reader.readCount(maximum, dependency_edge_bytes);
        charge_control(count, sizeof(SchemaObjectDependencyEdge));
        std::vector<SchemaObjectDependencyEdge> edges;
        edges.reserve(count);
        for (UInt64 index = 0; index < count; ++index)
            edges.push_back(readGraphEdge(reader));
        return edges;
    };
    result.graph_delta.node_additions = read_nodes(limits.maximum_graph_node_deltas);
    result.graph_delta.node_removals = read_nodes(limits.maximum_graph_node_deltas);
    result.graph_delta.edge_additions = read_edges(limits.maximum_graph_edge_deltas);
    result.graph_delta.edge_removals = read_edges(limits.maximum_graph_edge_deltas);

    const UInt64 artifact_count = reader.readCount(limits.maximum_staged_artifacts, staged_artifact_ref_minimum_bytes);
    charge_control(artifact_count, sizeof(DatabaseSchemaWALStagedArtifactRef));
    result.staged_artifacts.reserve(artifact_count);
    for (UInt64 index = 0; index < artifact_count; ++index)
        result.staged_artifacts.push_back(readArtifactRef(reader, limits));
    const UInt16 extension = readExtension(reader, supported_prepare_extension_flags);
    result.exact_repair = (extension & prepare_exact_repair_extension_flag) != 0;
    if ((extension & prepare_exact_repair_provenance_extension_flag) != 0)
        result.exact_repair_provenance = readExactRepairProvenance(reader);
    result.prepare_hash = reader.readArray<sizeof(Digest)>();
    reader.requireEnd();
    validatePrepareFields(result, limits);
    if (decoded_control_bytes
        != checkedAdd(
            getDatabaseSchemaWALPrepareDecodedControlBytes(result),
            prepare_decode_transient_control_bytes,
            "schema-WAL Prepare decoded control byte count overflows UInt64"))
        fail(DatabaseSchemaWALError::Code::InvalidValue, "schema-WAL Prepare decoded control accounting is inconsistent");
    const auto canonical_prefix = bytes.substr(0, bytes.size() - sizeof(Digest));
    if (result.prepare_hash != hashFramedDomainSeparated(schema_wal_hash_domain, canonical_prefix))
        fail(DatabaseSchemaWALError::Code::DigestMismatch, "schema-WAL prepare hash does not match its fields");
    return result;
}

DatabaseSchemaWALCommit decodeDatabaseSchemaWALCommit(std::string_view bytes, const DatabaseSchemaWALLimits & limits)
{
    validateLimits(limits);
    Reader reader(bytes, limits.maximum_encoded_bytes);
    readAndValidateHeader(reader, DatabaseSchemaWALRecordKind::Commit);
    DatabaseSchemaWALCommit result;
    result.transaction_id = reader.readUInt64LE();
    result.database_uuid = reader.readUUID();
    result.database_catalog_epoch = reader.readUInt64LE();
    result.inventory_root = reader.readArray<sizeof(Digest)>();
    result.schema_graph_root = reader.readArray<sizeof(Digest)>();
    result.authority_anchor = reader.readArray<sizeof(Digest)>();
    result.prepare_hash = reader.readArray<sizeof(Digest)>();
    static_cast<void>(readExtension(reader));
    result.commit_hash = reader.readArray<sizeof(Digest)>();
    reader.requireEnd();
    validateCommitFields(result, limits);
    const auto canonical_prefix = bytes.substr(0, bytes.size() - sizeof(Digest));
    if (result.commit_hash != hashFramedDomainSeparated(schema_wal_hash_domain, canonical_prefix))
        fail(DatabaseSchemaWALError::Code::DigestMismatch, "schema-WAL commit hash does not match its fields");
    return result;
}

DatabaseSchemaWALCheckpoint decodeDatabaseSchemaWALCheckpoint(std::string_view bytes, const DatabaseSchemaWALLimits & limits)
{
    validateLimits(limits);
    Reader reader(bytes, limits.maximum_encoded_bytes);
    readAndValidateHeader(reader, DatabaseSchemaWALRecordKind::Checkpoint);
    DatabaseSchemaWALCheckpoint result;
    result.checkpoint_id = reader.readUInt64LE();
    result.covered_commit = decodeDatabaseSchemaWALCommit(reader.readFrame(limits.maximum_encoded_bytes), limits);
    result.authority_state = readAuthorityStateFrame(reader, limits);
    result.inventory_snapshot_hash = reader.readArray<sizeof(Digest)>();
    result.schema_graph_snapshot_hash = reader.readArray<sizeof(Digest)>();
    const UInt16 extension = readExtension(reader, supported_checkpoint_extension_flags);
    if ((extension & checkpoint_exact_repair_provenance_extension_flag) != 0)
        result.last_exact_repair_provenance = readExactRepairProvenance(reader);
    result.checkpoint_hash = reader.readArray<sizeof(Digest)>();
    reader.requireEnd();
    validateCheckpointFields(result, limits);
    if (result.checkpoint_hash != computeDatabaseSchemaWALCheckpointHash(result, limits))
        fail(DatabaseSchemaWALError::Code::DigestMismatch, "schema-WAL checkpoint hash does not match its fields");
    return result;
}

void validateDatabaseSchemaWALCommit(
    const DatabaseSchemaWALValidatedTransition & transition, const DatabaseSchemaWALCommit & commit, const DatabaseSchemaWALLimits & limits)
{
    const auto expected = makeDatabaseSchemaWALCommit(transition, limits);
    if (commit.commit_hash != computeDatabaseSchemaWALCommitHash(commit, limits))
        fail(DatabaseSchemaWALError::Code::DigestMismatch, "schema-WAL commit hash does not match its fields");
    if (commit != expected)
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL commit does not match its validated transition");
}

DatabaseSchemaWALRecoveryDecision decideDatabaseSchemaWALRecovery(
    const DatabaseSchemaWALValidatedTransition & transition,
    const std::optional<DatabaseSchemaWALCommit> & commit,
    const DatabaseSchemaWALLimits & limits)
{
    if (!commit)
        return DatabaseSchemaWALRecoveryDecision::RollBackPrepared;
    validateDatabaseSchemaWALCommit(transition, *commit, limits);
    return DatabaseSchemaWALRecoveryDecision::CompleteCommitted;
}

DatabaseSchemaWALValidatedCheckpoint DatabaseSchemaWALCheckpointBuilder::build(
    UInt64 checkpoint_id,
    DatabaseSchemaWALCommit covered_commit,
    AuthorityState authority_state,
    AuthorityInventory::Ptr authority_inventory,
    SchemaObjectDependencyGraph::Ptr schema_graph,
    const DatabaseSchemaWALLimits & limits,
    std::optional<DatabaseSchemaWALExactRepairProvenance> last_exact_repair_provenance)
{
    validateLimits(limits);
    if (!authority_inventory || !schema_graph)
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL checkpoint snapshots are missing");
    if (covered_commit.commit_hash != computeDatabaseSchemaWALCommitHash(covered_commit, limits))
        fail(DatabaseSchemaWALError::Code::DigestMismatch, "schema-WAL covered commit hash does not match its fields");
    validateAuthorityState(authority_state, limits);
    validateCommitAgainstState(covered_commit, authority_state);
    if (!sameInventorySummary(authority_inventory->getSummary(), authority_state)
        || schema_graph->getDatabaseUUID() != authority_state.database_uuid
        || schema_graph->computeRoot() != authority_state.schema_graph_root)
    {
        fail(DatabaseSchemaWALError::Code::TransitionMismatch, "schema-WAL checkpoint actual snapshots do not match the authority state");
    }

    AuthorityInventorySnapshot inventory_snapshot;
    String inventory_bytes;
    try
    {
        inventory_snapshot = makeAuthorityInventorySnapshot(
            authority_state.database_uuid,
            std::vector<AuthorityInventoryLeaf>(authority_inventory->getLeaves().begin(), authority_inventory->getLeaves().end()),
            limits.inventory_snapshot);
        inventory_bytes = encodeAuthorityInventorySnapshot(inventory_snapshot, limits.inventory_snapshot);
    }
    catch (const AuthorityInventorySnapshotError &)
    {
        fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL inventory checkpoint snapshot is invalid");
    }
    String graph_bytes;
    try
    {
        graph_bytes = schema_graph->encodeSnapshot();
    }
    catch (const SchemaObjectDependencyGraphError &)
    {
        fail(DatabaseSchemaWALError::Code::LimitExceeded, "schema-WAL graph checkpoint snapshot is invalid");
    }
    DatabaseSchemaWALCheckpoint checkpoint{
        .format_version = database_schema_wal_format_version,
        .checkpoint_id = checkpoint_id,
        .covered_commit = std::move(covered_commit),
        .authority_state = std::move(authority_state),
        .inventory_snapshot_hash = computeAuthorityInventorySnapshotHash(inventory_snapshot, limits.inventory_snapshot),
        .schema_graph_snapshot_hash = schema_graph->computeRoot(),
        .last_exact_repair_provenance = std::move(last_exact_repair_provenance),
    };
    checkpoint.checkpoint_hash = computeDatabaseSchemaWALCheckpointHash(checkpoint, limits);
    return DatabaseSchemaWALValidatedCheckpoint(
        std::move(checkpoint), std::move(inventory_bytes), std::move(graph_bytes), std::move(authority_inventory), std::move(schema_graph));
}

DatabaseSchemaWALValidatedCheckpoint DatabaseSchemaWALCheckpointBuilder::validateDecoded(
    DatabaseSchemaWALCheckpoint checkpoint,
    std::string_view inventory_snapshot_bytes,
    std::string_view schema_graph_snapshot_bytes,
    const DatabaseSchemaWALLimits & limits)
{
    if (checkpoint.checkpoint_hash != computeDatabaseSchemaWALCheckpointHash(checkpoint, limits))
        fail(DatabaseSchemaWALError::Code::DigestMismatch, "schema-WAL checkpoint hash does not match its fields");
    AuthorityInventorySnapshot inventory_snapshot;
    AuthorityInventory::Ptr inventory;
    SchemaObjectDependencyGraph::Ptr graph;
    try
    {
        inventory_snapshot = decodeAuthorityInventorySnapshot(inventory_snapshot_bytes, limits.inventory_snapshot);
        inventory = buildAuthorityInventoryFromSnapshot(inventory_snapshot, limits.inventory_snapshot);
    }
    catch (const AuthorityInventorySnapshotError &)
    {
        fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL inventory checkpoint snapshot is invalid");
    }
    try
    {
        graph = SchemaObjectDependencyGraph::decodeSnapshot(schema_graph_snapshot_bytes, limits.schema_graph);
    }
    catch (const SchemaObjectDependencyGraphError &)
    {
        fail(DatabaseSchemaWALError::Code::ArtifactMismatch, "schema-WAL graph checkpoint snapshot is invalid");
    }
    if (computeAuthorityInventorySnapshotHash(inventory_snapshot, limits.inventory_snapshot) != checkpoint.inventory_snapshot_hash
        || graph->computeRoot() != checkpoint.schema_graph_snapshot_hash)
    {
        fail(DatabaseSchemaWALError::Code::DigestMismatch, "schema-WAL checkpoint snapshot hash does not match");
    }
    validateCheckpointSnapshots(checkpoint, inventory_snapshot, inventory, graph);
    return DatabaseSchemaWALValidatedCheckpoint(
        std::move(checkpoint),
        String(inventory_snapshot_bytes),
        String(schema_graph_snapshot_bytes),
        std::move(inventory),
        std::move(graph));
}

}
