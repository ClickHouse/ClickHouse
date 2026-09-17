#include <Databases/UDT/SyntheticObjectMetadata.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/PhysicalTypeFingerprint.h>

#include <Common/Exception.h>

#include <algorithm>
#include <array>
#include <limits>
#include <span>
#include <utility>

namespace DB::UDT
{
namespace
{

using Error = SyntheticObjectMetadataError;

constexpr UInt64 implementation_maximum_metadata_bytes = 16ULL << 20;
constexpr UInt64 implementation_maximum_occurrences = 65'536;
constexpr UInt64 implementation_maximum_path_depth = 64;
constexpr UInt64 implementation_maximum_retained_path_components = 4ULL << 20;
constexpr UInt64 implementation_maximum_diagnostic_name_bytes = 4ULL << 10;
constexpr UInt64 implementation_maximum_canonical_physical_type_bytes = 64ULL << 10;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

UInt64 checkedSize(size_t value, std::string_view message)
{
    if (!std::in_range<UInt64>(value))
        fail(Error::Code::LimitExceeded, message);
    return static_cast<UInt64>(value);
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

void checkedCharge(UInt64 & value, UInt64 additional, UInt64 maximum, std::string_view message)
{
    if (value > maximum || additional > maximum - value)
        fail(Error::Code::LimitExceeded, message);
    value += additional;
}

bool containsZero(std::string_view value) noexcept
{
    return value.find('\0') != std::string_view::npos;
}

void validateLimits(const SyntheticObjectMetadataLimits & limits)
{
    if (!limits.maximum_metadata_bytes || limits.maximum_metadata_bytes > implementation_maximum_metadata_bytes
        || !limits.maximum_occurrences || limits.maximum_occurrences > implementation_maximum_occurrences
        || limits.maximum_path_depth > implementation_maximum_path_depth || !limits.maximum_retained_path_components
        || limits.maximum_retained_path_components > implementation_maximum_retained_path_components
        || !limits.maximum_diagnostic_name_bytes || limits.maximum_diagnostic_name_bytes > implementation_maximum_diagnostic_name_bytes
        || !limits.maximum_canonical_physical_type_bytes
        || limits.maximum_canonical_physical_type_bytes > implementation_maximum_canonical_physical_type_bytes)
        fail(Error::Code::InvalidConfiguration, "synthetic user-defined type metadata limits are invalid");
}

class Writer final
{
public:
    explicit Writer(UInt64 maximum_bytes_)
        : maximum_bytes(maximum_bytes_)
    {
    }

    void byte(UInt8 value)
    {
        reserve(1);
        output.push_back(static_cast<char>(value));
    }

    void uint16(UInt16 value)
    {
        byte(static_cast<UInt8>(value));
        byte(static_cast<UInt8>(value >> 8));
    }

    void uint64(UInt64 value)
    {
        for (size_t index = 0; index < sizeof(value); ++index)
            byte(static_cast<UInt8>(value >> (8 * index)));
    }

    void varUInt(UInt64 value)
    {
        do
        {
            UInt8 current = static_cast<UInt8>(value & 0x7f);
            value >>= 7;
            if (value)
                current = static_cast<UInt8>(current | 0x80);
            byte(current);
        } while (value);
    }

    void bytes(std::string_view value)
    {
        reserve(checkedSize(value.size(), "synthetic metadata bytes exceed UInt64"));
        output.append(value);
    }

    template <size_t size>
    void bytes(const std::array<CanonicalByte, size> & value)
    {
        bytes({reinterpret_cast<const char *>(value.data()), value.size()});
    }

    void frame(std::string_view value)
    {
        varUInt(checkedSize(value.size(), "synthetic metadata frame exceeds UInt64"));
        bytes(value);
    }

    void uuid(const UUID & value) { bytes(uuidToCanonicalBytes(value)); }
    void digest(const Digest & value) { bytes(value); }

    String finish() && { return std::move(output); }

private:
    void reserve(UInt64 additional)
    {
        const UInt64 current = checkedSize(output.size(), "synthetic metadata output exceeds UInt64");
        if (current > maximum_bytes || additional > maximum_bytes - current)
            fail(Error::Code::LimitExceeded, "synthetic metadata exceeds its byte limit");
    }

    const UInt64 maximum_bytes;
    String output;
};

class Reader final
{
public:
    explicit Reader(std::string_view input_)
        : input(input_)
    {
    }

    size_t remaining() const noexcept { return input.size() - position; }

    UInt8 byte()
    {
        require(1);
        return static_cast<UInt8>(input[position++]);
    }

    UInt16 uint16()
    {
        require(sizeof(UInt16));
        const UInt16 result = static_cast<UInt16>(
            static_cast<UInt8>(input[position]) | (static_cast<UInt16>(static_cast<UInt8>(input[position + 1])) << 8));
        position += sizeof(UInt16);
        return result;
    }

    UInt64 uint64()
    {
        require(sizeof(UInt64));
        UInt64 result = 0;
        for (size_t index = 0; index < sizeof(UInt64); ++index)
            result |= static_cast<UInt64>(static_cast<UInt8>(input[position + index])) << (8 * index);
        position += sizeof(UInt64);
        return result;
    }

    UInt64 minimalVarUInt(UInt64 maximum, std::string_view message)
    {
        UInt64 result = 0;
        UInt8 shift = 0;
        UInt64 encoded_size = 0;
        while (true)
        {
            const UInt8 current = byte();
            ++encoded_size;
            if (shift == 63 && (current & 0xfe) != 0)
                fail(Error::Code::InvalidValue, "synthetic metadata VarUInt overflows UInt64");
            result |= static_cast<UInt64>(current & 0x7f) << shift;
            if ((current & 0x80) == 0)
                break;
            if (shift >= 63)
                fail(Error::Code::InvalidValue, "synthetic metadata VarUInt overflows UInt64");
            shift = static_cast<UInt8>(shift + 7);
        }
        if (encoded_size != varUIntSize(result))
            fail(Error::Code::NonCanonical, "synthetic metadata VarUInt is not minimally encoded");
        if (result > maximum)
            fail(Error::Code::LimitExceeded, message);
        return result;
    }

    UUID uuid() { return uuidFromCanonicalBytes(array<sizeof(CanonicalUUID)>()); }
    Digest digest() { return array<sizeof(Digest)>(); }

    std::string_view frame(UInt64 maximum, std::string_view message)
    {
        const UInt64 size = minimalVarUInt(maximum, message);
        if (!std::in_range<size_t>(size))
            fail(Error::Code::LimitExceeded, "synthetic metadata frame exceeds the host size domain");
        const size_t materialized_size = static_cast<size_t>(size);
        require(materialized_size);
        const auto result = input.substr(position, materialized_size);
        position += materialized_size;
        return result;
    }

    template <size_t size>
    std::array<CanonicalByte, size> array()
    {
        require(size);
        std::array<CanonicalByte, size> result{};
        std::copy_n(reinterpret_cast<const CanonicalByte *>(input.data() + position), size, result.begin());
        position += size;
        return result;
    }

    void requireItemsFit(UInt64 count, size_t minimum_bytes, std::string_view message) const
    {
        if (!minimum_bytes || count > remaining() / minimum_bytes)
            fail(Error::Code::Truncated, message);
    }

    void requireEnd() const
    {
        if (position != input.size())
            fail(Error::Code::TrailingData, "synthetic metadata contains trailing bytes");
    }

private:
    void require(size_t size) const
    {
        if (size > remaining())
            fail(Error::Code::Truncated, "synthetic metadata is truncated");
    }

    std::string_view input;
    size_t position = 0;
};

bool pathLess(const PersistedTypeOccurrencePath & lhs, const PersistedTypeOccurrencePath & rhs) noexcept
{
    if (lhs.section != rhs.section)
        return static_cast<UInt8>(lhs.section) < static_cast<UInt8>(rhs.section);
    if (lhs.site != rhs.site)
        return static_cast<UInt8>(lhs.site) < static_cast<UInt8>(rhs.site);
    if (lhs.object_ordinal != rhs.object_ordinal)
        return lhs.object_ordinal < rhs.object_ordinal;
    if (lhs.occurrence_ordinal != rhs.occurrence_ordinal)
        return lhs.occurrence_ordinal < rhs.occurrence_ordinal;
    return std::lexicographical_compare(
        lhs.type_child_ordinals.begin(), lhs.type_child_ordinals.end(), rhs.type_child_ordinals.begin(), rhs.type_child_ordinals.end());
}

void writePath(Writer & writer, const PersistedTypeOccurrencePath & path)
{
    if (path.site != PersistedTypeOccurrenceSite::Declaration)
        fail(Error::Code::UnsupportedVersion, "synthetic V1 metadata cannot encode a non-declaration occurrence site");
    writer.byte(254);
    writer.varUInt(path.object_ordinal);
    writer.varUInt(path.occurrence_ordinal);
    writer.varUInt(checkedSize(path.type_child_ordinals.size(), "synthetic metadata path depth exceeds UInt64"));
    for (const UInt64 ordinal : path.type_child_ordinals)
        writer.varUInt(ordinal);
}

PersistedTypeOccurrencePath readPath(Reader & reader, const SyntheticObjectMetadataLimits & limits, UInt64 & retained_path_components)
{
    if (reader.byte() != 254)
        fail(Error::Code::InvalidValue, "synthetic metadata path has an unknown section");
    PersistedTypeOccurrencePath result;
    result.section = PersistedTypePathSection::SyntheticPayload;
    result.object_ordinal = reader.minimalVarUInt(std::numeric_limits<UInt64>::max(), "synthetic metadata object ordinal is invalid");
    result.occurrence_ordinal
        = reader.minimalVarUInt(std::numeric_limits<UInt64>::max(), "synthetic metadata occurrence ordinal is invalid");
    const UInt64 depth = reader.minimalVarUInt(limits.maximum_path_depth, "synthetic metadata path exceeds its depth limit");
    checkedCharge(
        retained_path_components,
        depth,
        limits.maximum_retained_path_components,
        "synthetic metadata retained path components exceed their limit");
    reader.requireItemsFit(depth, 1, "synthetic metadata path ordinals are truncated");
    result.type_child_ordinals.reserve(static_cast<size_t>(depth));
    for (UInt64 index = 0; index < depth; ++index)
    {
        result.type_child_ordinals.push_back(
            reader.minimalVarUInt(std::numeric_limits<UInt64>::max(), "synthetic metadata type-child ordinal is invalid"));
    }
    return result;
}

void writeOccurrence(Writer & writer, const SyntheticObjectPhysicalOccurrence & occurrence)
{
    writePath(writer, occurrence.path);
    writer.frame(occurrence.canonical_physical_type);
    writer.digest(occurrence.storage_fingerprint);
    writer.byte(occurrence.selected_semantic_capabilities);
}

void writePhysicalOccurrence(Writer & writer, const SyntheticObjectPhysicalOccurrence & occurrence)
{
    writePath(writer, occurrence.path);
    writer.frame(occurrence.canonical_physical_type);
    writer.digest(occurrence.storage_fingerprint);
}

void validateStructure(const SyntheticObjectMetadata & metadata, const SyntheticObjectMetadataLimits & limits)
{
    if (metadata.format_version != synthetic_object_metadata_format_version || metadata.semantic_extension_version != 1
        || metadata.semantic_extension_flags != 0)
        fail(Error::Code::UnsupportedVersion, "synthetic metadata version is not supported");
    if (!metadata.object.isValid() || metadata.object.kind != SchemaObjectKind::SyntheticTestObject || metadata.object_schema_revision == 0)
        fail(Error::Code::InvalidValue, "synthetic metadata identity or revision is invalid");
    if (metadata.diagnostic_name.empty() || containsZero(metadata.diagnostic_name)
        || checkedSize(metadata.diagnostic_name.size(), "synthetic metadata diagnostic name exceeds UInt64")
            > limits.maximum_diagnostic_name_bytes)
        fail(Error::Code::InvalidValue, "synthetic metadata diagnostic name is invalid");
    if (checkedSize(metadata.occurrences.size(), "synthetic metadata occurrence count exceeds UInt64") > limits.maximum_occurrences)
        fail(Error::Code::LimitExceeded, "synthetic metadata occurrence count exceeds its limit");

    UInt64 retained_path_components = 0;
    const PersistedTypeOccurrencePath * previous = nullptr;
    for (const auto & occurrence : metadata.occurrences)
    {
        if (occurrence.path.section != PersistedTypePathSection::SyntheticPayload
            || occurrence.path.site != PersistedTypeOccurrenceSite::Declaration
            || checkedSize(occurrence.path.type_child_ordinals.size(), "synthetic metadata path depth exceeds UInt64")
                > limits.maximum_path_depth)
            fail(Error::Code::InvalidValue, "synthetic metadata occurrence path is invalid");
        checkedCharge(
            retained_path_components,
            checkedSize(occurrence.path.type_child_ordinals.size(), "synthetic metadata path depth exceeds UInt64"),
            limits.maximum_retained_path_components,
            "synthetic metadata retained path components exceed their limit");
        if (previous && !pathLess(*previous, occurrence.path))
            fail(Error::Code::NonCanonical, "synthetic metadata occurrence paths are not strictly ordered");
        previous = &occurrence.path;
        if (occurrence.canonical_physical_type.empty() || containsZero(occurrence.canonical_physical_type)
            || checkedSize(occurrence.canonical_physical_type.size(), "synthetic metadata physical type exceeds UInt64")
                > limits.maximum_canonical_physical_type_bytes)
            fail(Error::Code::InvalidValue, "synthetic metadata physical type is invalid");
        if ((occurrence.selected_semantic_capabilities & static_cast<SemanticCapabilityMask>(~all_semantic_capabilities)) != 0)
            fail(Error::Code::InvalidValue, "synthetic metadata semantic capability mask is invalid");
    }
}

String encodeFingerprintPayload(const SyntheticObjectMetadata & metadata, const SyntheticObjectMetadataLimits & limits)
{
    Writer writer(limits.maximum_metadata_bytes);
    /// The expectation and metadata-record hashes bind object identity and
    /// revision separately. This digest is deliberately stable when an ALTER
    /// advances only the object revision or logical role annotations while
    /// preserving its physical schema.
    writer.varUInt(checkedSize(metadata.occurrences.size(), "synthetic metadata occurrence count exceeds UInt64"));
    for (const auto & occurrence : metadata.occurrences)
        writePhysicalOccurrence(writer, occurrence);
    return std::move(writer).finish();
}

String encodeRecord(const SyntheticObjectMetadata & metadata, const SyntheticObjectMetadataLimits & limits)
{
    Writer writer(limits.maximum_metadata_bytes);
    writer.uint16(metadata.format_version);
    writer.byte(254);
    writer.uuid(metadata.object.database_uuid);
    writer.uuid(metadata.object.object_uuid);
    writer.uint64(metadata.object_schema_revision);
    writer.frame(metadata.diagnostic_name);
    writer.varUInt(checkedSize(metadata.occurrences.size(), "synthetic metadata occurrence count exceeds UInt64"));
    for (const auto & occurrence : metadata.occurrences)
        writeOccurrence(writer, occurrence);
    writer.digest(metadata.physical_schema_fingerprint);
    writer.uint16(metadata.semantic_extension_version);
    writer.uint16(metadata.semantic_extension_flags);
    writer.digest(metadata.canonical_record_hash);
    return std::move(writer).finish();
}

String encodeRecordHashPayload(const SyntheticObjectMetadata & metadata, const SyntheticObjectMetadataLimits & limits)
{
    String result = encodeRecord(metadata, limits);
    if (result.size() < sizeof(Digest))
        fail(Error::Code::InvalidValue, "synthetic metadata record payload is incomplete");
    result.resize(result.size() - sizeof(Digest));
    return result;
}

AtomicAuthorityValidatedDependentObject
validateAgainstExpectation(const SidecarExpectationRecord & expectation, const SyntheticObjectMetadata & metadata)
{
    if (expectation.object.kind != SchemaObjectKind::SyntheticTestObject || metadata.object != expectation.object
        || metadata.object_schema_revision != expectation.object_schema_revision
        || metadata.physical_schema_fingerprint != expectation.physical_schema_fingerprint)
        fail(Error::Code::FingerprintMismatch, "synthetic metadata differs from its authority expectation");
    return {
        .object = metadata.object,
        .object_schema_revision = metadata.object_schema_revision,
        .physical_schema_fingerprint = metadata.physical_schema_fingerprint,
    };
}

}

SyntheticObjectMetadataError::SyntheticObjectMetadataError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

Digest computeSyntheticObjectPhysicalFingerprint(const SyntheticObjectMetadata & metadata, const SyntheticObjectMetadataLimits & limits)
{
    validateLimits(limits);
    validateStructure(metadata, limits);
    return hashFramedDomainSeparated(synthetic_object_physical_fingerprint_domain, encodeFingerprintPayload(metadata, limits));
}

Digest computeSyntheticObjectMetadataRecordHash(const SyntheticObjectMetadata & metadata, const SyntheticObjectMetadataLimits & limits)
{
    validateLimits(limits);
    validateStructure(metadata, limits);
    if (computeSyntheticObjectPhysicalFingerprint(metadata, limits) != metadata.physical_schema_fingerprint)
        fail(Error::Code::FingerprintMismatch, "synthetic metadata physical fingerprint is invalid");
    return hashFramedDomainSeparated(synthetic_object_metadata_record_hash_domain, encodeRecordHashPayload(metadata, limits));
}

SyntheticObjectMetadata makeSyntheticObjectMetadata(
    SchemaObjectID object,
    UInt64 object_schema_revision,
    String diagnostic_name,
    std::vector<SyntheticObjectPhysicalOccurrence> occurrences,
    const SyntheticObjectMetadataLimits & limits)
{
    SyntheticObjectMetadata result{
        .object = object,
        .object_schema_revision = object_schema_revision,
        .diagnostic_name = std::move(diagnostic_name),
        .occurrences = std::move(occurrences),
    };
    result.physical_schema_fingerprint = computeSyntheticObjectPhysicalFingerprint(result, limits);
    result.canonical_record_hash = computeSyntheticObjectMetadataRecordHash(result, limits);
    static_cast<void>(encodeSyntheticObjectMetadata(result, limits));
    return result;
}

String encodeSyntheticObjectMetadata(const SyntheticObjectMetadata & metadata, const SyntheticObjectMetadataLimits & limits)
{
    validateLimits(limits);
    validateStructure(metadata, limits);
    if (computeSyntheticObjectPhysicalFingerprint(metadata, limits) != metadata.physical_schema_fingerprint)
        fail(Error::Code::FingerprintMismatch, "synthetic metadata physical fingerprint is invalid");
    if (computeSyntheticObjectMetadataRecordHash(metadata, limits) != metadata.canonical_record_hash)
        fail(Error::Code::FingerprintMismatch, "synthetic metadata record hash is invalid");
    return encodeRecord(metadata, limits);
}

SyntheticObjectMetadata decodeSyntheticObjectMetadata(std::string_view bytes, const SyntheticObjectMetadataLimits & limits)
{
    validateLimits(limits);
    if (checkedSize(bytes.size(), "synthetic metadata input exceeds UInt64") > limits.maximum_metadata_bytes)
        fail(Error::Code::LimitExceeded, "synthetic metadata exceeds its byte limit");

    Reader reader(bytes);
    SyntheticObjectMetadata result;
    result.format_version = reader.uint16();
    if (result.format_version != synthetic_object_metadata_format_version)
        fail(Error::Code::UnsupportedVersion, "synthetic metadata version is not supported");
    if (reader.byte() != 254)
        fail(Error::Code::InvalidValue, "synthetic metadata object kind is invalid");
    result.object = {
        .kind = SchemaObjectKind::SyntheticTestObject,
        .database_uuid = reader.uuid(),
        .object_uuid = reader.uuid(),
    };
    result.object_schema_revision = reader.uint64();
    result.diagnostic_name
        = String(reader.frame(limits.maximum_diagnostic_name_bytes, "synthetic metadata diagnostic name exceeds its limit"));
    const UInt64 occurrence_count
        = reader.minimalVarUInt(limits.maximum_occurrences, "synthetic metadata occurrence count exceeds its limit");
    reader.requireItemsFit(occurrence_count, 5, "synthetic metadata occurrences are truncated");
    result.occurrences.reserve(static_cast<size_t>(occurrence_count));
    UInt64 retained_path_components = 0;
    for (UInt64 index = 0; index < occurrence_count; ++index)
    {
        SyntheticObjectPhysicalOccurrence occurrence;
        occurrence.path = readPath(reader, limits, retained_path_components);
        occurrence.canonical_physical_type
            = String(reader.frame(limits.maximum_canonical_physical_type_bytes, "synthetic metadata physical type exceeds its limit"));
        occurrence.storage_fingerprint = reader.digest();
        occurrence.selected_semantic_capabilities = reader.byte();
        result.occurrences.push_back(std::move(occurrence));
    }
    result.physical_schema_fingerprint = reader.digest();
    result.semantic_extension_version = reader.uint16();
    result.semantic_extension_flags = reader.uint16();
    result.canonical_record_hash = reader.digest();
    reader.requireEnd();

    validateStructure(result, limits);
    if (computeSyntheticObjectPhysicalFingerprint(result, limits) != result.physical_schema_fingerprint)
        fail(Error::Code::FingerprintMismatch, "synthetic metadata physical fingerprint is invalid");
    if (computeSyntheticObjectMetadataRecordHash(result, limits) != result.canonical_record_hash)
        fail(Error::Code::FingerprintMismatch, "synthetic metadata record hash is invalid");
    if (encodeRecord(result, limits) != bytes)
        fail(Error::Code::NonCanonical, "synthetic metadata is not canonical");
    return result;
}

AtomicAuthorityValidatedDependentObject validateSyntheticDependentObjectMetadata(
    const SidecarExpectationRecord & expectation, std::string_view canonical_metadata_bytes, const SyntheticObjectMetadataLimits & limits)
{
    const auto metadata = decodeSyntheticObjectMetadata(canonical_metadata_bytes, limits);
    return validateAgainstExpectation(expectation, metadata);
}

AtomicAuthorityValidatedDependentObject validateSyntheticDependentObjectMetadata(
    const SidecarExpectationRecord & expectation,
    std::string_view canonical_metadata_bytes,
    std::string_view canonical_sidecar_bytes,
    const SyntheticObjectMetadataLimits & metadata_limits,
    const PersistedTypeReferencesLimits & sidecar_limits)
{
    const auto metadata = decodeSyntheticObjectMetadata(canonical_metadata_bytes, metadata_limits);
    const auto validated = validateAgainstExpectation(expectation, metadata);

    PersistedTypeReferences references;
    try
    {
        references = decodePersistedTypeReferences(canonical_sidecar_bytes, sidecar_limits);
    }
    catch (const PersistedTypeReferencesError &)
    {
        fail(Error::Code::PhysicalTypeMismatch, "synthetic persisted references sidecar is invalid");
    }

    if (references.object != expectation.object || references.object_schema_revision != expectation.object_schema_revision
        || references.physical_schema_fingerprint != expectation.physical_schema_fingerprint
        || computePersistedTypeReferencesSidecarHash(references, sidecar_limits) != expectation.sidecar_hash)
        fail(Error::Code::FingerprintMismatch, "synthetic persisted references differ from their authority expectation");
    if (metadata.occurrences.size() != references.occurrence_paths.size())
        fail(Error::Code::PhysicalTypeMismatch, "synthetic metadata occurrence count differs from its persisted references");

    for (size_t index = 0; index < metadata.occurrences.size(); ++index)
    {
        const auto & occurrence = metadata.occurrences[index];
        const auto & use = references.uses[index];
        const auto & descriptor = references.descriptors[static_cast<size_t>(use.descriptor_id)];
        if (occurrence.path != references.occurrence_paths[index]
            || occurrence.canonical_physical_type != descriptor.getCanonicalPhysicalType()
            || occurrence.storage_fingerprint != descriptor.getStorageFingerprint()
            || (occurrence.selected_semantic_capabilities & descriptor.getSemanticCapabilities())
                != occurrence.selected_semantic_capabilities)
            fail(Error::Code::PhysicalTypeMismatch, "synthetic metadata occurrence differs from its persisted descriptor");
    }
    return validated;
}

BoundObjectPhysicalSchema
makeSyntheticBoundPhysicalSchema(const SyntheticObjectMetadata & metadata, const SyntheticObjectMetadataLimits & limits)
{
    validateLimits(limits);
    validateStructure(metadata, limits);
    if (computeSyntheticObjectPhysicalFingerprint(metadata, limits) != metadata.physical_schema_fingerprint)
        fail(Error::Code::FingerprintMismatch, "synthetic metadata physical fingerprint is invalid");

    BoundObjectPhysicalSchema result{
        .object = metadata.object,
        .object_schema_revision = metadata.object_schema_revision,
        .physical_schema_fingerprint = metadata.physical_schema_fingerprint,
        .occurrences = {},
    };
    result.occurrences.reserve(metadata.occurrences.size());
    for (const auto & occurrence : metadata.occurrences)
    {
        DataTypePtr physical_type;
        try
        {
            physical_type = DataTypeFactory::instance().get(occurrence.canonical_physical_type);
            if (!physical_type || physical_type->getName() != occurrence.canonical_physical_type
                || physicalTypeFingerprint(physical_type) != occurrence.storage_fingerprint)
                fail(Error::Code::PhysicalTypeMismatch, "synthetic metadata physical type differs from its fingerprint");
        }
        catch (const Error &)
        {
            throw;
        }
        catch (const Exception &)
        {
            fail(Error::Code::PhysicalTypeMismatch, "synthetic metadata physical type is not registered");
        }
        catch (const std::exception &)
        {
            fail(Error::Code::PhysicalTypeMismatch, "synthetic metadata physical type is not registered");
        }
        result.occurrences.push_back({
            .path = occurrence.path,
            .physical_type = std::move(physical_type),
            .runtime_owner_key = {},
            .selected_semantic_capabilities = occurrence.selected_semantic_capabilities,
        });
    }
    return result;
}

}
