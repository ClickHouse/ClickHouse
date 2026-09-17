#include <DataTypes/UDT/PersistedTypeReferences.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/quoteString.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <limits>
#include <span>
#include <string_view>
#include <system_error>
#include <utility>

namespace DB::UDT
{
namespace
{

constexpr UInt16 persisted_descriptor_format_version = 1;
constexpr UInt64 implementation_maximum_sidecar_bytes = 16ULL << 20;
constexpr UInt64 implementation_maximum_descriptors = 65'536;
constexpr UInt64 implementation_maximum_occurrence_paths = 65'536;
constexpr UInt64 implementation_maximum_path_depth = 64;
constexpr UInt64 implementation_maximum_canonical_arguments_bytes = 64ULL << 10;
constexpr UInt64 implementation_maximum_canonical_physical_type_bytes = 64ULL << 10;
constexpr UInt64 implementation_maximum_qualified_name_bytes = 4ULL << 10;
constexpr UInt64 implementation_maximum_text_bytes = 64ULL << 20;

[[noreturn]] void fail(PersistedTypeReferencesError::Code code, std::string_view message)
{
    throw PersistedTypeReferencesError(code, message);
}

UInt64 checkedSize(size_t value, std::string_view message)
{
    if (!std::in_range<UInt64>(value))
        fail(PersistedTypeReferencesError::Code::LimitExceeded, message);
    return static_cast<UInt64>(value);
}

UInt64 varUIntSize(UInt64 value) noexcept
{
    UInt64 size = 1;
    while (value >= 0x80)
    {
        value >>= 7;
        ++size;
    }
    return size;
}

bool isSidecarObjectKind(SchemaObjectKind kind) noexcept
{
    return kind == SchemaObjectKind::Table || kind == SchemaObjectKind::View || kind == SchemaObjectKind::Dictionary
        || kind == SchemaObjectKind::SyntheticTestObject;
}

PersistedTypePathSection pathSectionForObjectKind(SchemaObjectKind kind)
{
    switch (kind)
    {
        case SchemaObjectKind::Table: return PersistedTypePathSection::ColumnType;
        case SchemaObjectKind::View: return PersistedTypePathSection::ViewExpression;
        case SchemaObjectKind::Dictionary: return PersistedTypePathSection::DictionaryAttribute;
        case SchemaObjectKind::SyntheticTestObject: return PersistedTypePathSection::SyntheticPayload;
        case SchemaObjectKind::TypeDefinition: break;
    }
    fail(PersistedTypeReferencesError::Code::InvalidValue, "schema object kind cannot own a persisted type references sidecar");
}

UInt8 encodePathSection(PersistedTypePathSection section)
{
    switch (section)
    {
        case PersistedTypePathSection::ColumnType: return 1;
        case PersistedTypePathSection::ViewExpression: return 2;
        case PersistedTypePathSection::DictionaryAttribute: return 3;
        case PersistedTypePathSection::SyntheticPayload: return 254;
    }
    fail(PersistedTypeReferencesError::Code::InvalidValue, "unknown persisted type occurrence-path section");
}

PersistedTypePathSection decodePathSection(UInt8 value)
{
    switch (value)
    {
        case 1: return PersistedTypePathSection::ColumnType;
        case 2: return PersistedTypePathSection::ViewExpression;
        case 3: return PersistedTypePathSection::DictionaryAttribute;
        case 254: return PersistedTypePathSection::SyntheticPayload;
        default: fail(PersistedTypeReferencesError::Code::InvalidValue, "unknown persisted type occurrence-path section");
    }
}

UInt8 encodeOccurrenceSite(PersistedTypeOccurrenceSite site)
{
    switch (site)
    {
        case PersistedTypeOccurrenceSite::Declaration: return 1;
        case PersistedTypeOccurrenceSite::StoredExpression: return 2;
        case PersistedTypeOccurrenceSite::SchemaString: return 3;
    }
    fail(PersistedTypeReferencesError::Code::InvalidValue, "unknown persisted type occurrence site");
}

PersistedTypeOccurrenceSite decodeOccurrenceSite(UInt8 value)
{
    switch (value)
    {
        case 1: return PersistedTypeOccurrenceSite::Declaration;
        case 2: return PersistedTypeOccurrenceSite::StoredExpression;
        case 3: return PersistedTypeOccurrenceSite::SchemaString;
        default: fail(PersistedTypeReferencesError::Code::InvalidValue, "unknown persisted type occurrence site");
    }
}

bool isSupportedFormatPair(UInt16 format_version, UInt16 path_dictionary_version) noexcept
{
    return (format_version == persisted_type_references_format_version && path_dictionary_version == persisted_type_path_dictionary_version)
        || (format_version == persisted_type_references_format_version_v2
            && path_dictionary_version == persisted_type_path_dictionary_version_v2);
}

void validateLimits(const PersistedTypeReferencesLimits & limits)
{
    if (!limits.maximum_sidecar_bytes || !limits.maximum_descriptors || !limits.maximum_occurrence_paths
        || !limits.maximum_canonical_arguments_bytes || !limits.maximum_canonical_physical_type_bytes
        || !limits.maximum_qualified_name_bytes)
        fail(PersistedTypeReferencesError::Code::InvalidValue, "every persisted type references limit except path depth must be nonzero");
    if (limits.maximum_sidecar_bytes > implementation_maximum_sidecar_bytes
        || limits.maximum_descriptors > implementation_maximum_descriptors
        || limits.maximum_occurrence_paths > implementation_maximum_occurrence_paths
        || limits.maximum_path_depth > implementation_maximum_path_depth
        || limits.maximum_canonical_arguments_bytes > implementation_maximum_canonical_arguments_bytes
        || limits.maximum_canonical_physical_type_bytes > implementation_maximum_canonical_physical_type_bytes
        || limits.maximum_qualified_name_bytes > implementation_maximum_qualified_name_bytes)
        fail(PersistedTypeReferencesError::Code::InvalidValue, "a persisted type references limit exceeds the V1 implementation maximum");
}

void validateTextLimits(const PersistedTypeReferencesLimits & limits)
{
    validateLimits(limits);
    if (!limits.maximum_text_bytes || limits.maximum_text_bytes > implementation_maximum_text_bytes)
        fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references text byte limit is invalid");
}

TypeDescriptorLimits descriptorLimits(const PersistedTypeReferencesLimits & limits)
{
    TypeDescriptorLimits result;
    result.maximum_canonical_arguments_bytes = limits.maximum_canonical_arguments_bytes;
    result.maximum_canonical_physical_type_bytes = limits.maximum_canonical_physical_type_bytes;
    result.maximum_qualified_name_bytes = limits.maximum_qualified_name_bytes;
    return result;
}

class Writer final
{
public:
    explicit Writer(UInt64 maximum_bytes_, bool count_only_ = false)
        : maximum_bytes(maximum_bytes_)
        , count_only(count_only_)
    {
    }

    void writeByte(UInt8 value)
    {
        reserveLogical(1);
        if (!count_only)
            output.push_back(static_cast<char>(value));
    }

    void writeUInt16LE(UInt16 value)
    {
        writeByte(static_cast<UInt8>(value));
        writeByte(static_cast<UInt8>(value >> 8));
    }

    void writeUInt64LE(UInt64 value)
    {
        for (size_t index = 0; index < sizeof(value); ++index)
            writeByte(static_cast<UInt8>(value >> (8 * index)));
    }

    void writeVarUInt(UInt64 value)
    {
        do
        {
            UInt8 byte = static_cast<UInt8>(value & 0x7f);
            value >>= 7;
            if (value)
                byte = static_cast<UInt8>(byte | 0x80);
            writeByte(byte);
        } while (value);
    }

    void writeBytes(std::string_view value)
    {
        reserveLogical(checkedSize(value.size(), "persisted type references byte string exceeds UInt64"));
        if (!count_only)
            output.append(value);
    }

    void writeBytes(std::span<const CanonicalByte> value) { writeBytes({reinterpret_cast<const char *>(value.data()), value.size()}); }

    template <size_t size>
    void writeBytes(const std::array<CanonicalByte, size> & value)
    {
        writeBytes(std::span<const CanonicalByte>(value));
    }

    void writeUUID(const UUID & value) { writeBytes(uuidToCanonicalBytes(value)); }
    void writeDigest(const Digest & value) { writeBytes(value); }

    void writeFrame(std::string_view value)
    {
        writeVarUInt(checkedSize(value.size(), "persisted type references frame exceeds UInt64"));
        writeBytes(value);
    }

    UInt64 size() const noexcept { return logical_size; }
    UInt64 maximum() const noexcept { return maximum_bytes; }

    void reserveMaterialized(UInt64 size)
    {
        if (count_only || !std::in_range<size_t>(size) || size > output.max_size())
            fail(PersistedTypeReferencesError::Code::LimitExceeded, "persisted type references output exceeds the host string domain");
        output.reserve(static_cast<size_t>(size));
    }

    String finish() &&
    {
        if (count_only)
            fail(PersistedTypeReferencesError::Code::InvalidValue, "cannot materialize a counting persisted type references writer");
        return std::move(output);
    }

private:
    void reserveLogical(UInt64 addition)
    {
        if (addition > maximum_bytes || logical_size > maximum_bytes - addition)
            fail(PersistedTypeReferencesError::Code::LimitExceeded, "persisted type references exceed their sidecar byte limit");
        logical_size += addition;
    }

    UInt64 maximum_bytes;
    bool count_only;
    UInt64 logical_size = 0;
    String output;
};

template <typename Action>
void writeNestedFrame(Writer & writer, Action && action)
{
    Writer counter(writer.maximum(), true);
    action(counter);
    writer.writeVarUInt(counter.size());
    action(writer);
}

class Reader final
{
public:
    explicit Reader(std::string_view bytes_)
        : bytes(bytes_)
    {
    }

    size_t remaining() const noexcept { return bytes.size() - position; }

    UInt8 readByte()
    {
        require(1);
        return static_cast<UInt8>(bytes[position++]);
    }

    UInt16 readUInt16LE()
    {
        require(sizeof(UInt16));
        const UInt16 value = static_cast<UInt16>(
            static_cast<UInt8>(bytes[position]) | (static_cast<UInt16>(static_cast<UInt8>(bytes[position + 1])) << 8));
        position += sizeof(UInt16);
        return value;
    }

    UInt64 readUInt64LE()
    {
        require(sizeof(UInt64));
        UInt64 value = 0;
        for (size_t index = 0; index < sizeof(UInt64); ++index)
            value |= static_cast<UInt64>(static_cast<UInt8>(bytes[position + index])) << (8 * index);
        position += sizeof(UInt64);
        return value;
    }

    UInt64 readMinimalVarUInt(UInt64 maximum, std::string_view limit_message)
    {
        UInt64 value = 0;
        UInt8 shift = 0;
        size_t encoded_size = 0;
        while (true)
        {
            const UInt8 byte = readByte();
            ++encoded_size;
            if (shift == 63 && (byte & 0xfe) != 0)
                fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references VarUInt overflows UInt64");
            value |= static_cast<UInt64>(byte & 0x7f) << shift;
            if ((byte & 0x80) == 0)
                break;
            if (shift >= 63)
                fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references VarUInt overflows UInt64");
            shift = static_cast<UInt8>(shift + 7);
        }
        if (encoded_size != varUIntSize(value))
            fail(PersistedTypeReferencesError::Code::NonCanonical, "persisted type references VarUInt is not minimally encoded");
        if (value > maximum)
            fail(PersistedTypeReferencesError::Code::LimitExceeded, limit_message);
        return value;
    }

    UUID readUUID() { return uuidFromCanonicalBytes(readArray<sizeof(CanonicalUUID)>()); }
    Digest readDigest() { return readArray<sizeof(Digest)>(); }

    std::string_view readFrame(UInt64 maximum, std::string_view limit_message)
    {
        const UInt64 size = readMinimalVarUInt(maximum, limit_message);
        if (!std::in_range<size_t>(size))
            fail(PersistedTypeReferencesError::Code::LimitExceeded, "persisted type references frame exceeds the host size domain");
        require(static_cast<size_t>(size));
        const std::string_view result = bytes.substr(position, static_cast<size_t>(size));
        position += static_cast<size_t>(size);
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

    void requireItemsFit(UInt64 count, size_t minimum_item_bytes, std::string_view message) const
    {
        if (!minimum_item_bytes || count > remaining() / minimum_item_bytes)
            fail(PersistedTypeReferencesError::Code::Truncated, message);
    }

    void requireEnd(std::string_view message) const
    {
        if (position != bytes.size())
            fail(PersistedTypeReferencesError::Code::TrailingData, message);
    }

private:
    void require(size_t size) const
    {
        if (size > remaining())
            fail(PersistedTypeReferencesError::Code::Truncated, "persisted type references are truncated");
    }

    std::string_view bytes;
    size_t position = 0;
};

void writeObjectID(Writer & writer, const SchemaObjectID & object)
{
    writer.writeByte(static_cast<UInt8>(object.kind));
    writer.writeUUID(object.database_uuid);
    writer.writeUUID(object.object_uuid);
}

SchemaObjectID readObjectID(Reader & reader)
{
    return {
        .kind = static_cast<SchemaObjectKind>(reader.readByte()),
        .database_uuid = reader.readUUID(),
        .object_uuid = reader.readUUID(),
    };
}

void writeDescriptor(Writer & writer, const PersistedTypeDescriptor & descriptor)
{
    const auto & identity = descriptor.getDefinitionIdentity();
    writer.writeUInt16LE(persisted_descriptor_format_version);
    writer.writeUUID(identity.database_uuid);
    writer.writeUUID(identity.type_uuid);
    writer.writeUInt64LE(identity.revision);
    writer.writeDigest(descriptor.getDefinitionHash());
    writer.writeFrame(descriptor.getCanonicalArgumentsEncoding());
    writer.writeFrame(descriptor.getCanonicalPhysicalType());
    writer.writeDigest(descriptor.getInstantiationSemanticHash());
    writer.writeDigest(descriptor.getStorageFingerprint());
    writer.writeUInt16LE(descriptor.getCheckerABI());
    writer.writeUInt16LE(descriptor.getCheckerChargeABI());
    writer.writeUInt16LE(descriptor.getPolicyABI());
    writer.writeUInt16LE(descriptor.getFunctionRegistryABI());
    writer.writeDigest(descriptor.getPolicySemanticHash());
    writer.writeByte(descriptor.getSemanticCapabilities());
    writer.writeFrame(descriptor.getLastKnownQualifiedName());
}

PersistedTypeDescriptor readDescriptor(std::string_view bytes, const PersistedTypeReferencesLimits & limits)
{
    Reader reader(bytes);
    if (reader.readUInt16LE() != persisted_descriptor_format_version)
        fail(PersistedTypeReferencesError::Code::UnsupportedVersion, "unsupported persisted descriptor version");

    DefinitionIdentity identity{
        .database_uuid = reader.readUUID(),
        .type_uuid = reader.readUUID(),
        .revision = reader.readUInt64LE(),
    };
    const Digest definition_hash = reader.readDigest();
    const std::string_view canonical_arguments
        = reader.readFrame(limits.maximum_canonical_arguments_bytes, "persisted descriptor canonical arguments exceed their byte limit");
    const std::string_view canonical_physical_type
        = reader.readFrame(limits.maximum_canonical_physical_type_bytes, "persisted descriptor physical type exceeds its byte limit");
    const Digest instantiation_semantic_hash = reader.readDigest();
    const Digest storage_fingerprint = reader.readDigest();
    const UInt16 checker_abi = reader.readUInt16LE();
    const UInt16 checker_charge_abi = reader.readUInt16LE();
    const UInt16 policy_abi = reader.readUInt16LE();
    const UInt16 function_registry_abi = reader.readUInt16LE();
    const Digest policy_semantic_hash = reader.readDigest();
    const SemanticCapabilityMask semantic_capabilities = reader.readByte();
    const std::string_view last_known_qualified_name
        = reader.readFrame(limits.maximum_qualified_name_bytes, "persisted descriptor diagnostic name exceeds its byte limit");
    reader.requireEnd("persisted descriptor frame has trailing data");

    try
    {
        return PersistedTypeDescriptor::fromCanonicalPersistenceFields(
            identity,
            definition_hash,
            String(canonical_arguments),
            String(canonical_physical_type),
            instantiation_semantic_hash,
            storage_fingerprint,
            checker_abi,
            checker_charge_abi,
            policy_abi,
            function_registry_abi,
            policy_semantic_hash,
            semantic_capabilities,
            String(last_known_qualified_name),
            descriptorLimits(limits));
    }
    catch (const DescriptorError & error)
    {
        if (error.code == DescriptorError::Code::LimitExceeded)
            fail(PersistedTypeReferencesError::Code::LimitExceeded, error.what());
        if (error.code == DescriptorError::Code::ConflictingIdentity)
            fail(PersistedTypeReferencesError::Code::DigestMismatch, error.what());
        fail(PersistedTypeReferencesError::Code::InvalidValue, error.what());
    }
}

void writePath(Writer & writer, const PersistedTypeOccurrencePath & path, UInt16 path_dictionary_version)
{
    writer.writeByte(encodePathSection(path.section));
    if (path_dictionary_version == persisted_type_path_dictionary_version_v2)
        writer.writeByte(encodeOccurrenceSite(path.site));
    writer.writeVarUInt(path.object_ordinal);
    writer.writeVarUInt(path.occurrence_ordinal);
    writer.writeVarUInt(checkedSize(path.type_child_ordinals.size(), "persisted type occurrence-path depth exceeds UInt64"));
    for (const UInt64 ordinal : path.type_child_ordinals)
        writer.writeVarUInt(ordinal);
}

PersistedTypeOccurrencePath readPath(std::string_view bytes, UInt16 path_dictionary_version, const PersistedTypeReferencesLimits & limits)
{
    Reader reader(bytes);
    PersistedTypeOccurrencePath result;
    result.section = decodePathSection(reader.readByte());
    if (path_dictionary_version == persisted_type_path_dictionary_version_v2)
        result.site = decodeOccurrenceSite(reader.readByte());
    result.object_ordinal = reader.readMinimalVarUInt(std::numeric_limits<UInt64>::max(), {});
    result.occurrence_ordinal = reader.readMinimalVarUInt(std::numeric_limits<UInt64>::max(), {});
    const UInt64 depth = reader.readMinimalVarUInt(limits.maximum_path_depth, "persisted type occurrence path exceeds its depth limit");
    reader.requireItemsFit(depth, 1, "persisted type occurrence path has fewer child ordinals than declared");
    result.type_child_ordinals.reserve(static_cast<size_t>(depth));
    for (UInt64 index = 0; index < depth; ++index)
        result.type_child_ordinals.push_back(reader.readMinimalVarUInt(std::numeric_limits<UInt64>::max(), {}));
    reader.requireEnd("persisted type occurrence-path frame has trailing data");
    return result;
}

void writeUse(Writer & writer, const PersistedTypeOccurrenceUse & use)
{
    writer.writeVarUInt(use.path_id);
    writer.writeVarUInt(use.descriptor_id);
}

PersistedTypeOccurrenceUse readUse(std::string_view bytes)
{
    Reader reader(bytes);
    PersistedTypeOccurrenceUse result;
    result.path_id = reader.readMinimalVarUInt(std::numeric_limits<UInt64>::max(), {});
    result.descriptor_id = reader.readMinimalVarUInt(std::numeric_limits<UInt64>::max(), {});
    reader.requireEnd("persisted type occurrence-use frame has trailing data");
    return result;
}

bool pathLess(const PersistedTypeOccurrencePath & lhs, const PersistedTypeOccurrencePath & rhs) noexcept
{
    const UInt8 lhs_section = static_cast<UInt8>(lhs.section);
    const UInt8 rhs_section = static_cast<UInt8>(rhs.section);
    if (lhs_section != rhs_section)
        return lhs_section < rhs_section;
    const UInt8 lhs_site = static_cast<UInt8>(lhs.site);
    const UInt8 rhs_site = static_cast<UInt8>(rhs.site);
    if (lhs_site != rhs_site)
        return lhs_site < rhs_site;
    if (lhs.object_ordinal != rhs.object_ordinal)
        return lhs.object_ordinal < rhs.object_ordinal;
    if (lhs.occurrence_ordinal != rhs.occurrence_ordinal)
        return lhs.occurrence_ordinal < rhs.occurrence_ordinal;
    return std::lexicographical_compare(
        lhs.type_child_ordinals.begin(), lhs.type_child_ordinals.end(), rhs.type_child_ordinals.begin(), rhs.type_child_ordinals.end());
}

bool useLess(const PersistedTypeOccurrenceUse & lhs, const PersistedTypeOccurrenceUse & rhs) noexcept
{
    return lhs.path_id < rhs.path_id || (lhs.path_id == rhs.path_id && lhs.descriptor_id < rhs.descriptor_id);
}

void validateDescriptorAgainstLimits(const PersistedTypeDescriptor & descriptor, const PersistedTypeReferencesLimits & limits)
{
    if (descriptor.getCanonicalArgumentsEncoding().size() > limits.maximum_canonical_arguments_bytes
        || descriptor.getCanonicalPhysicalType().size() > limits.maximum_canonical_physical_type_bytes
        || descriptor.getLastKnownQualifiedName().size() > limits.maximum_qualified_name_bytes)
        fail(PersistedTypeReferencesError::Code::LimitExceeded, "a persisted descriptor field exceeds its sidecar limit");
}

void validateReferences(const PersistedTypeReferences & references, const PersistedTypeReferencesLimits & limits)
{
    validateLimits(limits);
    if (references.format_version != persisted_type_references_format_version
        && references.format_version != persisted_type_references_format_version_v2)
        fail(PersistedTypeReferencesError::Code::UnsupportedVersion, "unsupported persisted type references version");
    if (!references.object.isValid() || !isSidecarObjectKind(references.object.kind))
        fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references object identity is invalid");
    if (!references.object_schema_revision)
        fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references object schema revision is zero");
    if (!isSupportedFormatPair(references.format_version, references.path_dictionary_version))
        fail(PersistedTypeReferencesError::Code::UnsupportedVersion, "unsupported persisted type occurrence-path dictionary version");
    if (references.semantic_extension_version != 1 || references.semantic_extension_flags != 0)
        fail(PersistedTypeReferencesError::Code::UnsupportedVersion, "unsupported persisted type references semantic extension");
    const PersistedTypePathSection required_path_section = pathSectionForObjectKind(references.object.kind);

    if (references.descriptors.empty() || references.occurrence_paths.empty())
        fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references sidecar is logically empty");
    if (references.descriptors.size() > limits.maximum_descriptors)
        fail(PersistedTypeReferencesError::Code::LimitExceeded, "persisted descriptor count exceeds its limit");
    if (references.occurrence_paths.size() > limits.maximum_occurrence_paths || references.uses.size() > limits.maximum_occurrence_paths)
        fail(PersistedTypeReferencesError::Code::LimitExceeded, "persisted occurrence count exceeds its limit");

    for (size_t index = 0; index < references.descriptors.size(); ++index)
    {
        validateDescriptorAgainstLimits(references.descriptors[index], limits);
        if (references.descriptors[index].getDefinitionIdentity().database_uuid != references.object.database_uuid)
            fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted descriptor belongs to a different database than its object");
        if (index && !references.descriptors[index - 1].stableLess(references.descriptors[index]))
            fail(PersistedTypeReferencesError::Code::NonCanonical, "persisted descriptor dictionary is not strictly sorted");
    }
    for (size_t index = 0; index < references.occurrence_paths.size(); ++index)
    {
        const auto & path = references.occurrence_paths[index];
        static_cast<void>(encodePathSection(path.section));
        static_cast<void>(encodeOccurrenceSite(path.site));
        if (path.section != required_path_section)
            fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted occurrence-path section does not match its object kind");
        if (references.format_version == persisted_type_references_format_version && path.site != PersistedTypeOccurrenceSite::Declaration)
            fail(PersistedTypeReferencesError::Code::InvalidValue, "V1 persisted occurrence path is not a declaration endpoint");
        if (path.type_child_ordinals.size() > limits.maximum_path_depth)
            fail(PersistedTypeReferencesError::Code::LimitExceeded, "persisted occurrence path exceeds its depth limit");
        if (index && !pathLess(references.occurrence_paths[index - 1], path))
            fail(PersistedTypeReferencesError::Code::NonCanonical, "persisted occurrence-path dictionary is not strictly sorted");
    }

    if (references.uses.size() != references.occurrence_paths.size())
        fail(PersistedTypeReferencesError::Code::InvalidValue, "every persisted occurrence path must have exactly one use");
    std::vector<bool> descriptor_referenced(references.descriptors.size(), false);
    for (size_t index = 0; index < references.uses.size(); ++index)
    {
        const auto & use = references.uses[index];
        if (index && !useLess(references.uses[index - 1], use))
            fail(PersistedTypeReferencesError::Code::NonCanonical, "persisted occurrence uses are not strictly sorted");
        if (use.path_id != index)
            fail(PersistedTypeReferencesError::Code::NonCanonical, "persisted occurrence uses do not cover path IDs exactly once in order");
        if (use.descriptor_id >= references.descriptors.size())
            fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted occurrence use references an absent descriptor");
        descriptor_referenced[static_cast<size_t>(use.descriptor_id)] = true;
    }
    if (std::find(descriptor_referenced.begin(), descriptor_referenced.end(), false) != descriptor_referenced.end())
        fail(PersistedTypeReferencesError::Code::NonCanonical, "persisted descriptor dictionary contains an unreferenced entry");
}

void writeReferences(Writer & writer, const PersistedTypeReferences & references)
{
    writer.writeUInt16LE(references.format_version);
    writeObjectID(writer, references.object);
    writer.writeUInt64LE(references.object_schema_revision);
    writer.writeDigest(references.physical_schema_fingerprint);
    writer.writeUInt16LE(references.path_dictionary_version);

    writer.writeVarUInt(checkedSize(references.descriptors.size(), "persisted descriptor count exceeds UInt64"));
    for (const auto & descriptor : references.descriptors)
        writeNestedFrame(writer, [&](Writer & nested) { writeDescriptor(nested, descriptor); });

    writer.writeVarUInt(checkedSize(references.occurrence_paths.size(), "persisted occurrence-path count exceeds UInt64"));
    for (const auto & path : references.occurrence_paths)
        writeNestedFrame(writer, [&](Writer & nested) { writePath(nested, path, references.path_dictionary_version); });

    writer.writeVarUInt(checkedSize(references.uses.size(), "persisted occurrence-use count exceeds UInt64"));
    for (const auto & use : references.uses)
        writeNestedFrame(writer, [&](Writer & nested) { writeUse(nested, use); });

    writer.writeUInt16LE(references.semantic_extension_version);
    writer.writeUInt16LE(references.semantic_extension_flags);
}

std::string_view objectKindName(SchemaObjectKind kind)
{
    switch (kind)
    {
        case SchemaObjectKind::Table: return "TABLE";
        case SchemaObjectKind::View: return "VIEW";
        case SchemaObjectKind::Dictionary: return "DICTIONARY";
        case SchemaObjectKind::SyntheticTestObject: return "SYNTHETIC";
        case SchemaObjectKind::TypeDefinition: break;
    }
    fail(PersistedTypeReferencesError::Code::InvalidValue, "schema object kind cannot own a persisted type references clause");
}

class TextWriter final
{
public:
    explicit TextWriter(UInt64 maximum_bytes_)
        : maximum_bytes(maximum_bytes_)
    {
    }

    void write(std::string_view value)
    {
        const UInt64 addition = checkedSize(value.size(), "persisted type references text fragment exceeds UInt64");
        const UInt64 current_size = checkedSize(output.size(), "persisted type references text exceeds UInt64");
        if (addition > maximum_bytes || current_size > maximum_bytes - addition)
            fail(PersistedTypeReferencesError::Code::LimitExceeded, "persisted type references exceed their text byte limit");
        output.append(value);
    }

    void writeUInt64(UInt64 value)
    {
        std::array<char, std::numeric_limits<UInt64>::digits10 + 1> buffer{};
        const auto [end, error] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
        if (error != std::errc{})
            fail(PersistedTypeReferencesError::Code::InvalidValue, "cannot format a persisted type references integer");
        write({buffer.data(), static_cast<size_t>(end - buffer.data())});
    }

    void writeUUIDLiteral(const UUID & value)
    {
        const auto formatted = formatUUID(value);
        write("'");
        write({formatted.data(), formatted.size()});
        write("'");
    }

    void writeDigestLiteral(const Digest & value)
    {
        write("'");
        writeHex(std::span(value));
        write("'");
    }

    void writeHexLiteral(std::string_view value)
    {
        write("'");
        writeHex({reinterpret_cast<const CanonicalByte *>(value.data()), value.size()});
        write("'");
    }

    void writeQuoted(std::string_view value) { write(quoteString(value)); }

    String finish() && { return std::move(output); }

private:
    void writeHex(std::span<const CanonicalByte> value)
    {
        constexpr std::string_view digits = "0123456789abcdef";
        if (value.size() > std::numeric_limits<size_t>::max() / 2)
            fail(PersistedTypeReferencesError::Code::LimitExceeded, "persisted type references hexadecimal text exceeds the host domain");
        String encoded(value.size() * 2, '\0');
        for (size_t index = 0; index < value.size(); ++index)
        {
            encoded[index * 2] = digits[value[index] >> 4];
            encoded[index * 2 + 1] = digits[value[index] & 0x0f];
        }
        write(encoded);
    }

    UInt64 maximum_bytes;
    String output;
};

UInt8 decodeHexDigit(char value)
{
    if (value >= '0' && value <= '9')
        return static_cast<UInt8>(value - '0');
    if (value >= 'a' && value <= 'f')
        return static_cast<UInt8>(value - 'a' + 10);
    if (value >= 'A' && value <= 'F')
        return static_cast<UInt8>(value - 'A' + 10);
    fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references contain a non-hexadecimal digit");
}

class TextReader final
{
public:
    explicit TextReader(std::string_view text_)
        : text(text_)
    {
    }

    bool consume(std::string_view expected)
    {
        if (!remaining().starts_with(expected))
            return false;
        position += expected.size();
        return true;
    }

    void expect(std::string_view expected)
    {
        const std::string_view rest = remaining();
        if (rest.size() < expected.size())
        {
            if (expected.substr(0, rest.size()) == rest)
                fail(PersistedTypeReferencesError::Code::Truncated, "persisted type references text is truncated");
            fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references text has invalid syntax");
        }
        if (!rest.starts_with(expected))
            fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references text has invalid syntax");
        position += expected.size();
    }

    UInt64 readUInt64()
    {
        if (position == text.size())
            fail(PersistedTypeReferencesError::Code::Truncated, "persisted type references integer is truncated");
        const size_t begin = position;
        while (position < text.size() && text[position] >= '0' && text[position] <= '9')
            ++position;
        if (position == begin)
            fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references integer is missing");

        UInt64 result = 0;
        const auto [end, error] = std::from_chars(text.data() + begin, text.data() + position, result);
        if (error != std::errc{} || end != text.data() + position)
            fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references integer overflows UInt64");
        return result;
    }

    String readQuotedString(UInt64 maximum_bytes, std::string_view limit_message)
    {
        expect("'");
        String result;
        while (position < text.size())
        {
            char value = text[position++];
            if (value == '\'')
                return result;
            if (value == '\\')
            {
                if (position == text.size())
                    fail(PersistedTypeReferencesError::Code::Truncated, "persisted type references escape sequence is truncated");
                switch (text[position++])
                {
                    case '\\': value = '\\'; break;
                    case '\'': value = '\''; break;
                    case 'b': value = '\b'; break;
                    case 'f': value = '\f'; break;
                    case 'n': value = '\n'; break;
                    case 'r': value = '\r'; break;
                    case 't': value = '\t'; break;
                    case '0': value = '\0'; break;
                    default:
                        fail(
                            PersistedTypeReferencesError::Code::InvalidValue,
                            "persisted type references contain an invalid escape sequence");
                }
            }
            if (checkedSize(result.size(), limit_message) >= maximum_bytes)
                fail(PersistedTypeReferencesError::Code::LimitExceeded, limit_message);
            result.push_back(value);
        }
        fail(PersistedTypeReferencesError::Code::Truncated, "persisted type references quoted string is truncated");
    }

    UUID readUUID()
    {
        const String encoded = readQuotedString(36, "persisted type references UUID exceeds its text length");
        UUID result = UUIDHelpers::Nil;
        if (!tryParseUUID({reinterpret_cast<const UInt8 *>(encoded.data()), encoded.size()}, result))
            fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references contain an invalid UUID");
        return result;
    }

    Digest readDigest()
    {
        const String encoded = readQuotedString(64, "persisted type references digest exceeds its text length");
        if (encoded.size() != 64)
            fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references digest has the wrong length");
        Digest result{};
        for (size_t index = 0; index < result.size(); ++index)
            result[index] = static_cast<CanonicalByte>((decodeHexDigit(encoded[index * 2]) << 4) | decodeHexDigit(encoded[index * 2 + 1]));
        return result;
    }

    String readHexBytes(UInt64 maximum_decoded_bytes)
    {
        if (maximum_decoded_bytes > std::numeric_limits<UInt64>::max() / 2)
            fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references hexadecimal limit overflows UInt64");
        const String encoded
            = readQuotedString(maximum_decoded_bytes * 2, "persisted descriptor canonical arguments exceed their byte limit");
        if (encoded.size() % 2 != 0)
            fail(
                PersistedTypeReferencesError::Code::InvalidValue,
                "persisted descriptor canonical arguments have odd-length hexadecimal text");
        String result(encoded.size() / 2, '\0');
        for (size_t index = 0; index < result.size(); ++index)
            result[index] = static_cast<char>((decodeHexDigit(encoded[index * 2]) << 4) | decodeHexDigit(encoded[index * 2 + 1]));
        return result;
    }

    void requireEnd() const
    {
        if (position != text.size())
            fail(PersistedTypeReferencesError::Code::TrailingData, "persisted type references text has trailing data");
    }

private:
    std::string_view remaining() const { return text.substr(position); }

    std::string_view text;
    size_t position = 0;
};

SchemaObjectKind readObjectKind(TextReader & reader)
{
    if (reader.consume("TABLE"))
        return SchemaObjectKind::Table;
    if (reader.consume("VIEW"))
        return SchemaObjectKind::View;
    if (reader.consume("DICTIONARY"))
        return SchemaObjectKind::Dictionary;
    if (reader.consume("SYNTHETIC"))
        return SchemaObjectKind::SyntheticTestObject;
    fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references contain an invalid object kind");
}

UInt16 narrowUInt16(UInt64 value, std::string_view message)
{
    if (value > std::numeric_limits<UInt16>::max())
        fail(PersistedTypeReferencesError::Code::InvalidValue, message);
    return static_cast<UInt16>(value);
}

UInt8 narrowUInt8(UInt64 value, std::string_view message)
{
    if (value > std::numeric_limits<UInt8>::max())
        fail(PersistedTypeReferencesError::Code::InvalidValue, message);
    return static_cast<UInt8>(value);
}

void writeDescriptorText(TextWriter & writer, UInt64 descriptor_id, const PersistedTypeDescriptor & descriptor)
{
    const auto & identity = descriptor.getDefinitionIdentity();
    writer.write("  REF ");
    writer.writeUInt64(descriptor_id);
    writer.write(" DATABASE UUID ");
    writer.writeUUIDLiteral(identity.database_uuid);
    writer.write(" TYPE UUID ");
    writer.writeUUIDLiteral(identity.type_uuid);
    writer.write(" REVISION ");
    writer.writeUInt64(identity.revision);
    writer.write(" ARGUMENTS HEX ");
    writer.writeHexLiteral(descriptor.getCanonicalArgumentsEncoding());
    writer.write(" INSTANTIATION HASH ");
    writer.writeDigestLiteral(descriptor.getInstantiationSemanticHash());
    writer.write(" PHYSICAL TYPE ");
    writer.writeQuoted(descriptor.getCanonicalPhysicalType());
    writer.write(" NAME ");
    writer.writeQuoted(descriptor.getLastKnownQualifiedName());
    writer.write(" DEFINITION HASH ");
    writer.writeDigestLiteral(descriptor.getDefinitionHash());
    writer.write(" STORAGE FINGERPRINT ");
    writer.writeDigestLiteral(descriptor.getStorageFingerprint());
    writer.write(" CHECKER ABI ");
    writer.writeUInt64(descriptor.getCheckerABI());
    writer.write(" CHECKER CHARGE ABI ");
    writer.writeUInt64(descriptor.getCheckerChargeABI());
    writer.write(" POLICY ABI ");
    writer.writeUInt64(descriptor.getPolicyABI());
    writer.write(" FUNCTION REGISTRY ABI ");
    writer.writeUInt64(descriptor.getFunctionRegistryABI());
    writer.write(" POLICY HASH ");
    writer.writeDigestLiteral(descriptor.getPolicySemanticHash());
    writer.write(" CAPABILITIES ");
    writer.writeUInt64(descriptor.getSemanticCapabilities());
}

void writeUseText(
    TextWriter & writer, UInt16 format_version, const PersistedTypeOccurrencePath & path, const PersistedTypeOccurrenceUse & use)
{
    writer.write("  USE PATH (");
    writer.writeUInt64(encodePathSection(path.section));
    if (format_version == persisted_type_references_format_version_v2)
    {
        writer.write(", ");
        writer.writeUInt64(encodeOccurrenceSite(path.site));
    }
    writer.write(", ");
    writer.writeUInt64(path.object_ordinal);
    writer.write(", ");
    writer.writeUInt64(path.occurrence_ordinal);
    for (const UInt64 ordinal : path.type_child_ordinals)
    {
        writer.write(", ");
        writer.writeUInt64(ordinal);
    }
    writer.write(") REF ");
    writer.writeUInt64(use.descriptor_id);
}

String writeReferencesText(const PersistedTypeReferences & references, UInt64 maximum_text_bytes)
{
    TextWriter writer(maximum_text_bytes);
    writer.write("TYPE REFERENCES V");
    writer.writeUInt64(references.format_version);
    writer.write(" (\n  OBJECT ");
    writer.write(objectKindName(references.object.kind));
    writer.write(" DATABASE UUID ");
    writer.writeUUIDLiteral(references.object.database_uuid);
    writer.write(" OBJECT UUID ");
    writer.writeUUIDLiteral(references.object.object_uuid);
    writer.write(" REVISION ");
    writer.writeUInt64(references.object_schema_revision);
    writer.write(" PHYSICAL FINGERPRINT ");
    writer.writeDigestLiteral(references.physical_schema_fingerprint);
    writer.write(" PATH DICTIONARY V");
    writer.writeUInt64(references.path_dictionary_version);
    writer.write(" SEMANTIC EXTENSION V");
    writer.writeUInt64(references.semantic_extension_version);
    writer.write(" FLAGS ");
    writer.writeUInt64(references.semantic_extension_flags);

    for (size_t index = 0; index < references.descriptors.size(); ++index)
    {
        writer.write(",\n");
        writeDescriptorText(writer, static_cast<UInt64>(index), references.descriptors[index]);
    }
    for (size_t index = 0; index < references.uses.size(); ++index)
    {
        writer.write(",\n");
        writeUseText(writer, references.format_version, references.occurrence_paths[index], references.uses[index]);
    }
    writer.write("\n)");
    return std::move(writer).finish();
}

void readDescriptorText(TextReader & reader, PersistedTypeReferences & references, const PersistedTypeReferencesLimits & limits)
{
    if (references.descriptors.size() >= limits.maximum_descriptors)
        fail(PersistedTypeReferencesError::Code::LimitExceeded, "persisted descriptor count exceeds its limit");
    const UInt64 descriptor_id = reader.readUInt64();
    if (descriptor_id != references.descriptors.size())
        fail(PersistedTypeReferencesError::Code::NonCanonical, "persisted text descriptor IDs are not compact and ordered");

    reader.expect(" DATABASE UUID ");
    DefinitionIdentity identity;
    identity.database_uuid = reader.readUUID();
    reader.expect(" TYPE UUID ");
    identity.type_uuid = reader.readUUID();
    reader.expect(" REVISION ");
    identity.revision = reader.readUInt64();
    reader.expect(" ARGUMENTS HEX ");
    String canonical_arguments = reader.readHexBytes(limits.maximum_canonical_arguments_bytes);
    reader.expect(" INSTANTIATION HASH ");
    const Digest instantiation_semantic_hash = reader.readDigest();
    reader.expect(" PHYSICAL TYPE ");
    String canonical_physical_type = reader.readQuotedString(
        limits.maximum_canonical_physical_type_bytes, "persisted descriptor physical type exceeds its byte limit");
    reader.expect(" NAME ");
    String last_known_qualified_name
        = reader.readQuotedString(limits.maximum_qualified_name_bytes, "persisted descriptor diagnostic name exceeds its byte limit");
    reader.expect(" DEFINITION HASH ");
    const Digest definition_hash = reader.readDigest();
    reader.expect(" STORAGE FINGERPRINT ");
    const Digest storage_fingerprint = reader.readDigest();
    reader.expect(" CHECKER ABI ");
    const UInt16 checker_abi = narrowUInt16(reader.readUInt64(), "persisted descriptor checker ABI exceeds UInt16");
    reader.expect(" CHECKER CHARGE ABI ");
    const UInt16 checker_charge_abi = narrowUInt16(reader.readUInt64(), "persisted descriptor checker charge ABI exceeds UInt16");
    reader.expect(" POLICY ABI ");
    const UInt16 policy_abi = narrowUInt16(reader.readUInt64(), "persisted descriptor policy ABI exceeds UInt16");
    reader.expect(" FUNCTION REGISTRY ABI ");
    const UInt16 function_registry_abi = narrowUInt16(reader.readUInt64(), "persisted descriptor function registry ABI exceeds UInt16");
    reader.expect(" POLICY HASH ");
    const Digest policy_semantic_hash = reader.readDigest();
    reader.expect(" CAPABILITIES ");
    const SemanticCapabilityMask semantic_capabilities
        = narrowUInt8(reader.readUInt64(), "persisted descriptor semantic capabilities exceed UInt8");

    try
    {
        references.descriptors.push_back(
            PersistedTypeDescriptor::fromCanonicalPersistenceFields(
                identity,
                definition_hash,
                std::move(canonical_arguments),
                std::move(canonical_physical_type),
                instantiation_semantic_hash,
                storage_fingerprint,
                checker_abi,
                checker_charge_abi,
                policy_abi,
                function_registry_abi,
                policy_semantic_hash,
                semantic_capabilities,
                std::move(last_known_qualified_name),
                descriptorLimits(limits)));
    }
    catch (const DescriptorError & error)
    {
        if (error.code == DescriptorError::Code::LimitExceeded)
            fail(PersistedTypeReferencesError::Code::LimitExceeded, error.what());
        if (error.code == DescriptorError::Code::ConflictingIdentity)
            fail(PersistedTypeReferencesError::Code::DigestMismatch, error.what());
        fail(PersistedTypeReferencesError::Code::InvalidValue, error.what());
    }
}

void readUseText(
    TextReader & reader, UInt16 format_version, PersistedTypeReferences & references, const PersistedTypeReferencesLimits & limits)
{
    if (references.occurrence_paths.size() >= limits.maximum_occurrence_paths)
        fail(PersistedTypeReferencesError::Code::LimitExceeded, "persisted occurrence count exceeds its limit");

    const UInt8 section = narrowUInt8(reader.readUInt64(), "persisted occurrence-path section exceeds UInt8");
    PersistedTypeOccurrencePath path;
    path.section = decodePathSection(section);
    reader.expect(", ");
    if (format_version == persisted_type_references_format_version_v2)
    {
        path.site = decodeOccurrenceSite(narrowUInt8(reader.readUInt64(), "persisted occurrence site exceeds UInt8"));
        reader.expect(", ");
    }
    path.object_ordinal = reader.readUInt64();
    reader.expect(", ");
    path.occurrence_ordinal = reader.readUInt64();
    while (reader.consume(", "))
    {
        if (path.type_child_ordinals.size() >= limits.maximum_path_depth)
            fail(PersistedTypeReferencesError::Code::LimitExceeded, "persisted type occurrence path exceeds its depth limit");
        path.type_child_ordinals.push_back(reader.readUInt64());
    }
    reader.expect(") REF ");

    PersistedTypeOccurrenceUse use;
    use.path_id = checkedSize(references.occurrence_paths.size(), "persisted occurrence-path ID exceeds UInt64");
    use.descriptor_id = reader.readUInt64();
    references.occurrence_paths.push_back(std::move(path));
    references.uses.push_back(use);
}

PersistedTypeReferences readReferencesText(std::string_view text, const PersistedTypeReferencesLimits & limits)
{
    TextReader reader(text);
    reader.expect("TYPE REFERENCES V");
    const UInt64 format_version = reader.readUInt64();
    if (format_version != persisted_type_references_format_version && format_version != persisted_type_references_format_version_v2)
        fail(PersistedTypeReferencesError::Code::UnsupportedVersion, "unsupported persisted type references text version");
    reader.expect(" (\n  OBJECT ");

    PersistedTypeReferences result;
    result.format_version = static_cast<UInt16>(format_version);
    result.object.kind = readObjectKind(reader);
    reader.expect(" DATABASE UUID ");
    result.object.database_uuid = reader.readUUID();
    reader.expect(" OBJECT UUID ");
    result.object.object_uuid = reader.readUUID();
    reader.expect(" REVISION ");
    result.object_schema_revision = reader.readUInt64();
    reader.expect(" PHYSICAL FINGERPRINT ");
    result.physical_schema_fingerprint = reader.readDigest();
    reader.expect(" PATH DICTIONARY V");
    const UInt64 path_dictionary_version = reader.readUInt64();
    if (path_dictionary_version > std::numeric_limits<UInt16>::max()
        || !isSupportedFormatPair(result.format_version, static_cast<UInt16>(path_dictionary_version)))
        fail(PersistedTypeReferencesError::Code::UnsupportedVersion, "unsupported persisted type references text path dictionary version");
    result.path_dictionary_version = static_cast<UInt16>(path_dictionary_version);
    reader.expect(" SEMANTIC EXTENSION V");
    const UInt64 semantic_extension_version = reader.readUInt64();
    if (semantic_extension_version != 1)
        fail(PersistedTypeReferencesError::Code::UnsupportedVersion, "unsupported persisted type references text semantic extension");
    result.semantic_extension_version = static_cast<UInt16>(semantic_extension_version);
    reader.expect(" FLAGS ");
    const UInt64 semantic_extension_flags = reader.readUInt64();
    if (semantic_extension_flags != 0)
        fail(PersistedTypeReferencesError::Code::UnsupportedVersion, "unsupported persisted type references text semantic flags");
    result.semantic_extension_flags = 0;

    reader.expect(",\n  REF ");
    while (true)
    {
        readDescriptorText(reader, result, limits);
        if (!reader.consume(",\n  REF "))
            break;
    }

    reader.expect(",\n  USE PATH (");
    while (true)
    {
        readUseText(reader, result.format_version, result, limits);
        if (!reader.consume(",\n  USE PATH ("))
            break;
    }
    reader.expect("\n)");
    reader.requireEnd();
    return result;
}

}

PersistedTypeReferencesError::PersistedTypeReferencesError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

void validatePersistedTypeReferencesLimits(const PersistedTypeReferencesLimits & limits)
{
    validateTextLimits(limits);
}

String encodePersistedTypeReferences(const PersistedTypeReferences & references, const PersistedTypeReferencesLimits & limits)
{
    validateReferences(references, limits);
    Writer counter(limits.maximum_sidecar_bytes, true);
    writeReferences(counter, references);

    Writer writer(limits.maximum_sidecar_bytes);
    writer.reserveMaterialized(counter.size());
    writeReferences(writer, references);
    if (writer.size() != counter.size())
        fail(PersistedTypeReferencesError::Code::InvalidValue, "persisted type references size changed during materialization");
    return std::move(writer).finish();
}

PersistedTypeReferences decodePersistedTypeReferences(std::string_view bytes, const PersistedTypeReferencesLimits & limits)
{
    validateLimits(limits);
    if (checkedSize(bytes.size(), "persisted type references input exceeds UInt64") > limits.maximum_sidecar_bytes)
        fail(PersistedTypeReferencesError::Code::LimitExceeded, "persisted type references exceed their sidecar byte limit");

    Reader reader(bytes);
    PersistedTypeReferences result;
    result.format_version = reader.readUInt16LE();
    if (result.format_version != persisted_type_references_format_version
        && result.format_version != persisted_type_references_format_version_v2)
        fail(PersistedTypeReferencesError::Code::UnsupportedVersion, "unsupported persisted type references version");
    result.object = readObjectID(reader);
    result.object_schema_revision = reader.readUInt64LE();
    result.physical_schema_fingerprint = reader.readDigest();
    result.path_dictionary_version = reader.readUInt16LE();
    if (!isSupportedFormatPair(result.format_version, result.path_dictionary_version))
        fail(PersistedTypeReferencesError::Code::UnsupportedVersion, "unsupported persisted type occurrence-path dictionary version");

    const UInt64 descriptor_count = reader.readMinimalVarUInt(limits.maximum_descriptors, "persisted descriptor count exceeds its limit");
    reader.requireItemsFit(descriptor_count, 1, "persisted descriptor count cannot fit the remaining sidecar bytes");
    result.descriptors.reserve(static_cast<size_t>(descriptor_count));
    for (UInt64 index = 0; index < descriptor_count; ++index)
    {
        const std::string_view frame
            = reader.readFrame(limits.maximum_sidecar_bytes, "persisted descriptor frame exceeds the sidecar byte limit");
        result.descriptors.push_back(readDescriptor(frame, limits));
    }

    const UInt64 path_count
        = reader.readMinimalVarUInt(limits.maximum_occurrence_paths, "persisted occurrence-path count exceeds its limit");
    reader.requireItemsFit(path_count, 1, "persisted occurrence-path count cannot fit the remaining sidecar bytes");
    result.occurrence_paths.reserve(static_cast<size_t>(path_count));
    for (UInt64 index = 0; index < path_count; ++index)
    {
        const std::string_view frame
            = reader.readFrame(limits.maximum_sidecar_bytes, "persisted occurrence-path frame exceeds the sidecar byte limit");
        result.occurrence_paths.push_back(readPath(frame, result.path_dictionary_version, limits));
    }

    const UInt64 use_count = reader.readMinimalVarUInt(limits.maximum_occurrence_paths, "persisted occurrence-use count exceeds its limit");
    reader.requireItemsFit(use_count, 1, "persisted occurrence-use count cannot fit the remaining sidecar bytes");
    result.uses.reserve(static_cast<size_t>(use_count));
    for (UInt64 index = 0; index < use_count; ++index)
    {
        const std::string_view frame
            = reader.readFrame(limits.maximum_sidecar_bytes, "persisted occurrence-use frame exceeds the sidecar byte limit");
        result.uses.push_back(readUse(frame));
    }

    result.semantic_extension_version = reader.readUInt16LE();
    result.semantic_extension_flags = reader.readUInt16LE();
    reader.requireEnd("persisted type references have trailing data");
    validateReferences(result, limits);
    return result;
}

String formatPersistedTypeReferencesText(const PersistedTypeReferences & references, const PersistedTypeReferencesLimits & limits)
{
    validateTextLimits(limits);
    static_cast<void>(encodePersistedTypeReferences(references, limits));
    return writeReferencesText(references, limits.maximum_text_bytes);
}

PersistedTypeReferences parsePersistedTypeReferencesText(std::string_view text, const PersistedTypeReferencesLimits & limits)
{
    validateTextLimits(limits);
    if (checkedSize(text.size(), "persisted type references text input exceeds UInt64") > limits.maximum_text_bytes)
        fail(PersistedTypeReferencesError::Code::LimitExceeded, "persisted type references exceed their text byte limit");

    PersistedTypeReferences result = readReferencesText(text, limits);
    static_cast<void>(encodePersistedTypeReferences(result, limits));
    if (writeReferencesText(result, limits.maximum_text_bytes) != text)
        fail(PersistedTypeReferencesError::Code::NonCanonical, "persisted type references text is not canonically formatted");
    return result;
}

Digest
computePersistedTypeReferencesSidecarHash(const PersistedTypeReferences & references, const PersistedTypeReferencesLimits & limits)
{
    const String encoded = encodePersistedTypeReferences(references, limits);
    const std::string_view hash_domain = references.format_version == persisted_type_references_format_version_v2
        ? persisted_type_references_sidecar_hash_domain_v2
        : persisted_type_references_sidecar_hash_domain;
    CanonicalHasher hasher(hash_domain);

    std::array<CanonicalByte, 10> encoded_size{};
    UInt64 remaining_size = checkedSize(encoded.size(), "persisted type references input exceeds UInt64");
    size_t encoded_size_bytes = 0;
    do
    {
        CanonicalByte byte = static_cast<CanonicalByte>(remaining_size & 0x7f);
        remaining_size >>= 7;
        if (remaining_size)
            byte = static_cast<CanonicalByte>(byte | 0x80);
        encoded_size[encoded_size_bytes++] = byte;
    } while (remaining_size);
    hasher.update(std::span(encoded_size).first(encoded_size_bytes));
    hasher.update(encoded);
    hasher.updateUUID(references.object.database_uuid);
    const std::array<CanonicalByte, 1> object_kind{static_cast<CanonicalByte>(references.object.kind)};
    hasher.update(object_kind);
    hasher.updateUUID(references.object.object_uuid);
    std::array<CanonicalByte, sizeof(UInt64)> revision{};
    for (size_t index = 0; index < revision.size(); ++index)
        revision[index] = static_cast<CanonicalByte>(references.object_schema_revision >> (8 * index));
    hasher.update(revision);
    hasher.update(references.physical_schema_fingerprint);
    return hasher.finalize();
}

}
