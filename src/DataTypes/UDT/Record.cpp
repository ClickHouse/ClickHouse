#include <DataTypes/UDT/Record.h>

#include <DataTypes/UDT/CanonicalHash.h>

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <utility>

namespace DB::UDT
{
namespace
{

constexpr std::string_view authority_record_hash_domain = "ClickHouse UDT authority record V1";

[[noreturn]] void fail(RecordError::Code code, std::string_view message)
{
    throw RecordError(code, message);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(RecordError::Code::LimitExceeded, "user-defined type record size overflows UInt64");
    return lhs + rhs;
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

bool containsNul(std::string_view value) noexcept
{
    return value.find('\0') != std::string_view::npos;
}

UInt8 encodeParameterKind(ParameterKind kind)
{
    switch (kind)
    {
        case ParameterKind::Type: return 1;
        case ParameterKind::Bool: return 2;
        case ParameterKind::UInt8: return 3;
        case ParameterKind::UInt16: return 4;
        case ParameterKind::UInt32: return 5;
        case ParameterKind::UInt64: return 6;
        case ParameterKind::Int8: return 7;
        case ParameterKind::Int16: return 8;
        case ParameterKind::Int32: return 9;
        case ParameterKind::Int64: return 10;
        case ParameterKind::String: return 11;
    }
    fail(RecordError::Code::InvalidValue, "unknown user-defined type parameter kind");
}

ParameterKind decodeParameterKind(UInt8 value)
{
    switch (value)
    {
        case 1: return ParameterKind::Type;
        case 2: return ParameterKind::Bool;
        case 3: return ParameterKind::UInt8;
        case 4: return ParameterKind::UInt16;
        case 5: return ParameterKind::UInt32;
        case 6: return ParameterKind::UInt64;
        case 7: return ParameterKind::Int8;
        case 8: return ParameterKind::Int16;
        case 9: return ParameterKind::Int32;
        case 10: return ParameterKind::Int64;
        case 11: return ParameterKind::String;
        default: fail(RecordError::Code::InvalidValue, "unknown user-defined type parameter kind");
    }
}

UInt8 encodeStorageBackend(StorageBackend backend)
{
    switch (backend)
    {
        case StorageBackend::AtomicDisk: return 1;
    }
    fail(RecordError::Code::InvalidValue, "unknown user-defined type record storage backend");
}

StorageBackend decodeStorageBackend(UInt8 value)
{
    if (value == 1)
        return StorageBackend::AtomicDisk;
    fail(RecordError::Code::InvalidValue, "unknown user-defined type record storage backend");
}

bool dependencyLess(const DefinitionDependency & lhs, const DefinitionDependency & rhs) noexcept
{
    const auto lhs_uuid = uuidToCanonicalBytes(lhs.type_uuid);
    const auto rhs_uuid = uuidToCanonicalBytes(rhs.type_uuid);
    if (lhs_uuid != rhs_uuid)
        return lhs_uuid < rhs_uuid;
    return lhs.revision < rhs.revision;
}

void validateLimits(const RecordLimits & limits)
{
    constexpr UInt64 implementation_maximum_record_bytes = 16ULL << 20;
    constexpr UInt64 implementation_maximum_field_bytes = 4ULL << 20;
    constexpr UInt64 implementation_maximum_parameters = 1ULL << 10;
    constexpr UInt64 implementation_maximum_dependencies = 1ULL << 16;
    if (!limits.maximum_record_bytes || !limits.maximum_name_bytes || !limits.maximum_parameter_count
        || !limits.maximum_parameter_name_bytes || !limits.maximum_canonical_sql_bytes || !limits.maximum_template_ir_bytes
        || !limits.maximum_dependency_count || !limits.maximum_checker_certificate_bytes || !limits.maximum_owner_display_name_bytes
        || !limits.maximum_comment_bytes)
        fail(RecordError::Code::InvalidValue, "every user-defined type record limit must be nonzero");
    if (limits.maximum_record_bytes > implementation_maximum_record_bytes || limits.maximum_name_bytes > implementation_maximum_field_bytes
        || limits.maximum_parameter_count > implementation_maximum_parameters
        || limits.maximum_parameter_name_bytes > implementation_maximum_field_bytes
        || limits.maximum_canonical_sql_bytes > implementation_maximum_field_bytes
        || limits.maximum_template_ir_bytes > implementation_maximum_field_bytes
        || limits.maximum_dependency_count > implementation_maximum_dependencies
        || limits.maximum_checker_certificate_bytes > implementation_maximum_field_bytes
        || limits.maximum_owner_display_name_bytes > implementation_maximum_field_bytes
        || limits.maximum_comment_bytes > implementation_maximum_field_bytes)
        fail(RecordError::Code::InvalidValue, "a user-defined type record limit exceeds the implementation maximum");
}

void validateText(
    std::string_view value,
    UInt64 maximum_bytes,
    bool require_nonempty,
    bool reject_nul,
    std::string_view empty_message,
    std::string_view limit_message,
    std::string_view nul_message)
{
    if (require_nonempty && value.empty())
        fail(RecordError::Code::InvalidValue, empty_message);
    if (value.size() > maximum_bytes)
        fail(RecordError::Code::LimitExceeded, limit_message);
    if (reject_nul && containsNul(value))
        fail(RecordError::Code::InvalidValue, nul_message);
}

void validateRecord(const Record & record, const RecordLimits & limits)
{
    validateLimits(limits);
    if (record.format_version != record_format_version)
        fail(RecordError::Code::UnsupportedVersion, "unsupported user-defined type record version");
    if (record.identity.database_uuid == UUIDHelpers::Nil || record.identity.type_uuid == UUIDHelpers::Nil)
        fail(RecordError::Code::InvalidValue, "user-defined type record identity contains a nil UUID");
    if (!record.identity.revision)
        fail(RecordError::Code::InvalidValue, "user-defined type record revision is zero");

    validateText(
        record.normalized_name,
        limits.maximum_name_bytes,
        true,
        true,
        "user-defined type normalized name is empty",
        "user-defined type normalized name exceeds its limit",
        "user-defined type normalized name contains NUL");
    validateText(
        record.normalized_local_name,
        limits.maximum_name_bytes,
        false,
        true,
        {},
        "user-defined type local name exceeds its limit",
        "user-defined type local name contains NUL");

    if (record.parameters.size() > limits.maximum_parameter_count)
        fail(RecordError::Code::LimitExceeded, "user-defined type parameter count exceeds its limit");
    for (size_t index = 0; index < record.parameters.size(); ++index)
    {
        static_cast<void>(encodeParameterKind(record.parameters[index].kind));
        validateText(
            record.parameters[index].normalized_name,
            limits.maximum_parameter_name_bytes,
            true,
            true,
            "user-defined type parameter name is empty",
            "user-defined type parameter name exceeds its limit",
            "user-defined type parameter name contains NUL");
        for (size_t previous = 0; previous < index; ++previous)
            if (record.parameters[previous].normalized_name == record.parameters[index].normalized_name)
                fail(RecordError::Code::InvalidValue, "user-defined type parameter name is duplicated");
    }

    if (record.decreasing_parameter)
    {
        if (*record.decreasing_parameter >= record.parameters.size())
            fail(RecordError::Code::InvalidValue, "user-defined type decreasing parameter is out of range");
        if (!isUnsignedIntegerParameter(record.parameters[*record.decreasing_parameter].kind))
            fail(RecordError::Code::InvalidValue, "user-defined type decreasing parameter is not unsigned");
    }

    if ((record.checker_abi != 1 && record.checker_abi != 2) || record.checker_charge_abi != 1 || record.policy_abi != 1
        || record.function_registry_abi != 1)
        fail(RecordError::Code::InvalidValue, "user-defined type record contains an unsupported semantic ABI");
    const bool recursive = record.checker_abi == 2;
    if (recursive != record.decreasing_parameter.has_value() || (recursive && record.policy_bearing))
        fail(RecordError::Code::InvalidValue, "user-defined type record checker mode is inconsistent");
    if (record.policy_bearing == (record.policy_semantic_hash == CheckerProof::empty_policy_semantic_hash))
        fail(RecordError::Code::InvalidValue, "user-defined type record policy state is inconsistent");
    if ((record.semantic_capabilities & ~all_semantic_capabilities) != 0)
        fail(RecordError::Code::InvalidValue, "user-defined type record contains an unknown semantic capability");

    validateText(
        record.canonical_definition_sql,
        limits.maximum_canonical_sql_bytes,
        true,
        true,
        "canonical user-defined type definition SQL is empty",
        "canonical user-defined type definition SQL exceeds its limit",
        "canonical user-defined type definition SQL contains NUL");
    validateText(
        record.canonical_physical_template_sql,
        limits.maximum_canonical_sql_bytes,
        true,
        true,
        "canonical physical template SQL is empty",
        "canonical physical template SQL exceeds its limit",
        "canonical physical template SQL contains NUL");
    validateText(
        record.canonical_template_ir,
        limits.maximum_template_ir_bytes,
        true,
        false,
        "canonical user-defined type template IR is empty",
        "canonical user-defined type template IR exceeds its limit",
        {});

    if (record.dependencies.size() > limits.maximum_dependency_count)
        fail(RecordError::Code::LimitExceeded, "user-defined type dependency count exceeds its limit");
    for (size_t index = 0; index < record.dependencies.size(); ++index)
    {
        const auto & dependency = record.dependencies[index];
        if (dependency.type_uuid == UUIDHelpers::Nil || !dependency.revision)
            fail(RecordError::Code::InvalidValue, "user-defined type dependency identity is invalid");
        if (index && !dependencyLess(record.dependencies[index - 1], dependency))
            fail(RecordError::Code::NonCanonical, "user-defined type dependencies are not strictly sorted");
    }

    validateText(
        record.encoded_checker_certificate,
        limits.maximum_checker_certificate_bytes,
        true,
        false,
        "encoded user-defined type checker certificate is empty",
        "encoded user-defined type checker certificate exceeds its limit",
        {});
    if (record.checker_certificate_digest != hashDomainSeparated(CheckerProof::checker_proof_domain, record.encoded_checker_certificate))
        fail(RecordError::Code::InvalidValue, "user-defined type checker certificate digest does not match its bytes");
    if (!record.charged_work || !record.logical_node_count || record.maximum_template_depth >= record.logical_node_count)
        fail(RecordError::Code::InvalidValue, "user-defined type checker accounting is inconsistent");
    if (record.owner_uuid == UUIDHelpers::Nil)
        fail(RecordError::Code::InvalidValue, "user-defined type record owner UUID is nil");
    validateText(
        record.owner_display_name,
        limits.maximum_owner_display_name_bytes,
        true,
        true,
        "user-defined type owner display name is empty",
        "user-defined type owner display name exceeds its limit",
        "user-defined type owner display name contains NUL");
    validateText(
        record.comment,
        limits.maximum_comment_bytes,
        false,
        true,
        {},
        "user-defined type comment exceeds its limit",
        "user-defined type comment contains NUL");
    static_cast<void>(encodeStorageBackend(record.storage_backend));
    if (record.semantic_extension_version != 1 || record.semantic_extension_flags != 0)
        fail(RecordError::Code::UnsupportedVersion, "unsupported user-defined type semantic extension");
}

class Writer final
{
public:
    explicit Writer(UInt64 maximum_bytes_)
        : maximum_bytes(maximum_bytes_)
    {
    }

    void writeByte(UInt8 value) { append({reinterpret_cast<const char *>(&value), sizeof(value)}); }

    void writeBool(bool value) { writeByte(value ? 1 : 0); }

    void writeUInt16LE(UInt16 value)
    {
        std::array<char, sizeof(value)> bytes{static_cast<char>(value), static_cast<char>(value >> 8)};
        append(bytes);
    }

    void writeUInt64LE(UInt64 value)
    {
        std::array<char, sizeof(value)> bytes{};
        for (size_t index = 0; index < bytes.size(); ++index)
            bytes[index] = static_cast<char>(value >> (8 * index));
        append(bytes);
    }

    void writeInt64LE(Int64 value) { writeUInt64LE(static_cast<UInt64>(value)); }

    void writeVarUInt(UInt64 value)
    {
        std::array<char, 10> bytes{};
        size_t size = 0;
        do
        {
            UInt8 byte = static_cast<UInt8>(value & 0x7f);
            value >>= 7;
            if (value)
                byte = static_cast<UInt8>(byte | 0x80);
            bytes[size++] = static_cast<char>(byte);
        } while (value);
        append(std::string_view(bytes.data(), size));
    }

    void writeUUID(const UUID & uuid) { writeBytes(uuidToCanonicalBytes(uuid)); }
    void writeDigest(const Digest & digest) { writeBytes(digest); }

    void writeFrame(std::string_view value)
    {
        writeVarUInt(value.size());
        append(value);
    }

    String finish() && { return std::move(output); }

private:
    void writeBytes(std::span<const CanonicalByte> bytes) { append({reinterpret_cast<const char *>(bytes.data()), bytes.size()}); }

    template <size_t size>
    void append(const std::array<char, size> & bytes)
    {
        append(std::string_view(bytes.data(), bytes.size()));
    }

    void append(std::string_view bytes)
    {
        const UInt64 next_size = checkedAdd(output.size(), bytes.size());
        if (next_size > maximum_bytes)
            fail(RecordError::Code::LimitExceeded, "user-defined type record exceeds its byte limit");
        output.append(bytes);
    }

    UInt64 maximum_bytes;
    String output;
};

class Reader final
{
public:
    explicit Reader(std::string_view bytes_)
        : bytes(bytes_)
    {
    }

    UInt8 readByte()
    {
        require(sizeof(UInt8));
        return static_cast<UInt8>(bytes[position++]);
    }

    bool readBool()
    {
        const UInt8 value = readByte();
        if (value > 1)
            fail(RecordError::Code::InvalidValue, "user-defined type record boolean is not zero or one");
        return value != 0;
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

    Int64 readInt64LE() { return std::bit_cast<Int64>(readUInt64LE()); }

    UInt64 readMinimalVarUInt(UInt64 maximum)
    {
        UInt64 value = 0;
        UInt8 shift = 0;
        size_t encoded_size = 0;
        while (true)
        {
            const UInt8 byte = readByte();
            ++encoded_size;
            if (shift == 63 && (byte & 0xfe) != 0)
                fail(RecordError::Code::InvalidValue, "user-defined type record VarUInt overflows UInt64");
            value |= static_cast<UInt64>(byte & 0x7f) << shift;
            if ((byte & 0x80) == 0)
                break;
            if (shift >= 63)
                fail(RecordError::Code::InvalidValue, "user-defined type record VarUInt overflows UInt64");
            shift = static_cast<UInt8>(shift + 7);
        }
        if (encoded_size != varUIntSize(value))
            fail(RecordError::Code::NonCanonical, "user-defined type record VarUInt is not minimally encoded");
        if (value > maximum)
            fail(RecordError::Code::LimitExceeded, "user-defined type record count exceeds its limit");
        return value;
    }

    UUID readUUID() { return uuidFromCanonicalBytes(readArray<sizeof(CanonicalUUID)>()); }
    Digest readDigest() { return readArray<sizeof(Digest)>(); }

    String readFrame(UInt64 maximum)
    {
        const UInt64 size = readMinimalVarUInt(maximum);
        if (!std::in_range<size_t>(size))
            fail(RecordError::Code::LimitExceeded, "user-defined type record frame exceeds the platform size domain");
        require(static_cast<size_t>(size));
        String value(bytes.substr(position, static_cast<size_t>(size)));
        position += static_cast<size_t>(size);
        return value;
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

    void requireEnd() const
    {
        if (position != bytes.size())
            fail(RecordError::Code::TrailingData, "user-defined type record has trailing data");
    }

    void requireItemsFit(UInt64 count, size_t minimum_item_bytes, std::string_view message) const
    {
        if (count > (bytes.size() - position) / minimum_item_bytes)
            fail(RecordError::Code::Truncated, message);
    }

private:
    void require(size_t count) const
    {
        if (count > bytes.size() - position)
            fail(RecordError::Code::Truncated, "user-defined type record is truncated");
    }

    std::string_view bytes;
    size_t position = 0;
};

}

RecordError::RecordError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

Record makeRecord(
    const Definition & definition, RecordMetadata metadata, const RecordLimits & limits)
{
    const auto & certificate = definition.getCertificate();
    Record record{
        .format_version = record_format_version,
        .identity = definition.getIdentity(),
        .normalized_name = definition.getNormalizedName(),
        .normalized_local_name = definition.getNormalizedLocalName(),
        .parameters = definition.getParameters(),
        .decreasing_parameter = definition.getDecreasingParameter(),
        .checker_abi = definition.getCheckerABI(),
        .checker_charge_abi = definition.getCheckerChargeABI(),
        .policy_abi = definition.getPolicyABI(),
        .function_registry_abi = definition.getFunctionRegistryABI(),
        .policy_bearing = definition.isPolicyBearing(),
        .semantic_capabilities = definition.getSemanticCapabilities(),
        .policy_semantic_hash = definition.getPolicySemanticHash(),
        .canonical_definition_sql = std::move(metadata.canonical_definition_sql),
        .canonical_physical_template_sql = std::move(metadata.canonical_physical_template_sql),
        .canonical_template_ir = certificate.canonical_template_ir,
        .dependencies = definition.getDependencies(),
        .semantic_definition_digest = certificate.semantic_definition_digest,
        .definition_hash = certificate.definition_hash,
        .compositional_dependency_closure_digest = certificate.compositional_dependency_closure_digest,
        .encoded_checker_certificate = certificate.encoded_certificate,
        .checker_certificate_digest = certificate.certificate_digest,
        .charged_work = certificate.charged_work,
        .logical_node_count = certificate.logical_node_count,
        .maximum_template_depth = certificate.maximum_template_depth,
        .owner_uuid = metadata.owner_uuid,
        .owner_display_name = std::move(metadata.owner_display_name),
        .comment = std::move(metadata.comment),
        .creation_time_us_utc = metadata.creation_time_us_utc,
        .storage_backend = metadata.storage_backend,
        .semantic_extension_version = metadata.semantic_extension_version,
        .semantic_extension_flags = metadata.semantic_extension_flags,
    };
    validateRecord(record, limits);
    return record;
}

String encodeRecord(const Record & record, const RecordLimits & limits)
{
    validateRecord(record, limits);
    Writer writer(limits.maximum_record_bytes);
    writer.writeUInt16LE(record.format_version);
    writer.writeUUID(record.identity.database_uuid);
    writer.writeUUID(record.identity.type_uuid);
    writer.writeUInt64LE(record.identity.revision);
    writer.writeFrame(record.normalized_name);
    writer.writeFrame(record.normalized_local_name);
    writer.writeVarUInt(record.parameters.size());
    for (const auto & parameter : record.parameters)
    {
        writer.writeByte(encodeParameterKind(parameter.kind));
        writer.writeFrame(parameter.normalized_name);
    }
    writer.writeBool(record.decreasing_parameter.has_value());
    if (record.decreasing_parameter)
        writer.writeUInt16LE(*record.decreasing_parameter);
    writer.writeUInt16LE(record.checker_abi);
    writer.writeUInt16LE(record.checker_charge_abi);
    writer.writeUInt16LE(record.policy_abi);
    writer.writeUInt16LE(record.function_registry_abi);
    writer.writeBool(record.policy_bearing);
    writer.writeByte(record.semantic_capabilities);
    writer.writeDigest(record.policy_semantic_hash);
    writer.writeFrame(record.canonical_definition_sql);
    writer.writeFrame(record.canonical_physical_template_sql);
    writer.writeFrame(record.canonical_template_ir);
    writer.writeVarUInt(record.dependencies.size());
    for (const auto & dependency : record.dependencies)
    {
        writer.writeUUID(dependency.type_uuid);
        writer.writeUInt64LE(dependency.revision);
        writer.writeDigest(dependency.target_definition_hash);
    }
    writer.writeDigest(record.semantic_definition_digest);
    writer.writeDigest(record.definition_hash);
    writer.writeDigest(record.compositional_dependency_closure_digest);
    writer.writeFrame(record.encoded_checker_certificate);
    writer.writeDigest(record.checker_certificate_digest);
    writer.writeUInt64LE(record.charged_work);
    writer.writeUInt64LE(record.logical_node_count);
    writer.writeUInt64LE(record.maximum_template_depth);
    writer.writeUUID(record.owner_uuid);
    writer.writeFrame(record.owner_display_name);
    writer.writeFrame(record.comment);
    writer.writeInt64LE(record.creation_time_us_utc);
    writer.writeByte(encodeStorageBackend(record.storage_backend));
    writer.writeUInt16LE(record.semantic_extension_version);
    writer.writeUInt16LE(record.semantic_extension_flags);
    return std::move(writer).finish();
}

Record decodeRecord(std::string_view bytes, const RecordLimits & limits)
{
    validateLimits(limits);
    if (bytes.size() > limits.maximum_record_bytes)
        fail(RecordError::Code::LimitExceeded, "user-defined type record exceeds its byte limit");

    Reader reader(bytes);
    Record record;
    record.format_version = reader.readUInt16LE();
    if (record.format_version != record_format_version)
        fail(RecordError::Code::UnsupportedVersion, "unsupported user-defined type record version");
    record.identity.database_uuid = reader.readUUID();
    record.identity.type_uuid = reader.readUUID();
    record.identity.revision = reader.readUInt64LE();
    record.normalized_name = reader.readFrame(limits.maximum_name_bytes);
    record.normalized_local_name = reader.readFrame(limits.maximum_name_bytes);
    const UInt64 parameter_count = reader.readMinimalVarUInt(limits.maximum_parameter_count);
    reader.requireItemsFit(parameter_count, 2, "user-defined type parameter list is truncated");
    record.parameters.reserve(static_cast<size_t>(parameter_count));
    for (UInt64 index = 0; index < parameter_count; ++index)
    {
        record.parameters.push_back({
            .normalized_name = {},
            .kind = decodeParameterKind(reader.readByte()),
        });
        record.parameters.back().normalized_name = reader.readFrame(limits.maximum_parameter_name_bytes);
    }
    if (reader.readBool())
        record.decreasing_parameter = reader.readUInt16LE();
    record.checker_abi = reader.readUInt16LE();
    record.checker_charge_abi = reader.readUInt16LE();
    record.policy_abi = reader.readUInt16LE();
    record.function_registry_abi = reader.readUInt16LE();
    record.policy_bearing = reader.readBool();
    record.semantic_capabilities = reader.readByte();
    record.policy_semantic_hash = reader.readDigest();
    record.canonical_definition_sql = reader.readFrame(limits.maximum_canonical_sql_bytes);
    record.canonical_physical_template_sql = reader.readFrame(limits.maximum_canonical_sql_bytes);
    record.canonical_template_ir = reader.readFrame(limits.maximum_template_ir_bytes);
    const UInt64 dependency_count = reader.readMinimalVarUInt(limits.maximum_dependency_count);
    constexpr size_t minimum_dependency_bytes = sizeof(CanonicalUUID) + sizeof(UInt64) + sizeof(Digest);
    reader.requireItemsFit(dependency_count, minimum_dependency_bytes, "user-defined type dependency list is truncated");
    record.dependencies.reserve(static_cast<size_t>(dependency_count));
    for (UInt64 index = 0; index < dependency_count; ++index)
    {
        record.dependencies.push_back({
            .type_uuid = reader.readUUID(),
            .revision = reader.readUInt64LE(),
            .target_definition_hash = reader.readDigest(),
        });
    }
    record.semantic_definition_digest = reader.readDigest();
    record.definition_hash = reader.readDigest();
    record.compositional_dependency_closure_digest = reader.readDigest();
    record.encoded_checker_certificate = reader.readFrame(limits.maximum_checker_certificate_bytes);
    record.checker_certificate_digest = reader.readDigest();
    record.charged_work = reader.readUInt64LE();
    record.logical_node_count = reader.readUInt64LE();
    record.maximum_template_depth = reader.readUInt64LE();
    record.owner_uuid = reader.readUUID();
    record.owner_display_name = reader.readFrame(limits.maximum_owner_display_name_bytes);
    record.comment = reader.readFrame(limits.maximum_comment_bytes);
    record.creation_time_us_utc = reader.readInt64LE();
    record.storage_backend = decodeStorageBackend(reader.readByte());
    record.semantic_extension_version = reader.readUInt16LE();
    record.semantic_extension_flags = reader.readUInt16LE();
    reader.requireEnd();
    validateRecord(record, limits);
    return record;
}

Digest computeRecordHash(const Record & record, const RecordLimits & limits)
{
    return hashFramedDomainSeparated(authority_record_hash_domain, encodeRecord(record, limits));
}

bool recordMatchesCheckedDefinition(
    const Record & record, const Definition & definition) noexcept
{
    const auto & certificate = definition.getCertificate();
    return record.identity == definition.getIdentity() && record.normalized_name == definition.getNormalizedName()
        && record.normalized_local_name == definition.getNormalizedLocalName() && record.parameters == definition.getParameters()
        && record.decreasing_parameter == definition.getDecreasingParameter() && record.checker_abi == definition.getCheckerABI()
        && record.checker_charge_abi == definition.getCheckerChargeABI() && record.policy_abi == definition.getPolicyABI()
        && record.function_registry_abi == definition.getFunctionRegistryABI() && record.policy_bearing == definition.isPolicyBearing()
        && record.semantic_capabilities == definition.getSemanticCapabilities()
        && record.policy_semantic_hash == definition.getPolicySemanticHash() && record.dependencies == definition.getDependencies()
        && record.canonical_template_ir == certificate.canonical_template_ir
        && record.semantic_definition_digest == certificate.semantic_definition_digest
        && record.definition_hash == certificate.definition_hash
        && record.compositional_dependency_closure_digest == certificate.compositional_dependency_closure_digest
        && record.encoded_checker_certificate == certificate.encoded_certificate
        && record.checker_certificate_digest == certificate.certificate_digest && record.charged_work == certificate.charged_work
        && record.logical_node_count == certificate.logical_node_count
        && record.maximum_template_depth == certificate.maximum_template_depth;
}

}
