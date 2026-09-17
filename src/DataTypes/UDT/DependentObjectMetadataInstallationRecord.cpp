#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>

#include <algorithm>
#include <array>
#include <utility>

namespace DB::UDT
{
namespace
{

constexpr std::string_view installation_record_hash_domain = "ClickHouse UDT dependent object metadata installation record V1";

[[noreturn]] void fail(DependentObjectMetadataInstallationRecordError::Code code, std::string_view message)
{
    throw DependentObjectMetadataInstallationRecordError(code, message);
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

bool isDependentObjectKind(SchemaObjectKind kind) noexcept
{
    return kind == SchemaObjectKind::Table || kind == SchemaObjectKind::View || kind == SchemaObjectKind::Dictionary
        || kind == SchemaObjectKind::SyntheticTestObject;
}

void validateLimits(const DependentObjectMetadataInstallationRecordLimits & limits)
{
    constexpr UInt64 implementation_maximum_encoded_bytes = 16ULL << 20;
    constexpr UInt64 implementation_maximum_object_name_bytes = 4ULL << 20;
    if (limits.maximum_encoded_bytes == 0 || limits.maximum_object_name_bytes == 0
        || limits.maximum_encoded_bytes > implementation_maximum_encoded_bytes
        || limits.maximum_object_name_bytes > implementation_maximum_object_name_bytes)
    {
        fail(
            DependentObjectMetadataInstallationRecordError::Code::InvalidConfiguration,
            "dependent-object metadata installation record limits are invalid");
    }
}

void validateRecord(
    const DependentObjectMetadataInstallationRecord & record, const DependentObjectMetadataInstallationRecordLimits & limits)
{
    validateLimits(limits);
    if (record.format_version != dependent_object_metadata_installation_record_format_version)
    {
        fail(
            DependentObjectMetadataInstallationRecordError::Code::UnsupportedVersion,
            "unsupported dependent-object metadata installation record version");
    }
    if (!record.object.isValid() || !isDependentObjectKind(record.object.kind))
    {
        fail(
            DependentObjectMetadataInstallationRecordError::Code::InvalidValue,
            "dependent-object metadata installation identity is invalid");
    }
    if (record.object_schema_revision == 0)
    {
        fail(
            DependentObjectMetadataInstallationRecordError::Code::InvalidValue,
            "dependent-object metadata installation revision is zero");
    }
    if (record.object_name.empty() || record.object_name.find('\0') != String::npos)
    {
        fail(
            DependentObjectMetadataInstallationRecordError::Code::InvalidValue, "dependent-object metadata installation name is invalid");
    }
    if (record.object_name.size() > limits.maximum_object_name_bytes)
    {
        fail(
            DependentObjectMetadataInstallationRecordError::Code::LimitExceeded,
            "dependent-object metadata installation name exceeds its limit");
    }
    if (record.semantic_extension_version != 1 || record.semantic_extension_flags != 0)
    {
        fail(
            DependentObjectMetadataInstallationRecordError::Code::UnsupportedVersion,
            "unsupported dependent-object metadata installation semantic extension");
    }
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

    void writeBytes(std::string_view value)
    {
        require(value.size());
        output.append(value);
    }

    String release() && { return std::move(output); }

private:
    void require(UInt64 count) const
    {
        if (output.size() > maximum_bytes || count > maximum_bytes - output.size())
        {
            fail(
                DependentObjectMetadataInstallationRecordError::Code::LimitExceeded,
                "dependent-object metadata installation record exceeds its byte limit");
        }
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
        {
            fail(
                DependentObjectMetadataInstallationRecordError::Code::LimitExceeded,
                "dependent-object metadata installation record exceeds its byte limit");
        }
    }

    UInt8 readByte()
    {
        require(sizeof(UInt8));
        return static_cast<UInt8>(bytes[position++]);
    }

    UInt16 readUInt16LE()
    {
        UInt16 value = readByte();
        value |= static_cast<UInt16>(readByte()) << 8;
        return value;
    }

    UInt64 readUInt64LE()
    {
        UInt64 value = 0;
        for (size_t index = 0; index < sizeof(value); ++index)
            value |= static_cast<UInt64>(readByte()) << (8 * index);
        return value;
    }

    UInt64 readMinimalVarUInt(UInt64 maximum)
    {
        UInt64 result = 0;
        UInt8 shift = 0;
        UInt64 encoded_size = 0;
        while (true)
        {
            const UInt8 byte = readByte();
            ++encoded_size;
            if (shift == 63 && (byte & 0xfe) != 0)
            {
                fail(
                    DependentObjectMetadataInstallationRecordError::Code::InvalidValue,
                    "dependent-object metadata installation name length overflows UInt64");
            }
            result |= static_cast<UInt64>(byte & 0x7f) << shift;
            if ((byte & 0x80) == 0)
                break;
            if (shift >= 63)
            {
                fail(
                    DependentObjectMetadataInstallationRecordError::Code::InvalidValue,
                    "dependent-object metadata installation name length overflows UInt64");
            }
            shift = static_cast<UInt8>(shift + 7);
        }
        if (encoded_size != varUIntSize(result))
        {
            fail(
                DependentObjectMetadataInstallationRecordError::Code::NonCanonical,
                "dependent-object metadata installation name length is not minimally encoded");
        }
        if (result > maximum)
        {
            fail(
                DependentObjectMetadataInstallationRecordError::Code::LimitExceeded,
                "dependent-object metadata installation name exceeds its limit");
        }
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

    std::string_view readBytes(UInt64 size)
    {
        require(size);
        const auto result = bytes.substr(position, size);
        position += size;
        return result;
    }

    void requireEnd() const
    {
        if (position != bytes.size())
        {
            fail(
                DependentObjectMetadataInstallationRecordError::Code::TrailingData,
                "dependent-object metadata installation record has trailing data");
        }
    }

private:
    void require(UInt64 count) const
    {
        if (count > bytes.size() - position)
        {
            fail(
                DependentObjectMetadataInstallationRecordError::Code::Truncated,
                "dependent-object metadata installation record is truncated");
        }
    }

    std::string_view bytes;
    size_t position = 0;
};

void writeObjectID(Writer & writer, const SchemaObjectID & object)
{
    writer.writeByte(static_cast<UInt8>(object.kind));
    writer.writeArray(uuidToCanonicalBytes(object.database_uuid));
    writer.writeArray(uuidToCanonicalBytes(object.object_uuid));
}

SchemaObjectID readObjectID(Reader & reader)
{
    return {
        .kind = static_cast<SchemaObjectKind>(reader.readByte()),
        .database_uuid = uuidFromCanonicalBytes(reader.readArray<sizeof(CanonicalUUID)>()),
        .object_uuid = uuidFromCanonicalBytes(reader.readArray<sizeof(CanonicalUUID)>()),
    };
}

}

DependentObjectMetadataInstallationRecordError::DependentObjectMetadataInstallationRecordError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

String encodeDependentObjectMetadataInstallationRecord(
    const DependentObjectMetadataInstallationRecord & record, const DependentObjectMetadataInstallationRecordLimits & limits)
{
    validateRecord(record, limits);
    Writer writer(limits.maximum_encoded_bytes);
    writer.writeUInt16LE(record.format_version);
    writeObjectID(writer, record.object);
    writer.writeUInt64LE(record.object_schema_revision);
    writer.writeVarUInt(record.object_name.size());
    writer.writeBytes(record.object_name);
    writer.writeArray(record.metadata_artifact_hash);
    writer.writeUInt16LE(record.semantic_extension_version);
    writer.writeUInt16LE(record.semantic_extension_flags);
    return std::move(writer).release();
}

DependentObjectMetadataInstallationRecord
decodeDependentObjectMetadataInstallationRecord(std::string_view bytes, const DependentObjectMetadataInstallationRecordLimits & limits)
{
    validateLimits(limits);
    Reader reader(bytes, limits.maximum_encoded_bytes);
    DependentObjectMetadataInstallationRecord result;
    result.format_version = reader.readUInt16LE();
    if (result.format_version != dependent_object_metadata_installation_record_format_version)
    {
        fail(
            DependentObjectMetadataInstallationRecordError::Code::UnsupportedVersion,
            "unsupported dependent-object metadata installation record version");
    }
    result.object = readObjectID(reader);
    result.object_schema_revision = reader.readUInt64LE();
    const UInt64 name_size = reader.readMinimalVarUInt(limits.maximum_object_name_bytes);
    result.object_name = String(reader.readBytes(name_size));
    result.metadata_artifact_hash = reader.readArray<sizeof(Digest)>();
    result.semantic_extension_version = reader.readUInt16LE();
    result.semantic_extension_flags = reader.readUInt16LE();
    reader.requireEnd();
    validateRecord(result, limits);
    if (encodeDependentObjectMetadataInstallationRecord(result, limits) != bytes)
    {
        fail(
            DependentObjectMetadataInstallationRecordError::Code::NonCanonical,
            "dependent-object metadata installation record is not canonical");
    }
    return result;
}

Digest computeDependentObjectMetadataInstallationRecordHash(
    const DependentObjectMetadataInstallationRecord & record, const DependentObjectMetadataInstallationRecordLimits & limits)
{
    return hashFramedDomainSeparated(installation_record_hash_domain, encodeDependentObjectMetadataInstallationRecord(record, limits));
}

}
