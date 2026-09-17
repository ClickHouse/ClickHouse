#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <array>
#include <exception>
#include <utility>

namespace DB::UDT
{
namespace
{

constexpr std::string_view authority_record_hash_domain = "ClickHouse UDT authority record V1";

[[noreturn]] void fail(SidecarExpectationRecordError::Code code, std::string_view message)
{
    throw SidecarExpectationRecordError(code, message);
}

bool isSidecarObjectKind(SchemaObjectKind kind) noexcept
{
    return kind == SchemaObjectKind::Table || kind == SchemaObjectKind::View || kind == SchemaObjectKind::Dictionary
        || kind == SchemaObjectKind::SyntheticTestObject;
}

void validate(const SidecarExpectationRecord & record)
{
    if (record.format_version != sidecar_expectation_record_format_version)
        fail(SidecarExpectationRecordError::Code::UnsupportedVersion, "unsupported sidecar expectation record version");
    if (!record.object.isValid() || !isSidecarObjectKind(record.object.kind))
        fail(SidecarExpectationRecordError::Code::InvalidValue, "sidecar expectation object identity is invalid");
    if (!record.object_schema_revision)
        fail(SidecarExpectationRecordError::Code::InvalidValue, "sidecar expectation object revision is zero");
    if (record.semantic_extension_version != 1 || record.semantic_extension_flags != 0)
        fail(SidecarExpectationRecordError::Code::UnsupportedVersion, "unsupported sidecar expectation semantic extension");
}

class Writer final
{
public:
    void writeByte(UInt8 value) { output.push_back(static_cast<char>(value)); }

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

    template <size_t size>
    void writeBytes(const std::array<CanonicalByte, size> & bytes)
    {
        output.append(reinterpret_cast<const char *>(bytes.data()), bytes.size());
    }

    String finish() && { return std::move(output); }

private:
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

    template <size_t size>
    std::array<CanonicalByte, size> readBytes()
    {
        require(size);
        std::array<CanonicalByte, size> result{};
        for (size_t index = 0; index < size; ++index)
            result[index] = static_cast<CanonicalByte>(bytes[position + index]);
        position += size;
        return result;
    }

    size_t remaining() const noexcept { return bytes.size() - position; }

    void requireEnd() const
    {
        if (position != bytes.size())
            fail(SidecarExpectationRecordError::Code::TrailingData, "sidecar expectation record has trailing data");
    }

private:
    void require(size_t count) const
    {
        if (count > bytes.size() - position)
            fail(SidecarExpectationRecordError::Code::Truncated, "sidecar expectation record is truncated");
    }

    std::string_view bytes;
    size_t position = 0;
};

void writeObjectID(Writer & writer, const SchemaObjectID & object)
{
    writer.writeByte(static_cast<UInt8>(object.kind));
    writer.writeBytes(uuidToCanonicalBytes(object.database_uuid));
    writer.writeBytes(uuidToCanonicalBytes(object.object_uuid));
}

SchemaObjectID readObjectID(Reader & reader)
{
    return {
        .kind = static_cast<SchemaObjectKind>(reader.readByte()),
        .database_uuid = uuidFromCanonicalBytes(reader.readBytes<sizeof(CanonicalUUID)>()),
        .object_uuid = uuidFromCanonicalBytes(reader.readBytes<sizeof(CanonicalUUID)>()),
    };
}

}

SidecarExpectationRecordError::SidecarExpectationRecordError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

String encodeSidecarExpectationRecord(const SidecarExpectationRecord & record)
{
    validate(record);
    Writer writer;
    writer.writeUInt16LE(record.format_version);
    writeObjectID(writer, record.object);
    writer.writeUInt64LE(record.object_schema_revision);
    writer.writeBytes(record.sidecar_hash);
    writer.writeBytes(record.physical_schema_fingerprint);
    writer.writeUInt16LE(record.semantic_extension_version);
    writer.writeUInt16LE(record.semantic_extension_flags);
    if (record.installation_record_hash)
        writer.writeBytes(*record.installation_record_hash);
    String result = std::move(writer).finish();
    const size_t expected_size = record.installation_record_hash ? sidecar_expectation_record_extended_encoded_bytes
                                                                 : sidecar_expectation_record_encoded_bytes;
    if (result.size() != expected_size)
        std::terminate();
    return result;
}

SidecarExpectationRecord decodeSidecarExpectationRecord(std::string_view bytes)
{
    Reader reader(bytes);
    SidecarExpectationRecord record;
    record.format_version = reader.readUInt16LE();
    if (record.format_version != sidecar_expectation_record_format_version)
        fail(SidecarExpectationRecordError::Code::UnsupportedVersion, "unsupported sidecar expectation record version");
    record.object = readObjectID(reader);
    record.object_schema_revision = reader.readUInt64LE();
    record.sidecar_hash = reader.readBytes<sizeof(Digest)>();
    record.physical_schema_fingerprint = reader.readBytes<sizeof(Digest)>();
    record.semantic_extension_version = reader.readUInt16LE();
    record.semantic_extension_flags = reader.readUInt16LE();
    if (reader.remaining() == sizeof(Digest))
        record.installation_record_hash = reader.readBytes<sizeof(Digest)>();
    else if (reader.remaining() != 0)
    {
        const auto code = reader.remaining() < sizeof(Digest) ? SidecarExpectationRecordError::Code::Truncated
                                                              : SidecarExpectationRecordError::Code::TrailingData;
        fail(code, "sidecar expectation installation-record hash has an invalid encoded size");
    }
    reader.requireEnd();
    validate(record);
    return record;
}

Digest computeSidecarExpectationRecordHash(const SidecarExpectationRecord & record)
{
    return hashFramedDomainSeparated(authority_record_hash_domain, encodeSidecarExpectationRecord(record));
}

}
