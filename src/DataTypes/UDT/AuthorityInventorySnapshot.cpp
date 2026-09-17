#include <DataTypes/UDT/AuthorityInventorySnapshot.h>

#include <algorithm>
#include <array>
#include <limits>
#include <utility>

namespace DB::UDT
{
namespace
{

constexpr std::string_view authority_inventory_snapshot_hash_domain = "ClickHouse UDT authority inventory checkpoint snapshot V1";
constexpr UInt16 extension_version = 1;
constexpr UInt16 extension_flags = 0;
constexpr UInt64 inventory_leaf_encoded_bytes = sizeof(UInt16) + sizeof(UInt8) + sizeof(CanonicalUUID) + sizeof(UInt64) + sizeof(Digest);

[[noreturn]] void fail(AuthorityInventorySnapshotError::Code code, std::string_view message)
{
    throw AuthorityInventorySnapshotError(code, message);
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

void validateLimits(const AuthorityInventorySnapshotLimits & limits)
{
    constexpr UInt64 maximum_implementation_bytes = 1ULL << 30;
    if (limits.maximum_snapshot_bytes == 0 || limits.maximum_snapshot_bytes > maximum_implementation_bytes)
        fail(AuthorityInventorySnapshotError::Code::InvalidConfiguration, "authority-inventory snapshot byte limit is invalid");
}

class Writer final
{
public:
    explicit Writer(UInt64 maximum_bytes_)
        : maximum_bytes(maximum_bytes_)
    {
    }

    void writeUInt16LE(UInt16 value)
    {
        require(sizeof(value));
        output.push_back(static_cast<char>(value));
        output.push_back(static_cast<char>(value >> 8));
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
            fail(AuthorityInventorySnapshotError::Code::LimitExceeded, "authority-inventory snapshot exceeds its byte limit");
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
            fail(AuthorityInventorySnapshotError::Code::LimitExceeded, "authority-inventory snapshot exceeds its byte limit");
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
                fail(AuthorityInventorySnapshotError::Code::InvalidValue, "authority-inventory snapshot count overflows UInt64");
            result |= static_cast<UInt64>(byte & 0x7f) << shift;
            if ((byte & 0x80) == 0)
                break;
            if (shift >= 63)
                fail(AuthorityInventorySnapshotError::Code::InvalidValue, "authority-inventory snapshot count overflows UInt64");
            shift = static_cast<UInt8>(shift + 7);
        }
        if (encoded_size != varUIntSize(result))
            fail(AuthorityInventorySnapshotError::Code::NonCanonical, "authority-inventory snapshot count is not minimally encoded");
        if (result > maximum)
            fail(AuthorityInventorySnapshotError::Code::LimitExceeded, "authority-inventory snapshot leaf count exceeds its limit");
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

    UInt64 remaining() const noexcept { return bytes.size() - position; }

    void requireEnd() const
    {
        if (position != bytes.size())
            fail(AuthorityInventorySnapshotError::Code::TrailingData, "authority-inventory snapshot has trailing data");
    }

private:
    void require(UInt64 count) const
    {
        if (count > bytes.size() - position)
            fail(AuthorityInventorySnapshotError::Code::Truncated, "authority-inventory snapshot is truncated");
    }

    std::string_view bytes;
    size_t position = 0;
};

AuthorityInventory::Ptr
validateAndBuildInventory(const AuthorityInventorySnapshot & snapshot, const AuthorityInventorySnapshotLimits & limits)
{
    validateLimits(limits);
    if (snapshot.format_version != authority_inventory_snapshot_format_version)
        fail(AuthorityInventorySnapshotError::Code::UnsupportedVersion, "unsupported authority-inventory snapshot version");
    if (snapshot.database_uuid == UUIDHelpers::Nil)
        fail(AuthorityInventorySnapshotError::Code::InvalidValue, "authority-inventory snapshot database UUID is nil");
    if (snapshot.semantic_extension_version != extension_version || snapshot.semantic_extension_flags != extension_flags)
        fail(AuthorityInventorySnapshotError::Code::UnsupportedVersion, "unsupported authority-inventory snapshot extension");
    try
    {
        const auto summary = buildAuthorityInventorySummary(snapshot.leaves, limits.inventory);
        return AuthorityInventory::create(summary, snapshot.leaves, limits.inventory);
    }
    catch (const AuthorityInventoryError & error)
    {
        if (error.code == AuthorityInventoryError::Code::LimitExceeded)
            fail(AuthorityInventorySnapshotError::Code::LimitExceeded, "authority-inventory snapshot leaves exceed their limit");
        if (error.code == AuthorityInventoryError::Code::NonCanonical)
            fail(AuthorityInventorySnapshotError::Code::NonCanonical, "authority-inventory snapshot leaves are not canonical");
        fail(AuthorityInventorySnapshotError::Code::InvalidValue, "authority-inventory snapshot contains an invalid leaf");
    }
}

}

AuthorityInventorySnapshotError::AuthorityInventorySnapshotError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

AuthorityInventorySnapshot makeAuthorityInventorySnapshot(
    UUID database_uuid, std::vector<AuthorityInventoryLeaf> sorted_leaves, const AuthorityInventorySnapshotLimits & limits)
{
    AuthorityInventorySnapshot result{
        .database_uuid = database_uuid,
        .leaves = std::move(sorted_leaves),
    };
    static_cast<void>(encodeAuthorityInventorySnapshot(result, limits));
    return result;
}

String encodeAuthorityInventorySnapshot(const AuthorityInventorySnapshot & snapshot, const AuthorityInventorySnapshotLimits & limits)
{
    static_cast<void>(validateAndBuildInventory(snapshot, limits));
    Writer writer(limits.maximum_snapshot_bytes);
    writer.writeUInt16LE(snapshot.format_version);
    writer.writeArray(uuidToCanonicalBytes(snapshot.database_uuid));
    writer.writeVarUInt(snapshot.leaves.size());
    for (const auto & leaf : snapshot.leaves)
        writer.writeBytes(encodeAuthorityInventoryLeaf(leaf, limits.inventory));
    writer.writeUInt16LE(snapshot.semantic_extension_version);
    writer.writeUInt16LE(snapshot.semantic_extension_flags);
    return std::move(writer).release();
}

AuthorityInventorySnapshot decodeAuthorityInventorySnapshot(std::string_view bytes, const AuthorityInventorySnapshotLimits & limits)
{
    validateLimits(limits);
    Reader reader(bytes, limits.maximum_snapshot_bytes);
    AuthorityInventorySnapshot result;
    result.format_version = reader.readUInt16LE();
    if (result.format_version != authority_inventory_snapshot_format_version)
        fail(AuthorityInventorySnapshotError::Code::UnsupportedVersion, "unsupported authority-inventory snapshot version");
    result.database_uuid = uuidFromCanonicalBytes(reader.readArray<sizeof(CanonicalUUID)>());
    const UInt64 leaf_count = reader.readMinimalVarUInt(limits.inventory.maximum_leaves);
    if (leaf_count > reader.remaining() / inventory_leaf_encoded_bytes)
        fail(AuthorityInventorySnapshotError::Code::Truncated, "authority-inventory snapshot leaf count exceeds remaining bytes");
    result.leaves.reserve(static_cast<size_t>(leaf_count));
    for (UInt64 index = 0; index < leaf_count; ++index)
    {
        result.leaves.push_back(decodeAuthorityInventoryLeaf(reader.readBytes(inventory_leaf_encoded_bytes), limits.inventory));
    }
    result.semantic_extension_version = reader.readUInt16LE();
    result.semantic_extension_flags = reader.readUInt16LE();
    reader.requireEnd();
    static_cast<void>(validateAndBuildInventory(result, limits));
    return result;
}

Digest
computeAuthorityInventorySnapshotHash(const AuthorityInventorySnapshot & snapshot, const AuthorityInventorySnapshotLimits & limits)
{
    return hashFramedDomainSeparated(authority_inventory_snapshot_hash_domain, encodeAuthorityInventorySnapshot(snapshot, limits));
}

AuthorityInventory::Ptr
buildAuthorityInventoryFromSnapshot(const AuthorityInventorySnapshot & snapshot, const AuthorityInventorySnapshotLimits & limits)
{
    return validateAndBuildInventory(snapshot, limits);
}

}
